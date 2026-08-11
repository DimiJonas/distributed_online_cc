use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio_util::sync::CancellationToken;

/// this module is used to realize the different result publication strategies:
///     - immediate
///     - grace period
///     - revocable result
use crate::{
    measurement::{api::result_emitted, record::ResultTrigger},
    network::{
        message_types::{CCResultMessage, PeerMessage},
        tcp_router::send_to_conformance_peer,
    },
    state::{
        app_state::AppState, result_revision_store::RevisionNumber,
        result_send_buffer::ResultBufferPolicy,
    },
    types::{CaseId, ConstraintId, ConstraintResultType, ConstraintType, EventInstanceId},
};

/// emit a constraint result, applying the configured buffering policy for the
/// given constraint
///
/// - Immediate (zero grace): writes to cc_results immediately
/// - GracePeriod: writes to cc_results immediately (so the store is always up to date) AND buffers the send; the buffered send fires after the
///   grace window, merging any updates that arrive in the meantime
/// - Revocable: writes to cc_results immediately and keeps the result open to overwrite for the time window, then finalizes
#[allow(clippy::too_many_arguments)]
pub async fn emit_result(
    case_id: &CaseId,
    constraint_id: &ConstraintId,
    constraint_type: ConstraintType,
    result: ConstraintResultType,
    info: String,
    state: AppState,
    result_trigger: ResultTrigger,
    instance_id: Option<EventInstanceId>,
) {
    emit_result_at(
        Utc::now(),
        case_id,
        constraint_id,
        constraint_type,
        result,
        info,
        state,
        result_trigger,
        instance_id,
    )
    .await
}

/// emit a constraint result, stamping the result with the supplied decision time and applying the configured buffering policy for the
/// given constraint
///
/// - Immediate (zero grace): writes to cc_results immediately
/// - GracePeriod: writes to cc_results immediately (so the store is always up to date) AND buffers the send; the buffered send fires after the
///   grace window, merging any updates that arrive in the meantime
/// - Revocable: writes to cc_results immediately and keeps the result open to overwrite for the time window, then finalizes
#[allow(clippy::too_many_arguments)]
pub async fn emit_result_at(
    decided_at: DateTime<Utc>,
    case_id: &CaseId,
    constraint_id: &ConstraintId,
    constraint_type: ConstraintType,
    result: ConstraintResultType,
    info: String,
    state: AppState,
    result_trigger: ResultTrigger,
    instance_id: Option<EventInstanceId>,
) {
    let result_receive_time = decided_at;

    let revision = {
        let mut revisions = state.result_revisions.lock().unwrap();
        revisions.next_revision(case_id, constraint_id)
    };

    log::debug!(
        "emitting result for ({}/{}) revision {}: {:?}: {}",
        constraint_id,
        case_id,
        revision,
        result,
        info,
    );

    let policy = state.config.result_buffer_policy(constraint_id);
    // record result emitted
    result_emitted(
        case_id,
        constraint_id,
        constraint_type,
        result,
        &policy,
        instance_id,
        result_trigger,
        decided_at,
    );

    match policy {
        ResultBufferPolicy::GracePeriod { duration } if duration.is_zero() => {
            log::info!(
                "immediate result configured for ({}/{}): revision {} is final; sending result {:?}",
                case_id,
                constraint_id,
                revision,
                result
            );
            // Immediate: no buffering at all
            {
                let mut results = state.cc_results.lock().unwrap();
                results.upsert(
                    case_id,
                    constraint_id,
                    constraint_type,
                    result,
                    info.clone(),
                );
            }
            send_result_outbound(
                case_id,
                constraint_id,
                constraint_type,
                result,
                &info,
                state.clone(),
                result_receive_time,
                revision,
            )
            .await;
        }

        ResultBufferPolicy::GracePeriod { duration } => {
            // only update cc result store for now
            {
                let mut results = state.cc_results.lock().unwrap();
                results.upsert(
                    case_id,
                    constraint_id,
                    constraint_type,
                    result,
                    info.clone(),
                );
            }

            // buffer the send and (re)start the grace timer
            let cancel_token = {
                let mut buf = state.result_send_buffer.lock().unwrap();
                buf.insert_or_update(case_id.clone(), constraint_id.clone(), result, info)
            };

            tokio::spawn(grace_period_timer(
                duration,
                state.clone(),
                case_id.clone(),
                constraint_id.clone(),
                constraint_type,
                cancel_token,
                result_receive_time,
                revision,
            ));
        }

        ResultBufferPolicy::Revocable { duration } => {
            // write to cc_results and send immediately; allow overwrite within window
            {
                let mut results = state.cc_results.lock().unwrap();
                results.upsert(
                    case_id,
                    constraint_id,
                    constraint_type,
                    result,
                    info.clone(),
                );
            }
            send_result_outbound(
                case_id,
                constraint_id,
                constraint_type,
                result,
                &info,
                state.clone(),
                result_receive_time,
                revision,
            )
            .await;

            let cancel_token = {
                let mut buf = state.result_send_buffer.lock().unwrap();
                buf.insert_or_update(case_id.clone(), constraint_id.clone(), result, info)
            };

            tokio::spawn(revocable_result_timer(
                state.clone(),
                duration,
                case_id.clone(),
                constraint_id.clone(),
                cancel_token,
            ));
        }
    }
}

async fn grace_period_timer(
    duration: Duration,
    state: AppState,
    case_id: CaseId,
    constraint_id: ConstraintId,
    constraint_type: ConstraintType,
    cancel_token: CancellationToken,
    created_at: DateTime<Utc>,
    revision: RevisionNumber,
) {
    tokio::select! {
        _ = tokio::time::sleep(duration) => {
            let pending = {
                let mut buf = state.result_send_buffer.lock().unwrap();
                buf.take(&case_id, &constraint_id)
            };
            if let Some(pending) = pending {
                log::info!(
                    "grace period expired for ({}/{}): revision {} is now final; sending result {:?}",
                    case_id, constraint_id, revision, pending.result
                );
                send_result_outbound(
                    &case_id, &constraint_id,constraint_type,
                    pending.result, &pending.info, state.clone(),
                    created_at, revision,
                ).await;
            }
        }
        _ = cancel_token.cancelled() => {
            log::debug!(
                "grace period timer reset for ({}/{})",
                case_id, constraint_id
            );
        }
    }
}

async fn revocable_result_timer(
    state: AppState,
    duration: Duration,
    case_id: CaseId,
    constraint_id: ConstraintId,
    cancel_token: CancellationToken,
) {
    tokio::select! {
        _ = tokio::time::sleep(duration) => {
            {
                let mut buf = state.result_send_buffer.lock().unwrap();
                buf.take(&case_id, &constraint_id);
            }

            log::info!(
                "revocable window expired for ({}/{}): result is now final",
                case_id, constraint_id
            );
        }
        _ = cancel_token.cancelled() => {
            log::debug!(
                "revocable window reset for ({}/{})",
                case_id, constraint_id
            );
        }
    }
}

/// send the constraint check result to the sink
async fn send_result_outbound(
    case_id: &CaseId,
    constraint_id: &ConstraintId,
    constraint_type: ConstraintType,
    result: ConstraintResultType,
    info: &str,
    state: AppState,
    created_at: DateTime<Utc>,
    revision: RevisionNumber,
) {
    match &state.config.result_sink_addresses {
        Some(addresses) => {
            // sink addresses were configured - sending result there
            let result_message = PeerMessage::result(CCResultMessage {
                created_at,
                revision,
                info: info.to_string(),
                case_id: case_id.clone(),
                constraint_type,
                constraint_id: constraint_id.clone(),
                result,
            });
            for address in addresses {
                log::debug!(
                    "send_result_outbound: sending result message {} to result sink at {}",
                    result_message,
                    address,
                );
                if let Err(e) =
                    send_to_conformance_peer(*address, result_message.clone(), state.clone()).await
                {
                    log::error!(
                        "send_result_outbound: error sending result message {} to {} (result sink): {}",
                        result_message,
                        address,
                        e
                    );
                }
            }
        }
        None => {
            // no sink configured - logging result
            log::info!(
                "send_result_outbound: RESULT for case/constraint {}/{} (revision {}) is {}: {}",
                case_id,
                constraint_id,
                revision,
                result,
                info
            );
        }
    }
}

/// convenience method for handlers that want to finalize a result immediately, skipping the whole buffer logic
#[allow(clippy::too_many_arguments)]
pub async fn emit_result_immediate(
    case_id: &CaseId,
    constraint_id: &ConstraintId,
    constraint_type: ConstraintType,
    result: ConstraintResultType,
    info: String,
    state: AppState,
    result_trigger: ResultTrigger,
    instance_id: Option<EventInstanceId>,
) {
    let result_receive_time = Utc::now();

    let revision = {
        let mut revisions = state.result_revisions.lock().unwrap();
        revisions.next_revision(case_id, constraint_id)
    };

    // record result emitted
    result_emitted(
        case_id,
        constraint_id,
        constraint_type,
        result,
        &ResultBufferPolicy::GracePeriod {
            duration: Duration::ZERO,
        },
        instance_id,
        result_trigger,
        result_receive_time,
    );

    {
        let mut results = state.cc_results.lock().unwrap();
        results.upsert(
            case_id,
            constraint_id,
            constraint_type,
            result,
            info.clone(),
        );
    }
    send_result_outbound(
        case_id,
        constraint_id,
        constraint_type,
        result,
        &info,
        state.clone(),
        result_receive_time,
        revision,
    )
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::node_config::ResultBufferConfig;
    use crate::types::ConstraintType;
    use crate::util::test_tools::{make_config_with_buffers, make_state_from_config};
    use std::collections::HashMap;

    fn register(state: &AppState, case: &str) {
        state
            .cc_results
            .lock()
            .unwrap()
            .register_case(case.to_string());
    }

    /// Yield before and after advancing time so spawned tasks fully
    /// register their sleeps and then run to completion.
    async fn advance_and_settle(secs: u64) {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(std::time::Duration::from_secs(secs)).await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
    }

    fn read_result(state: &AppState, case: &str, constraint: &str) -> ConstraintResultType {
        state
            .cc_results
            .lock()
            .unwrap()
            .get_constraint(&case.to_string(), &constraint.to_string())
            .expect("constraint not registered")
            .constraint_result
    }

    // ── Immediate policy ─────────────────────────────────────────────────

    #[tokio::test]
    async fn immediate_writes_cc_results_synchronously() {
        let cfg = make_config_with_buffers(vec![], HashMap::new(), ResultBufferConfig::Immediate);
        let state = make_state_from_config(cfg);
        register(&state, "case-1");

        emit_result(
            &"case-1".to_string(),
            &"x".to_string(),
            ConstraintType::Response,
            ConstraintResultType::Violated,
            "v".to_string(),
            state.clone(),
            ResultTrigger::Event,
            Some(EventInstanceId::new_v4()),
        )
        .await;

        assert!(matches!(
            read_result(&state, "case-1", "x"),
            ConstraintResultType::Violated
        ));
        // No buffer entry for Immediate.
        assert!(
            !state
                .result_send_buffer
                .lock()
                .unwrap()
                .has_pending(&"case-1".to_string(), &"x".to_string())
        );
    }

    // ── GracePeriod policy ───────────────────────────────────────────────

    #[tokio::test(start_paused = true)]
    async fn grace_period_writes_cc_results_immediately_and_buffers_send() {
        let mut buffers = HashMap::new();
        buffers.insert(
            "x".to_string(),
            ResultBufferConfig::GracePeriod {
                duration: Duration::from_secs(5),
            },
        );
        let cfg = make_config_with_buffers(vec![], buffers, ResultBufferConfig::Immediate);
        let state = make_state_from_config(cfg);
        register(&state, "case-1");

        emit_result(
            &"case-1".to_string(),
            &"x".to_string(),
            ConstraintType::Response,
            ConstraintResultType::Violated,
            "v".to_string(),
            state.clone(),
            ResultTrigger::Event,
            Some(EventInstanceId::new_v4()),
        )
        .await;

        // cc_results is current right away
        assert!(matches!(
            read_result(&state, "case-1", "x"),
            ConstraintResultType::Violated
        ));

        // Buffer holds the pending send
        assert!(
            state
                .result_send_buffer
                .lock()
                .unwrap()
                .has_pending(&"case-1".to_string(), &"x".to_string())
        );

        // Advance through the grace period — the spawned task takes the entry
        advance_and_settle(6).await;

        assert!(
            !state
                .result_send_buffer
                .lock()
                .unwrap()
                .has_pending(&"case-1".to_string(), &"x".to_string())
        );
    }

    #[tokio::test(start_paused = true)]
    async fn grace_period_superseding_update_replaces_buffered_value() {
        let mut buffers = HashMap::new();
        buffers.insert(
            "x".to_string(),
            ResultBufferConfig::GracePeriod {
                duration: Duration::from_secs(5),
            },
        );
        let cfg = make_config_with_buffers(vec![], buffers, ResultBufferConfig::Immediate);
        let state = make_state_from_config(cfg);
        register(&state, "case-1");

        emit_result(
            &"case-1".to_string(),
            &"x".to_string(),
            ConstraintType::Response,
            ConstraintResultType::Violated,
            "tentative".to_string(),
            state.clone(),
            ResultTrigger::Event,
            Some(EventInstanceId::new_v4()),
        )
        .await;

        // Halfway through the grace window, supersede with a corrected result
        advance_and_settle(2).await;

        emit_result(
            &"case-1".to_string(),
            &"x".to_string(),
            ConstraintType::Response,
            ConstraintResultType::Satisfied,
            "corrected".to_string(),
            state.clone(),
            ResultTrigger::Event,
            Some(EventInstanceId::new_v4()),
        )
        .await;

        // cc_results reflects the corrected value
        assert!(matches!(
            read_result(&state, "case-1", "x"),
            ConstraintResultType::Satisfied
        ));

        // The buffer still has a pending entry but it's the corrected one
        {
            let buf = state.result_send_buffer.lock().unwrap();
            let pending = buf
                .get(&"case-1".to_string(), &"x".to_string())
                .expect("pending entry should exist");
            assert!(matches!(pending.result, ConstraintResultType::Satisfied));
            assert_eq!(pending.info, "corrected");
        }

        // Run past both windows; the buffer drains
        advance_and_settle(10).await;
        assert!(
            !state
                .result_send_buffer
                .lock()
                .unwrap()
                .has_pending(&"case-1".to_string(), &"x".to_string())
        );
    }

    // ── Revocable policy ─────────────────────────────────────────────────

    #[tokio::test(start_paused = true)]
    async fn revocable_writes_immediately_and_keeps_buffer_open() {
        let mut buffers = HashMap::new();
        buffers.insert(
            "x".to_string(),
            ResultBufferConfig::Revocable {
                duration: Duration::from_secs(5),
            },
        );
        let cfg = make_config_with_buffers(vec![], buffers, ResultBufferConfig::Immediate);
        let state = make_state_from_config(cfg);
        register(&state, "case-1");

        emit_result(
            &"case-1".to_string(),
            &"x".to_string(),
            ConstraintType::Response,
            ConstraintResultType::Violated,
            "v".to_string(),
            state.clone(),
            ResultTrigger::Event,
            Some(EventInstanceId::new_v4()),
        )
        .await;

        // cc_results updated right away (revocable sends immediately)
        assert!(matches!(
            read_result(&state, "case-1", "x"),
            ConstraintResultType::Violated
        ));
        // and the buffer holds the entry for the revocable window
        assert!(
            state
                .result_send_buffer
                .lock()
                .unwrap()
                .has_pending(&"case-1".to_string(), &"x".to_string())
        );

        // Advance past the window — buffer is taken (finalised)
        advance_and_settle(6).await;
        assert!(
            !state
                .result_send_buffer
                .lock()
                .unwrap()
                .has_pending(&"case-1".to_string(), &"x".to_string())
        );
    }

    // ── emit_result_immediate ────────────────────────────────────────────

    #[tokio::test]
    async fn emit_immediate_writes_cc_results_and_does_not_buffer() {
        let cfg = make_config_with_buffers(
            vec![],
            HashMap::new(),
            ResultBufferConfig::GracePeriod {
                // Even with grace-period default policy, emit_result_immediate
                // bypasses the buffer entirely.
                duration: Duration::from_secs(60),
            },
        );
        let state = make_state_from_config(cfg);
        register(&state, "case-1");

        emit_result_immediate(
            &"case-1".to_string(),
            &"x".to_string(),
            ConstraintType::Response,
            ConstraintResultType::Satisfied,
            "vacuous".to_string(),
            state.clone(),
            ResultTrigger::Event,
            Some(EventInstanceId::new_v4()),
        )
        .await;

        assert!(matches!(
            read_result(&state, "case-1", "x"),
            ConstraintResultType::Satisfied
        ));
        assert!(
            !state
                .result_send_buffer
                .lock()
                .unwrap()
                .has_pending(&"case-1".to_string(), &"x".to_string())
        );
    }
}
