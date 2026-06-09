use std::time::Duration;

use tokio_util::sync::CancellationToken;

/// this module is used to realize the different result publication strategies:
///     - immediate
///     - grace period
///     - revocable result
use crate::{
    state::{app_state::AppState, result_send_buffer::ResultBufferPolicy},
    types::{CaseId, ConstraintId, ConstraintResultType},
};

/// emit a constraint result, applying the configured buffering policy for the
/// given constraint
///
/// - Immediate (zero grace): writes to cc_results immediately
/// - GracePeriod: writes to cc_results immediately (so the store is always up to date) AND buffers the send; the buffered send fires after the
///   grace window, merging any updates that arrive in the meantime
/// - Revocable: writes to cc_results immediately and keeps the result open to overwrite for the time window, then finalizes
pub async fn emit_result(
    case_id: &CaseId,
    constraint_id: &ConstraintId,
    result: ConstraintResultType,
    info: String,
    state: AppState,
) {
    {
        // TODO: should i remove this or move this to debug?
        let result_store = state.cc_results.lock().unwrap();

        log::info!(
            "constraint check result produced for ({}/{}): {}",
            constraint_id,
            case_id,
            result_store.get_constraint(case_id, constraint_id).unwrap()
        );
    }

    let policy = state.config.result_buffer_policy(constraint_id);

    match policy {
        ResultBufferPolicy::GracePeriod { duration } if duration.is_zero() => {
            log::info!(
                "immediate result configured for ({}/{}): result is final; sending result {:?}",
                case_id,
                constraint_id,
                result
            );
            // Immediate: no buffering at all
            {
                let mut results = state.cc_results.lock().unwrap();
                results.update(case_id, constraint_id, result.clone(), info.clone());
            }
            send_result_outbound(case_id, constraint_id, &result, &info, state.clone()).await;

            call_finalizers(case_id, constraint_id, state.clone()).await;
        }

        ResultBufferPolicy::GracePeriod { duration } => {
            // only update cc result store for now
            {
                let mut results = state.cc_results.lock().unwrap();
                results.update(case_id, constraint_id, result.clone(), info.clone());
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
                cancel_token,
            ));
        }

        ResultBufferPolicy::Revocable { duration } => {
            // write to cc_results and send immediately; allow overwrite within window
            {
                let mut results = state.cc_results.lock().unwrap();
                results.update(case_id, constraint_id, result.clone(), info.clone());
            }
            send_result_outbound(case_id, constraint_id, &result, &info, state.clone()).await;

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
    cancel_token: CancellationToken,
) {
    tokio::select! {
        _ = tokio::time::sleep(duration) => {
            let pending = {
                let mut buf = state.result_send_buffer.lock().unwrap();
                buf.take(&case_id, &constraint_id)
            };
            if let Some(pending) = pending {
                log::info!(
                    "grace period expired for ({}/{}): result is now final; sending result {:?}",
                    case_id, constraint_id, pending.result
                );
                // cc_results already holds the current value; this
                // is the outbound dispatch.
                send_result_outbound(
                    &case_id, &constraint_id,
                    &pending.result, &pending.info, state.clone(),
                ).await;
            }

            call_finalizers(&case_id, &constraint_id, state.clone()).await;
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

            call_finalizers(&case_id, &constraint_id, state.clone()).await;
            // TODO: optionally send a "finalised" signal via TCP.
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
    result: &ConstraintResultType,
    _info: &str,
    _state: AppState,
) {
    // TODO: serialize and send the result somewhere
    log::info!(
        "send_result_outbound: ({}/{}) -> {} (not yet implemented)",
        case_id,
        constraint_id,
        result
    );
}

/// call finalize on the corresponding constraint handlers to clean up state
async fn call_finalizers(case_id: &CaseId, constraint_id: &ConstraintId, state: AppState) {
    log::info!(
        "finalizing constraint {} for case {}",
        constraint_id,
        case_id
    );

    let handlers = state.handlers.clone();
    let handler_refs = handlers.get_by_constraint_id(constraint_id);
    for handler in handler_refs {
        handler.finalize(case_id, state.clone()).await;
    }
}

/// convenience method for handlers that want to finalize a result immediately, skipping the whole buffer logic
pub async fn emit_result_immediate(
    case_id: &CaseId,
    constraint_id: &ConstraintId,
    result: ConstraintResultType,
    info: String,
    state: AppState,
) {
    {
        let mut results = state.cc_results.lock().unwrap();
        results.update(case_id, constraint_id, result.clone(), info.clone());
    }
    send_result_outbound(case_id, constraint_id, &result, &info, state.clone()).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::node_config::ResultBufferConfig;
    use crate::types::ConstraintType;
    use crate::util::test_tools::{make_config_with_buffers, make_state_from_config};
    use std::collections::HashMap;

    fn register(state: &AppState, case: &str, constraint: &str, ctype: ConstraintType) {
        state
            .cc_results
            .lock()
            .unwrap()
            .register_case(case.to_string(), &[(constraint.to_string(), ctype)]);
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
            .clone()
    }

    // ── Immediate policy ─────────────────────────────────────────────────

    #[tokio::test]
    async fn immediate_writes_cc_results_synchronously() {
        let cfg = make_config_with_buffers(vec![], HashMap::new(), ResultBufferConfig::Immediate);
        let state = make_state_from_config(cfg);
        register(&state, "case-1", "x", ConstraintType::Response);

        emit_result(
            &"case-1".to_string(),
            &"x".to_string(),
            ConstraintResultType::Violated,
            "v".to_string(),
            state.clone(),
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
        register(&state, "case-1", "x", ConstraintType::Response);

        emit_result(
            &"case-1".to_string(),
            &"x".to_string(),
            ConstraintResultType::Violated,
            "v".to_string(),
            state.clone(),
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
        register(&state, "case-1", "x", ConstraintType::Response);

        emit_result(
            &"case-1".to_string(),
            &"x".to_string(),
            ConstraintResultType::Violated,
            "tentative".to_string(),
            state.clone(),
        )
        .await;

        // Halfway through the grace window, supersede with a corrected result
        advance_and_settle(2).await;

        emit_result(
            &"case-1".to_string(),
            &"x".to_string(),
            ConstraintResultType::Satisfied,
            "corrected".to_string(),
            state.clone(),
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
        register(&state, "case-1", "x", ConstraintType::Response);

        emit_result(
            &"case-1".to_string(),
            &"x".to_string(),
            ConstraintResultType::Violated,
            "v".to_string(),
            state.clone(),
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
        register(&state, "case-1", "x", ConstraintType::Response);

        emit_result_immediate(
            &"case-1".to_string(),
            &"x".to_string(),
            ConstraintResultType::Satisfied,
            "vacuous".to_string(),
            state.clone(),
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
