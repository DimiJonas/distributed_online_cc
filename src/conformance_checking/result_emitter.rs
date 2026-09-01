use std::time::Duration;

use chrono::{DateTime, Utc};

/// this module is used to realize the different result publication strategies:
///     - immediate
///     - grace period
///     - revocable result
use crate::{
    measurement::{
        self,
        record::{ResultDropReason, ResultTrigger},
    },
    network::{
        message_types::{CCResultMessage, PeerMessage},
        tcp_router::send_to_conformance_peer,
    },
    state::{
        app_state::AppState,
        result_revision_store::{DecisionStamp, RevisionNumber},
        result_send_buffer::ResultBufferPolicy,
    },
    types::{CaseId, ConstraintId, ConstraintResultType, ConstraintType, EventInstanceId},
};

/// emit a constraint result, applying the configured buffering policy for the given constraint
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
    let decided_at = Utc::now();

    if is_final(&state, case_id, constraint_id) {
        record_drop(
            case_id,
            constraint_id,
            constraint_type,
            result,
            ResultDropReason::ResultAlreadyFinal,
            None,
            result_trigger,
            decided_at,
        );
        return;
    }
    let revision = {
        let mut revisions = state.result_revisions.lock().unwrap();
        revisions.next_revision(case_id, constraint_id)
    };
    emit_result_at(
        decided_at,
        revision,
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

/// emit a constraint result using a stamp that was drawn at decision time
#[allow(clippy::too_many_arguments)]
pub async fn emit_result_stamped(
    stamp: DecisionStamp,
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
        stamp.decided_at,
        stamp.revision,
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

/// draw a decision stamp from the app state
pub fn stamp_decision(
    state: &AppState,
    case_id: &CaseId,
    constraint_id: &ConstraintId,
) -> DecisionStamp {
    state
        .result_revisions
        .lock()
        .unwrap()
        .stamp(case_id, constraint_id)
}

fn is_final(state: &AppState, case_id: &CaseId, constraint_id: &ConstraintId) -> bool {
    state
        .result_revisions
        .lock()
        .unwrap()
        .is_final(case_id, constraint_id)
}

fn mark_final(state: &AppState, case_id: &CaseId, constraint_id: &ConstraintId) {
    state
        .result_revisions
        .lock()
        .unwrap()
        .mark_final(case_id, constraint_id);
}

#[allow(clippy::too_many_arguments)]
fn record_drop(
    case_id: &CaseId,
    constraint_id: &ConstraintId,
    constraint_type: ConstraintType,
    result: ConstraintResultType,
    reason: ResultDropReason,
    revision: Option<RevisionNumber>,
    result_trigger: ResultTrigger,
    decided_at: DateTime<Utc>,
) {
    log::debug!(
        "dropping result for ({}/{}): {} ({:?})",
        constraint_id,
        case_id,
        result,
        reason
    );
    // record dropped result
    measurement::api::result_dropped(
        case_id,
        constraint_id,
        constraint_type,
        result,
        reason,
        revision,
        result_trigger,
        decided_at,
    );
}

/// emit a constraint result, stamping the result with the supplied decision time and revision and applying the configured buffering policy for the given constraint
///
/// - Immediate (zero grace): written to cc_results and finalized and sent right away
/// - GracePeriod: written to cc_results right away (so the local store is always current) but the result is buffered and can be overwritten by later verdicts.
///   when the window expires the result is finalized and sent.
/// - Revocable: written to cc_results and sent right away and every verdict inside the revocable window is sent as well.
///   when the window expires the result is finalized.
#[allow(clippy::too_many_arguments)]
pub async fn emit_result_at(
    decided_at: DateTime<Utc>,
    revision: RevisionNumber,
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

    if is_final(&state, case_id, constraint_id) {
        record_drop(
            case_id,
            constraint_id,
            constraint_type,
            result,
            ResultDropReason::ResultAlreadyFinal,
            Some(revision),
            result_trigger,
            result_receive_time,
        );
        return;
    }

    // check if a newer revision has already been published
    {
        let revisions = state.result_revisions.lock().unwrap();
        if revisions.is_superseded(case_id, constraint_id, revision) {
            drop(revisions);
            record_drop(
                case_id,
                constraint_id,
                constraint_type,
                result,
                ResultDropReason::StaleRevision,
                Some(revision),
                result_trigger,
                result_receive_time,
            );
            return;
        }
    }

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
    measurement::api::result_emitted(
        case_id,
        constraint_id,
        constraint_type,
        result,
        &policy,
        instance_id,
        result_trigger,
        decided_at,
        revision,
    );

    match policy {
        ResultBufferPolicy::GracePeriod { duration } if duration.is_zero() => {
            log::debug!(
                "immediate result configured for ({}/{}): revision {} is final; sending result {:?}",
                case_id,
                constraint_id,
                revision,
                result
            );
            // Immediate: no buffering at all and no window to keep open
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
            publish(
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
            mark_final(&state, case_id, constraint_id);
        }

        ResultBufferPolicy::GracePeriod { duration } => {
            // only update local result store
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

            let opened = {
                let mut buf = state.result_send_buffer.lock().unwrap();
                buf.insert_or_update(
                    case_id.clone(),
                    constraint_id.clone(),
                    result,
                    info,
                    revision,
                    result_receive_time,
                )
            };

            // spawn grace period timer only if the result was the first for its pair
            if opened {
                tokio::spawn(grace_period_timer(
                    duration,
                    state.clone(),
                    case_id.clone(),
                    constraint_id.clone(),
                    constraint_type,
                ));
            }
        }

        ResultBufferPolicy::Revocable { duration } => {
            // write to cc_results and publish right away, but allow overwrite within window
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
            publish(
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

            // check if revocable window is still open
            let opened = {
                let mut buf = state.result_send_buffer.lock().unwrap();
                buf.open_window(case_id.clone(), constraint_id.clone())
            };

            if opened {
                tokio::spawn(revocable_result_timer(
                    state.clone(),
                    duration,
                    case_id.clone(),
                    constraint_id.clone(),
                ));
            }
        }
    }
}

/// after the timer expired: publish the last buffered value once and close the pair
async fn grace_period_timer(
    duration: Duration,
    state: AppState,
    case_id: CaseId,
    constraint_id: ConstraintId,
    constraint_type: ConstraintType,
) {
    tokio::time::sleep(duration).await;

    // get pending result from buffer
    let pending = {
        let mut buf = state.result_send_buffer.lock().unwrap();
        buf.take(&case_id, &constraint_id)
    };

    mark_final(&state, &case_id, &constraint_id);

    if let Some(pending) = pending {
        log::debug!(
            "grace period expired for ({}/{}): revision {} is now final; sending result {:?}",
            case_id,
            constraint_id,
            pending.revision,
            pending.result
        );
        publish(
            &case_id,
            &constraint_id,
            constraint_type,
            pending.result,
            &pending.info,
            state.clone(),
            pending.decided_at,
            pending.revision,
        )
        .await;
    }
}

/// after the timer expired: close the window for the pair (result was published already)
async fn revocable_result_timer(
    state: AppState,
    duration: Duration,
    case_id: CaseId,
    constraint_id: ConstraintId,
) {
    tokio::time::sleep(duration).await;

    mark_final(&state, &case_id, &constraint_id);
    {
        let mut buf = state.result_send_buffer.lock().unwrap();
        buf.close_window(&case_id, &constraint_id);
    }

    log::debug!(
        "revocable window expired for ({}/{}): result is now final",
        case_id,
        constraint_id
    );
}

/// send the constraint check result to the sink
#[allow(clippy::too_many_arguments)]
async fn publish(
    case_id: &CaseId,
    constraint_id: &ConstraintId,
    constraint_type: ConstraintType,
    result: ConstraintResultType,
    info: &str,
    state: AppState,
    created_at: DateTime<Utc>,
    revision: RevisionNumber,
) {
    // record what was actually published
    {
        let mut revisions = state.result_revisions.lock().unwrap();
        revisions.note_published(case_id, constraint_id, revision);
    }

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
                    "publish: sending result message {} to result sink at {}",
                    result_message,
                    address,
                );
                if let Err(e) =
                    send_to_conformance_peer(*address, result_message.clone(), state.clone()).await
                {
                    log::error!(
                        "publish: error sending result message {} to {} (result sink): {}",
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
                "publish: RESULT for case/constraint {}/{} (revision {}) is {}: {}",
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

    if is_final(&state, case_id, constraint_id) {
        record_drop(
            case_id,
            constraint_id,
            constraint_type,
            result,
            ResultDropReason::ResultAlreadyFinal,
            None,
            result_trigger,
            result_receive_time,
        );
        return;
    }

    let revision = {
        let mut revisions = state.result_revisions.lock().unwrap();
        revisions.next_revision(case_id, constraint_id)
    };

    // record result emitted
    measurement::api::result_emitted(
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
        revision,
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
    publish(
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

    mark_final(&state, case_id, constraint_id);
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

    fn config_with(policy: ResultBufferConfig) -> AppState {
        let mut buffers = HashMap::new();
        buffers.insert("x".to_string(), policy);
        let cfg = make_config_with_buffers(vec![], buffers, ResultBufferConfig::Immediate);
        let state = make_state_from_config(cfg);
        register(&state, "case-1");
        state
    }

    async fn emit(state: &AppState, result: ConstraintResultType, info: &str) {
        emit_result(
            &"case-1".to_string(),
            &"x".to_string(),
            ConstraintType::Response,
            result,
            info.to_string(),
            state.clone(),
            ResultTrigger::Event,
            Some(EventInstanceId::new_v4()),
        )
        .await;
    }

    fn is_closed(state: &AppState) -> bool {
        state
            .result_revisions
            .lock()
            .unwrap()
            .is_final(&"case-1".to_string(), &"x".to_string())
    }

    /// the highest revision issued so far. a dropped verdict must not move this: it never got a number
    fn revisions_issued(state: &AppState) -> RevisionNumber {
        state
            .result_revisions
            .lock()
            .unwrap()
            .current_revision(&"case-1".to_string(), &"x".to_string())
    }

    // ── Immediate policy ─────────────────────────────────────────────────

    #[tokio::test]
    async fn immediate_writes_cc_results_synchronously() {
        let cfg = make_config_with_buffers(vec![], HashMap::new(), ResultBufferConfig::Immediate);
        let state = make_state_from_config(cfg);
        register(&state, "case-1");

        emit(&state, ConstraintResultType::Violated, "v").await;

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

    #[tokio::test]
    async fn immediate_is_final_at_once_and_later_verdicts_are_dropped() {
        let cfg = make_config_with_buffers(vec![], HashMap::new(), ResultBufferConfig::Immediate);
        let state = make_state_from_config(cfg);
        register(&state, "case-1");

        emit(&state, ConstraintResultType::Violated, "first").await;
        assert!(is_closed(&state));

        let before = revisions_issued(&state);
        emit(&state, ConstraintResultType::Satisfied, "too late").await;

        // the store still holds the final value and the dropped verdict burnt no revision number
        assert!(matches!(
            read_result(&state, "case-1", "x"),
            ConstraintResultType::Violated
        ));
        assert_eq!(revisions_issued(&state), before);
    }

    // ── GracePeriod policy ───────────────────────────────────────────────

    #[tokio::test(start_paused = true)]
    async fn grace_period_writes_cc_results_immediately_and_buffers_send() {
        let state = config_with(ResultBufferConfig::GracePeriod {
            duration: Duration::from_secs(5),
        });

        emit(&state, ConstraintResultType::Violated, "v").await;

        // cc_results is current right away
        assert!(matches!(
            read_result(&state, "case-1", "x"),
            ConstraintResultType::Violated
        ));
        assert!(
            state
                .result_send_buffer
                .lock()
                .unwrap()
                .has_pending(&"case-1".to_string(), &"x".to_string())
        );
        assert!(!is_closed(&state));

        advance_and_settle(6).await;

        assert!(
            !state
                .result_send_buffer
                .lock()
                .unwrap()
                .has_pending(&"case-1".to_string(), &"x".to_string())
        );
        assert!(is_closed(&state));
    }

    #[tokio::test(start_paused = true)]
    async fn grace_period_superseding_update_replaces_buffered_value() {
        let state = config_with(ResultBufferConfig::GracePeriod {
            duration: Duration::from_secs(5),
        });

        emit(&state, ConstraintResultType::Violated, "tentative").await;
        advance_and_settle(2).await;
        emit(&state, ConstraintResultType::Satisfied, "corrected").await;

        assert!(matches!(
            read_result(&state, "case-1", "x"),
            ConstraintResultType::Satisfied
        ));
        {
            let buf = state.result_send_buffer.lock().unwrap();
            let pending = buf
                .get(&"case-1".to_string(), &"x".to_string())
                .expect("pending entry should exist");
            assert!(matches!(pending.result, ConstraintResultType::Satisfied));
            assert_eq!(pending.info, "corrected");
        }

        advance_and_settle(10).await;
        assert!(
            !state
                .result_send_buffer
                .lock()
                .unwrap()
                .has_pending(&"case-1".to_string(), &"x".to_string())
        );
    }

    /// the window must not be extended by later verdicts: it runs out 5s after the FIRST one, not 5s after the last
    #[tokio::test(start_paused = true)]
    async fn grace_period_window_does_not_slide() {
        let state = config_with(ResultBufferConfig::GracePeriod {
            duration: Duration::from_secs(5),
        });

        emit(&state, ConstraintResultType::Violated, "first").await;
        for _ in 0..4 {
            advance_and_settle(1).await;
            emit(&state, ConstraintResultType::Violated, "again").await;
            assert!(!is_closed(&state), "closed too early");
        }
        // 4s of repeated updates have passed; one more second ends the original window
        advance_and_settle(2).await;
        assert!(is_closed(&state), "the window slid instead of running out");
    }

    // ── Revocable policy ─────────────────────────────────────────────────

    #[tokio::test(start_paused = true)]
    async fn revocable_writes_immediately_and_keeps_window_open() {
        let state = config_with(ResultBufferConfig::Revocable {
            duration: Duration::from_secs(5),
        });

        emit(&state, ConstraintResultType::Violated, "v").await;

        assert!(matches!(
            read_result(&state, "case-1", "x"),
            ConstraintResultType::Violated
        ));
        // revocable stores no value, only the open window
        assert!(
            state
                .result_send_buffer
                .lock()
                .unwrap()
                .is_open(&"case-1".to_string(), &"x".to_string())
        );
        assert!(!is_closed(&state));

        advance_and_settle(6).await;
        assert!(
            !state
                .result_send_buffer
                .lock()
                .unwrap()
                .is_open(&"case-1".to_string(), &"x".to_string())
        );
        assert!(is_closed(&state));
    }

    /// inside the window every verdict is published; after it closes they are dropped
    #[tokio::test(start_paused = true)]
    async fn revocable_accepts_inside_the_window_and_drops_after_it() {
        let state = config_with(ResultBufferConfig::Revocable {
            duration: Duration::from_secs(5),
        });

        emit(&state, ConstraintResultType::Violated, "first").await;
        advance_and_settle(2).await;
        emit(&state, ConstraintResultType::Satisfied, "correction").await;
        assert!(matches!(
            read_result(&state, "case-1", "x"),
            ConstraintResultType::Satisfied
        ));

        advance_and_settle(6).await;
        assert!(is_closed(&state));

        let before = revisions_issued(&state);
        emit(&state, ConstraintResultType::Violated, "too late").await;
        assert!(
            matches!(
                read_result(&state, "case-1", "x"),
                ConstraintResultType::Satisfied
            ),
            "a verdict after the window must not move the result"
        );
        assert_eq!(revisions_issued(&state), before);
    }

    #[tokio::test(start_paused = true)]
    async fn revocable_window_does_not_slide() {
        let state = config_with(ResultBufferConfig::Revocable {
            duration: Duration::from_secs(5),
        });

        emit(&state, ConstraintResultType::Violated, "first").await;
        for _ in 0..4 {
            advance_and_settle(1).await;
            emit(&state, ConstraintResultType::Violated, "again").await;
            assert!(!is_closed(&state), "closed too early");
        }
        advance_and_settle(2).await;
        assert!(is_closed(&state), "the window slid instead of running out");
    }

    // ── stale revisions ──────────────────────────────────────────────────

    /// a stamped emission that lost the race must not move the result backwards
    #[tokio::test(start_paused = true)]
    async fn stamped_out_of_order_emission_is_dropped() {
        let state = config_with(ResultBufferConfig::Revocable {
            duration: Duration::from_secs(30),
        });

        let early = stamp_decision(&state, &"case-1".to_string(), &"x".to_string());
        let late = stamp_decision(&state, &"case-1".to_string(), &"x".to_string());

        // the later decision is emitted first
        emit_result_stamped(
            late,
            &"case-1".to_string(),
            &"x".to_string(),
            ConstraintType::Response,
            ConstraintResultType::Satisfied,
            "late decision".to_string(),
            state.clone(),
            ResultTrigger::Event,
            None,
        )
        .await;

        emit_result_stamped(
            early,
            &"case-1".to_string(),
            &"x".to_string(),
            ConstraintType::Response,
            ConstraintResultType::Violated,
            "early decision, arrives second".to_string(),
            state.clone(),
            ResultTrigger::Event,
            None,
        )
        .await;

        assert!(
            matches!(
                read_result(&state, "case-1", "x"),
                ConstraintResultType::Satisfied
            ),
            "a superseded revision must not overwrite a newer one"
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
        // and it closes the pair, so the configured 60s grace policy cannot reopen it
        assert!(is_closed(&state));
    }
}
