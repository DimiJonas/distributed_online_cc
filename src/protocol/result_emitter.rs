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

/// Emit a constraint result, applying the configured buffering policy for the
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
    let policy = state.config.result_buffer_policy(constraint_id);

    match policy {
        ResultBufferPolicy::GracePeriod { duration } if duration.is_zero() => {
            // Immediate: no buffering at all
            let mut results = state.cc_results.lock().unwrap();
            results.update(case_id, constraint_id, result, info);
        }

        ResultBufferPolicy::GracePeriod { duration } => {
            // only update cc result store for now
            {
                let mut results = state.cc_results.lock().unwrap();
                results.update(case_id, constraint_id, result.clone(), info.clone());
            }

            // Buffer the *send* and (re)start the grace timer.
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
            // Write to cc_results and send immediately; allow overwrite within window.
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
                    "grace period expired for ({}/{}): sending result {:?}",
                    case_id, constraint_id, pending.result
                );
                // cc_results already holds the current value; this
                // is the outbound dispatch.
                send_result_outbound(
                    &case_id, &constraint_id,
                    &pending.result, &pending.info, state.clone(),
                ).await;
            }
            // None => a superseding update rescheduled the timer;
            // that update's own task will send.
        }
        _ = cancel_token.cancelled() => {
            log::debug!(
                "grace period timer superseded for ({}/{})",
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
            let mut buf = state.result_send_buffer.lock().unwrap();
            buf.take(&case_id, &constraint_id);
            log::info!(
                "revocable window expired for ({}/{}): result is now final",
                case_id, constraint_id
            );
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
    log::debug!(
        "send_result_outbound: ({}/{}) → {:?} (not yet implemented)",
        case_id,
        constraint_id,
        result
    );
}

/// convenience method for handlers that want to finalize a result immediately, skipping the whole buffer logic
pub fn emit_result_immediate(
    case_id: &CaseId,
    constraint_id: &ConstraintId,
    result: ConstraintResultType,
    info: String,
    state: AppState,
) {
    let mut results = state.cc_results.lock().unwrap();
    results.update(case_id, constraint_id, result, info);
}
