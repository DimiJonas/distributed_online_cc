use std::time::Duration;

use chrono::Utc;
use tokio_util::sync::CancellationToken;

use crate::{
    config::node_config::MinMaxMode,
    conformance_checking::result_emitter::emit_result_at,
    measurement::record::ResultTrigger,
    state::app_state::AppState,
    types::{CaseId, ConstraintId, ConstraintResultType, ConstraintType, EventTime, TokenId},
};

/// waits for a time constraints deadline to pass in event time (based on stream clock)
#[allow(clippy::too_many_arguments)]
pub async fn watchdog_task(
    cancel_token: CancellationToken,
    token_id: TokenId,
    case_id: CaseId,
    constraint_id: ConstraintId,
    deadline: EventTime,
    duration: Duration,
    mode: MinMaxMode,
    state: AppState,
) {
    tokio::select! {
        biased; // this makes the branches being polled top to bottom for the case that both select arms are ready, the cancel token branch is used

        _ = cancel_token.cancelled() => {
            // handle_message cancelled the watchdog because the timer stop event or case end event arrived
            log::info!(
                "time watchdog: cancelled for token {} ({}/{})",
                token_id, constraint_id, case_id
            );
        }

        _ = state.stream_clock.wait_for(deadline) => {
            // the stream time moved past the deadline - stop event never arrived in time
            let (claimed, timer_store_read_at) = {
                let mut timer_store = state.timer_token_store.lock().unwrap();
                (timer_store.fire_timer_token(&token_id), Utc::now())
            };
            if claimed.is_none() {
                log::info!(
                    "time watchdog: token {} ({}/{}) was already settled elsewhere, not emitting",
                    token_id, constraint_id, case_id
                );
                return;
            }

            // remove cancel token (leave parent cancel token unaffected)
            {
                let mut cancel_tokens = state.watchdog_cancel_tokens.lock().unwrap();
                cancel_tokens.remove_token(&token_id);
            }

            // evaluate timeout
            match mode {
                MinMaxMode::Max => {
                    log::warn!(
                        "time watchdog: MAX constraint {}/case {} VIOLATED because token {} passed its deadline {} (limit {:?})",
                        constraint_id, case_id, token_id, deadline, duration
                    );
                    emit_result_at(
                        timer_store_read_at,
                        &case_id,
                        &constraint_id,
                        ConstraintType::Time,
                        ConstraintResultType::Violated,
                        format!(
                            "TimerMax: stream passed the deadline {} ({:?} after the start event) before the stop event arrived (token {})",
                            deadline, duration, token_id
                        ),
                        state.clone(),
                        ResultTrigger::Timer,
                        None,
                    ).await;

                }
                MinMaxMode::Min => {
                    log::info!(
                        "time watchdog: MIN constraint {}/case {}: token {} passed its deadline {}",
                        constraint_id, case_id, token_id, deadline
                    );
                    emit_result_at(
                        timer_store_read_at,
                        &case_id,
                        &constraint_id,
                        ConstraintType::Time,
                        ConstraintResultType::Satisfied,
                        format!(
                            "TimerMin: stream passed {} ({:?} after the start event); stop event never arrived (token {})",
                            deadline, duration, token_id
                        ),
                        state.clone(),
                        ResultTrigger::Timer,
                        None,
                    ).await;

                }
            }
        }
    }
}
