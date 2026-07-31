use std::time::Duration;

use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::{
    config::node_config::MinMaxMode,
    conformance_checking::result_emitter::emit_result,
    measurement::record::ResultTrigger,
    state::app_state::AppState,
    types::{CaseId, ConstraintId, ConstraintResultType, ConstraintType, TokenId},
};

pub async fn watchdog_task(
    cancel_token: CancellationToken,
    token_id: TokenId,
    case_id: CaseId,
    constraint_id: ConstraintId,
    duration: Duration,
    mode: MinMaxMode,
    state: AppState,
) {
    tokio::select! {
        _ = sleep(duration) => {
            // timeout - stop event never arrived

            // remove cancel token (leave parent cancel token unaffected)
            {
                let mut cancel_tokens = state.watchdog_cancel_tokens.lock().unwrap();
                cancel_tokens.remove_token(&token_id);
            }

            // remove the timer token
            {
                let mut timer_store = state.timer_token_store.lock().unwrap();
                timer_store.fire_timer_token(&token_id);
            }

            // evaluate timeout
            match mode {
                MinMaxMode::Max => {
                    log::warn!(
                        "time watchdog: MAX constraint {}/case {} VIOLATED because token {} timed out after {:?}",
                        constraint_id, case_id, token_id, duration
                    );
                    emit_result(
                        &case_id,
                        &constraint_id,
                        ConstraintType::Time,
                        ConstraintResultType::Violated,
                        format!(
                            "TimerMax: timer ran out after {:?} before stop event arrived (token {})",
                            duration, token_id
                        ),
                        state.clone(),
                        ResultTrigger::Timer,
                        None,
                    ).await;

                }
                MinMaxMode::Min => {
                    log::info!(
                        "time watchdog: MIN constraint {}/case {}: token {} timed out",
                        constraint_id, case_id, token_id
                    );
                    emit_result(
                        &case_id,
                        &constraint_id,
                        ConstraintType::Time,
                        ConstraintResultType::Satisfied,
                        format!(
                            "TimerMin: timer ran out after {:?}; stop event never arrived (token {})",
                            duration, token_id
                        ),
                        state.clone(),
                        ResultTrigger::Timer,
                        None,
                    ).await;

                }
            }
        }

        _ = cancel_token.cancelled() => {
            // handle_message cancelled watchdog because timer stop event arrived
            log::info!(
                "time watchdog: cancelled for token {} ({}/{})",
                token_id, constraint_id, case_id
            );
        }
    }
}
