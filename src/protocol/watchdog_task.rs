use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::{
    protocol::result_emitter::emit_result,
    state::app_state::AppState,
    types::{CaseId, ConstraintId, ConstraintResultType, TimeConstraintMode, TokenId},
};

pub async fn watchdog_task(
    cancel_token: CancellationToken,
    token_id: TokenId,
    case_id: CaseId,
    constraint_id: ConstraintId,
    duration: Duration,
    mode: TimeConstraintMode,
    state: AppState,
) {
    tokio::select! {
        _ = tokio::time::sleep(duration) => {
            // timeout - stop event never arrived

            // remove cancel token (leave parent cancel token unaffected)
            {
                let mut cancel_tokens = state.watchdog_cancel_tokens.lock().unwrap();
                cancel_tokens.remove_token(&token_id);
            }

            // remove the timer token
            {
                let mut store = state.token_store.lock().unwrap();
                store.fire_timer_token(&token_id);
            }

            // evaluate timeout
            match mode {
                TimeConstraintMode::Max => {
                    log::warn!(
                        "time watchdog: MAX constraint {}/case {} VIOLATED because token {} timed out after {:?}",
                        constraint_id, case_id, token_id, duration
                    );
                    emit_result(
                        &case_id,
                        &constraint_id,
                        ConstraintResultType::Violated,
                        format!(
                            "TimerMax: timer ran out after {:?} before stop event arrived (token {})",
                            duration, token_id
                        ),
                        state.clone(),
                    ).await;

                }
                TimeConstraintMode::Min => {
                    log::info!(
                        "time watchdog: MIN constraint {}/case {}: token {} timed out",
                        constraint_id, case_id, token_id
                    );
                    emit_result(
                        &case_id,
                        &constraint_id,
                        ConstraintResultType::Satisfied,
                        format!(
                            "TimerMin: timer ran out after {:?}; stop event never arrived \
                             (vacuously satisfied, token {})",
                            duration, token_id
                        ),
                        state.clone(),
                    ).await;

                }
            }
        }

        _ = cancel_token.cancelled() => {
            // handle_message cancelled watchdog because timer stop event arrived
            log::debug!(
                "time watchdog: cancelled for token {} ({}/{})",
                token_id, constraint_id, case_id
            );
        }
    }
}
