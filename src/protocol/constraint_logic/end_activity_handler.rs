use crate::impl_constraint_handler_fields;
use crate::protocol::case_closing_task::case_closing_task;
use crate::protocol::constraint_logic::constraint_handler_trait::ConstraintHandler;
use crate::state::app_state::AppState;
use crate::types::{ConstraintHandlerId, TracedEvent};
use async_trait::async_trait;

pub struct EndActivityHandler {
    pub handler_id: ConstraintHandlerId,
    pub event_id: String,
    pub constraint_id: String,
}

#[async_trait]
impl ConstraintHandler for EndActivityHandler {
    impl_constraint_handler_fields!(EndActivityHandler);

    // spawn case closing task
    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::info!(
            "end activity handler: received end event for case {}",
            event.case_id
        );

        let case_id = event.case_id.clone();

        // TODO: consider not spawning here to keep event processing order
        // spawn case closing task
        let handle = tokio::spawn(case_closing_task(state.clone(), case_id.clone()));

        // spawn a task that logs the error if the case closing task panics
        tokio::spawn(async move {
            if let Err(e) = handle.await {
                log::error!(
                    "an error occured in the case closing task for case {}: {}",
                    case_id,
                    e
                );
            }
        });
    }
}
