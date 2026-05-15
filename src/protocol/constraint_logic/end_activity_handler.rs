use crate::impl_constraint_handler_fields;
use crate::protocol::constraint_logic::constraint_handler_trait::ConstraintHandler;
use crate::state::app_state::AppState;
use crate::types::TracedEvent;
use async_trait::async_trait;

pub struct EndActivityHandler {
    pub event_id: String,
    pub constraint_id: String,
}

#[async_trait]
impl ConstraintHandler for EndActivityHandler {
    impl_constraint_handler_fields!(EndActivityHandler);

    // notify all handlers that this case has ended
    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::info!("received end event for case {}", event.case_id);
        for handler in state.handlers.all() {
            handler.handle_case_end(&event.case_id, state.clone()).await
        }
    }
}
