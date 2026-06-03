use std::time::Duration;

use async_trait::async_trait;

use crate::{
    impl_constraint_handler_fields,
    protocol::constraint_logic::constraint_handler_trait::ConstraintHandler,
    state::app_state::AppState,
    types::{ConstraintId, EventId, TimeConstraintMode, TracedEvent},
};

pub struct TimeConstraintHandler {
    pub constraint_id: ConstraintId,
    pub event_id: EventId,
    pub duration: Duration,
    pub mode: TimeConstraintMode,
}

#[async_trait]
impl ConstraintHandler for TimeConstraintHandler {
    impl_constraint_handler_fields!(TimeHandler);

    // notify all handlers that this case has ended
    async fn handle_event(&self, event: &TracedEvent, state: AppState) {}
}
