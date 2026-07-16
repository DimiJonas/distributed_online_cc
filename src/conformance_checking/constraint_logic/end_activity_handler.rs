use crate::conformance_checking::case_end_signal_handling::handle_case_end;
use crate::conformance_checking::constraint_logic::constraint_handler_trait::ConstraintHandler;
use crate::impl_constraint_handler_fields;
use crate::state::app_state::AppState;
use crate::types::{CaseEnd, ConstraintHandlerId, TracedEvent};
use async_trait::async_trait;
use chrono::Utc;

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

        let event_time = match event.timestamp_utc() {
            Some(timestamp) => timestamp,
            None => {
                log::warn!(
                    "end ativity handler: event did not contain a timestamp - using current time as event time"
                );
                Utc::now()
            }
        };

        let case_id = event.case_id.clone();

        // signal case end
        handle_case_end(
            state.clone(),
            CaseEnd {
                case_id,
                timestamp: event_time,
            },
        )
        .await;
    }
}
