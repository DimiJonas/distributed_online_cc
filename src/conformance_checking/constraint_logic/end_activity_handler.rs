use crate::conformance_checking::case_end_signal_handling::handle_case_end;
use crate::conformance_checking::constraint_logic::constraint_handler_trait::ConstraintHandler;
use crate::impl_constraint_handler_fields;
use crate::measurement::api::case_end_seen;
use crate::measurement::record::CaseEndSource;
use crate::state::app_state::AppState;
use crate::types::{CaseEnd, ConstraintId, ConstraintRole, EventId, HandlerIndex, TracedEvent};
use async_trait::async_trait;

pub struct EndActivityHandler {
    pub index: HandlerIndex,
    pub role: ConstraintRole,
    pub event_id: EventId,
    pub constraint_id: ConstraintId,
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

        // record case end
        case_end_seen(case_id.clone(), CaseEndSource::EndEvent);

        // signal case end
        handle_case_end(
            state.clone(),
            CaseEnd {
                case_id,
                timestamp: event.timestamp,
            },
        )
        .await;
    }
}
