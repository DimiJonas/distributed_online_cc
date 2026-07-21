use std::net::SocketAddr;

use crate::config::node_config::DistributedAppearanceCounterReset;
use crate::network::message_types::CCMessage;
use crate::state::app_state::AppState;
use crate::types::{CaseId, ConstraintRole, RequestId, TracedEvent};
use async_trait::async_trait;

#[async_trait]
pub trait ConstraintHandler: Send + Sync {
    fn event_id(&self) -> &str;
    fn constraint_id(&self) -> &str;
    fn role(&self) -> ConstraintRole;
    fn index(&self) -> usize;
    fn counter_reset(&self) -> Option<DistributedAppearanceCounterReset> {
        None
    }

    // return a list of peers that are configured to be connected to in order to coordinate the constraint check
    fn configured_peers(&self) -> Vec<SocketAddr> {
        vec![]
    }

    /// handle the event the handler is configured to listen on
    async fn handle_event(&self, event: &TracedEvent, state: AppState);

    /// handle messages addressing the constraint id of the constraint handler
    async fn handle_message(
        &self,
        _state: AppState,
        _message: CCMessage,
        _request_id: Option<RequestId>,
    ) {
    }

    /// do whats needed after the case end signal came in
    async fn handle_case_end(&self, _case_id: &CaseId, _state: AppState) {}

    /// clean up state after constraint check result was finalized
    async fn clean_up_state(&self, _case_id: &CaseId, _state: AppState) {}
}

// macro that implements the functions event_id() and constraint_id()
// because they are the same for each struct that implements this trait
// and the fields cant be moved to the trait in rust
#[macro_export]
macro_rules! impl_constraint_handler_fields {
    ($type:ty) => {
        fn event_id(&self) -> &str {
            &self.event_id
        }
        fn constraint_id(&self) -> &str {
            &self.constraint_id
        }
        fn role(&self) -> $crate::types::ConstraintRole {
            self.role
        }
        fn index(&self) -> usize {
            self.index
        }
    };
}
