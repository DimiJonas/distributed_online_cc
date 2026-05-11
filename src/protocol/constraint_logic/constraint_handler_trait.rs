use crate::state::app_state::AppState;
use crate::types::{CCMessage, CaseId, TracedEvent};
use async_trait::async_trait;

#[async_trait]
pub trait ConstraintHandler: Send + Sync {
    async fn handle_event(&self, event: &TracedEvent, state: AppState);
    async fn handle_message(&self, _state: AppState, _message: CCMessage) {}
    async fn handle_case_end(&self, _case_id: &CaseId, _state: AppState) {}
}
