use std::net::SocketAddr;

use async_trait::async_trait;

use crate::{
    config::node_config::{AppearanceCounterReset, MinMaxMode},
    impl_constraint_handler_fields,
    network::message_types::CCMessage,
    protocol::constraint_logic::constraint_handler_trait::ConstraintHandler,
    state::app_state::AppState,
    types::{CaseId, ConstraintHandlerId, TracedEvent},
};

pub struct AppearanceCounterHandler {
    pub handler_id: ConstraintHandlerId,
    pub event_id: String,
    pub constraint_id: String,
    pub mode: MinMaxMode,
    pub frequency: u64,
    pub reset: AppearanceCounterReset,
}

#[async_trait]
impl ConstraintHandler for AppearanceCounterHandler {
    impl_constraint_handler_fields!(AppearanceCounterHandler);

    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::info!(
            "appearance counter handler: received end event for case {}",
            event.case_id
        );
    }

    async fn handle_message(&self, state: AppState, message: CCMessage) {
        log::info!(
            "appearance counter handler: received message for constraint {}; message={}",
            self.constraint_id,
            message
        );
        log::debug!("message={:?}", message);
    }

    async fn handle_case_end(&self, case_id: &CaseId, state: AppState) {
        log::info!(
            "appearance counter handler: received case end signal for constraint {}; case={}",
            self.constraint_id,
            case_id
        );
    }

    async fn clean_up_state(&self, case_id: &CaseId, state: AppState) {
        log::info!(
            "appearance counter handler: clean up state for ({}/{})",
            case_id,
            self.constraint_id
        );
    }
}

pub struct AppearanceTriggerHandler {
    pub handler_id: ConstraintHandlerId,
    pub event_id: String,
    pub constraint_id: String,
    pub counter_peers: Vec<SocketAddr>,
}

#[async_trait]
impl ConstraintHandler for AppearanceTriggerHandler {
    impl_constraint_handler_fields!(AppearanceTriggerHandler);

    fn configured_peers(&self) -> Vec<SocketAddr> {
        self.counter_peers.clone()
    }

    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::info!(
            "appearance trigger handler: received end event for case {}",
            event.case_id
        );
    }

    async fn handle_message(&self, state: AppState, message: CCMessage) {
        log::info!(
            "appearance trigger handler: received message for constraint {}; message={}",
            self.constraint_id,
            message
        );
        log::debug!("message={:?}", message);
    }

    async fn handle_case_end(&self, case_id: &CaseId, state: AppState) {
        log::info!(
            "appearance trigger handler: received case end signal for constraint {}; case={}",
            self.constraint_id,
            case_id
        );
    }

    async fn clean_up_state(&self, case_id: &CaseId, state: AppState) {
        log::info!(
            "appearance trigger handler: clean up state for ({}/{})",
            case_id,
            self.constraint_id
        );
    }
}
