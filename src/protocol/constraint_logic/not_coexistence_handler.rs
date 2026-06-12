use std::net::SocketAddr;

use async_trait::async_trait;
use chrono::Utc;

use crate::{
    impl_constraint_handler_fields,
    network::message_types::CCMessage,
    protocol::constraint_logic::constraint_handler_trait::ConstraintHandler,
    state::app_state::AppState,
    types::{CaseId, ConstraintHandlerId, ConstraintType, Token, TracedEvent},
};

pub struct NotCoexistenceHandler {
    pub handler_id: ConstraintHandlerId,
    pub event_id: String,
    pub constraint_id: String,
    pub other_peer: SocketAddr,
}

#[async_trait]
impl ConstraintHandler for NotCoexistenceHandler {
    impl_constraint_handler_fields!(NotCoexistenceHandler);

    fn configured_peers(&self) -> Vec<SocketAddr> {
        vec![self.other_peer]
    }

    /// check if block/execute tokens exist
    ///     - yes: evaluate
    ///     - no: produce claim token an negotiate with other_peer, then check afterwards
    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::info!(
            "not coexistence handler: received end event for case {}",
            event.case_id
        );

        // check if token exists
        let meta_token_exists = {
            let meta_tokens = state.meta_token_store.lock().unwrap();
            meta_tokens.contains_meta_token(&event.case_id, &self.constraint_id, self.handler_id)
        };
        if !meta_token_exists {
            // create claim token and negotiate
            log::info!(
                "not coexistence handler: no meta execution token exists yet. producing claim token and starting negotiation"
            );

            let event_time = match event.timestamp_utc() {
                Some(timestamp) => timestamp,
                None => {
                    log::warn!(
                        "not coexistence handler: event did not contain a timestamp - using current time as event time"
                    );
                    Utc::now()
                }
            };
            let claim_token = Token::new(
                event.case_id.clone(),
                self.constraint_id.clone(),
                ConstraintType::NotCoexistence,
                Some(event_time),
                state.config.node_id,
            );
        }

        // evaluate execute permissions
    }

    /// receive a claim token negotiation request and negotiate
    async fn handle_message(&self, state: AppState, message: CCMessage) {
        log::info!(
            "not coexistence handler: received message for constraint {}; message={}",
            self.constraint_id,
            message
        );
        log::debug!("message={:?}", message);
    }

    /// emit a result if none of the events happened
    async fn handle_case_end(&self, case_id: &CaseId, state: AppState) {
        log::info!(
            "not coexistence handler: received case end signal for constraint {}; case={}",
            self.constraint_id,
            case_id
        );
    }

    /// remove execute/block token
    /// possibly also clear claim token?
    async fn clean_up_state(&self, case_id: &CaseId, state: AppState) {
        log::info!(
            "not coexistence handler: clean up state for ({}/{})",
            case_id,
            self.constraint_id
        );
    }
}
