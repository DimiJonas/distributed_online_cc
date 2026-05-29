use std::net::SocketAddr;

use async_trait::async_trait;

use crate::{
    impl_constraint_handler_fields, network::message_types::CCMessage,
    protocol::constraint_logic::constraint_handler_trait::ConstraintHandler,
    state::app_state::AppState, types::TracedEvent,
};

pub struct OrderFirstHandler {
    pub event_id: String,
    pub constraint_id: String,
}

#[async_trait]
impl ConstraintHandler for OrderFirstHandler {
    impl_constraint_handler_fields!(OrderFirstHandler);

    /// record event execution (remember last execution (or all executions) of event before receiving the message from order second for later check with timestamp)
    /// check if the token from order second is existent
    async fn handle_event(&self, event: &TracedEvent, state: AppState) {}

    /// receive token from order second with timestamp of first execution of order second (multiple order seconds possible?)
    /// check check that execution was after order first event(s)
    /// set the received (pushed) token so that all future order first events can be evaluated as a violation
    async fn handle_message(&self, state: AppState, message: CCMessage) {
        log::info!(
            "received message for constraint {}; message={}",
            self.constraint_id,
            message
        );
        log::debug!(
            "received message for constraint {}; message={:?}",
            self.constraint_id,
            message
        );
    }
}

pub struct OrderSecondHandler {
    pub event_id: String,
    pub constraint_id: String,
    pub order_first_peers: Vec<SocketAddr>,
}

#[async_trait]
impl ConstraintHandler for OrderSecondHandler {
    impl_constraint_handler_fields!(OrderSecondHandler);

    /// create token and push it on the order first peers
    /// remember that a token was pushed on the order first peers by using a (meta) token and checking its existence
    async fn handle_event(&self, event: &TracedEvent, state: AppState) {}
}
