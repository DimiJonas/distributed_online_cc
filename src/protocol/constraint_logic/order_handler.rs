use std::net::SocketAddr;

use async_trait::async_trait;
use chrono::Utc;

use crate::{
    impl_constraint_handler_fields,
    network::message_types::CCMessage,
    protocol::constraint_logic::constraint_handler_trait::ConstraintHandler,
    state::app_state::AppState,
    types::{ConstraintType, Token, TracedEvent},
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
    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        if let Some(sperr_token) = {
            let store = state.token_store.lock().unwrap();
            store.get_in_cloned(&event.case_id, &self.constraint_id)
        } {
            // order second node already pushed a token - can produce constraint check result
            // check if already some previous order first events were recorded (in token out)
            // then check their timestamps with order second timestamp
            // check also this events timestamp
            // delete order first token in token out
        } else {
            // no order-second-token received yet - just record the event time in the token out token payload that is produced by this order first node
            let event_time = match event.timestamp_utc() {
                Some(timestamp) => timestamp,
                None => Utc::now(),
            };
            let mut store = state.token_store.lock().unwrap();

            match store.get_out_mut(&event.case_id, &self.constraint_id) {
                Some(recording_token) => {
                    if let Err(e) = recording_token.push_timestamp(event_time) {
                        log::error!(
                            "tried to add event time {} to token {:?}, but an error occured while serializing payload as timestamp vector: {}",
                            event_time,
                            recording_token,
                            e
                        );
                    }
                }
                None => {
                    let mut recording_token = Token::new(
                        event.case_id.clone(),
                        self.constraint_id.clone(),
                        ConstraintType::Order,
                        event.timestamp_utc(),
                        state.config.node_id,
                        None,
                    );
                    recording_token.push_timestamp(event_time).ok();
                    store.produce_without_overwrite(recording_token);
                }
            };
        };
    }

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
