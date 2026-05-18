use std::net::SocketAddr;

use crate::impl_constraint_handler_fields;
use crate::network::tcp_router::send_to_peer;
/*
    a Response constraint defines that after Event/Activity A happened, Event/Activity B must also happen at some point after that

    implementation:
        node A observes event/activity A is executed
        node A produces a token in its token-out area that indicates for case X the event/activity A happened
        a node B that observes event/activity B for case X triggers the deletion of the token on node A
        after termination of case X node A checks if there is still a token in its token-out area

    configuration:
        on node A:
            [[constraints]]
            constraint_role = "ResponseProducer"
            constraint_id = "response_test"
            event_id = "response_producer_event"
        on node B:
            [[constraints]]
            constraint_role = "ResponseConsumer"
            constraint_id = "response_test"
            event_id = "response_consumer_event"
            producer_peers = ["127.0.0.1:8001"]

*/
use crate::network::message_types::{CCMessage, CCMessageType, PeerMessage};
use crate::protocol::constraint_logic::constraint_handler_trait::ConstraintHandler;
use crate::state::app_state::AppState;
use crate::types::{
    CaseId, ConstraintId, ConstraintResultType, ConstraintType, Token, TokenId, TracedEvent,
};
use async_trait::async_trait;

pub struct ResponseProducerHandler {
    pub constraint_id: ConstraintId,
    pub event_id: String,
}

#[async_trait]
impl ConstraintHandler for ResponseProducerHandler {
    impl_constraint_handler_fields!(ResponseProducerHandler);

    // create token that event happened
    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::debug!(
            "received configured event for constraint {}; event={}",
            self.constraint_id,
            event
        );

        let token = Token::new(
            event.case_id.clone(),
            self.constraint_id.clone(),
            ConstraintType::Response,
            state.config.node_id,
            None,
        );
        {
            let mut store = state.token_store.lock().unwrap();
            store.produce(token.clone());
        }

        log::info!("produced token: {}", token);
        log::debug!("produced token: {:?}", token);
    }

    // delete produced token
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

        if message.message_type == CCMessageType::DeleteToken {
            let mut store = state.token_store.lock().unwrap();
            if let Some(token) = store.remove_out(&message.case_id, &self.constraint_id) {
                log::info!("removed token {}", token);
                log::debug!("removed token {:?}", token);
            }
        }
    }

    // check if token still exists and set constraint result accordingly
    async fn handle_case_end(&self, case_id: &CaseId, state: AppState) {
        log::info!(
            "received case end signal for constraint {}; case={}",
            self.constraint_id,
            case_id
        );

        log::info!("creating constraint check result");
        // create constraint result (with info message)
        let (constraint_result, token) = {
            let mut token_store = state.token_store.lock().unwrap();
            let constraint_result: ConstraintResultType =
                match token_store.has_open_token(case_id, &self.constraint_id) {
                    true => ConstraintResultType::Violated,
                    false => ConstraintResultType::Satisfied,
                };
            (
                constraint_result,
                token_store.remove_out(case_id, &self.constraint_id),
            )
        };
        let info = match token {
            Some(t) => format!(
                "token still exists. id: {} timestamp: {}",
                t.id, t.created_at
            ),
            None => "no tokens exist".to_string(),
        };

        // handle constraint result
        // - right now its just saved in the result store and printed
        {
            let mut result_store = state.cc_results.lock().unwrap();
            result_store.update(case_id, &self.constraint_id, constraint_result, info);

            // TODO: send result somewhere after some grace period and delete it instead of printing
            log::info!(
                "response constraint check result after termination {}",
                result_store
                    .get_constraint(case_id, &self.constraint_id)
                    .unwrap()
            );
            log::debug!(
                "response constraint check result after termination {:?}",
                result_store
                    .get_constraint(case_id, &self.constraint_id)
                    .unwrap()
            )
        }
    }
}

pub struct ResponseConsumerHandler {
    pub constraint_id: ConstraintId,
    pub event_id: String,
    pub producer_peers: Vec<SocketAddr>,
}

#[async_trait]
impl ConstraintHandler for ResponseConsumerHandler {
    impl_constraint_handler_fields!(ResponseConsumerHandler);

    fn configured_peers(&self) -> Vec<SocketAddr> {
        self.producer_peers.clone()
    }

    // trigger token deletion on all configured response producers
    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::debug!(
            "received configured event for constraint {}; event={}",
            self.constraint_id,
            event
        );

        log::info!("triggering token deletion on response producer peers");
        log::debug!("response producer peers: {:?}", self.producer_peers);
        for &peer_addr in self.producer_peers.iter() {
            send_delete_token(
                peer_addr,
                event.case_id.clone(),
                self.constraint_id.clone(),
                state.clone(),
            )
            .await;
        }
    }
}

// helper method to create and send a message to delete the response consumer token
async fn send_delete_token(
    peer_addr: SocketAddr,
    trace_id: CaseId,
    constraint_id: ConstraintId,
    state: AppState,
) {
    // create message
    let message = PeerMessage::conformance(CCMessage::delete_token(
        trace_id,
        constraint_id,
        TokenId::nil(),
    ));

    // send message
    log::info!("sending delete token message to {}", peer_addr);
    send_to_peer(peer_addr, message, state.clone()).await;
}
