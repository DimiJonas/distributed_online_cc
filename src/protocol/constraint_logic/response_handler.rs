use std::net::SocketAddr;

/*
    a Response constraint defines that after Event/Activity A happened, Event/Activity B must also happen at some point after that

    implementation:
        node A observes event/activity A is executed
        node A produces a token in its token-out area that indicates for case X the event/activity A happened
        a node B that observes event/activity B for case X triggers the deletion of the token on node A
        after termination of case X node A checks if there is still a token in its token-out area

    configuration:
        on node A:
            constraintId: response
            node_type/constraint_role: producer
            eventId: <concept:name>
        on node B:
            constraintId: response
            node_type/constraint_role: consumer
            event_id: <concept:name>
            node_a: <list of node A IPs>

*/
use crate::protocol::constraint_logic::constraint_handler_trait::ConstraintHandler;
use crate::state::app_state::AppState;
use crate::types::{
    CCMessage, CCMessageType, CaseId, ConstraintId, ConstraintResultType, ConstraintType,
    PeerMessage, Token, TokenId, TracedEvent,
};
use async_trait::async_trait;

pub struct ResponseProducerHandler {
    pub constraint_id: ConstraintId,
    pub event_id: String,
}

#[async_trait]
impl ConstraintHandler for ResponseProducerHandler {
    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        let token = Token::new(
            event.case_id.clone(),
            self.constraint_id.clone(),
            ConstraintType::Response,
            state.config.node_id,
            None,
        );
        {
            let mut store = state.token_store.lock().unwrap();
            store.produce(token);
        }
    }
    async fn handle_message(&self, state: AppState, message: CCMessage) {
        match message.message_type {
            CCMessageType::DeleteToken => {
                let mut store = state.token_store.lock().unwrap();
                store.remove_out(&message.case_id, &self.constraint_id);
            }
            CCMessageType::CaseTerminated => {
                let (constraint_result, token) = {
                    let mut token_store = state.token_store.lock().unwrap();
                    let constraint_result: ConstraintResultType =
                        match token_store.has_open_token(&message.case_id, &self.constraint_id) {
                            true => ConstraintResultType::Violated,
                            false => ConstraintResultType::Satisfied,
                        };
                    (
                        constraint_result,
                        token_store.remove_out(&message.case_id, &self.constraint_id),
                    )
                };
                let info = match token {
                    Some(t) => format!(
                        "token still exists. id: {} timestamp: {}",
                        t.id, t.created_at
                    ),
                    None => "no tokens exist".to_string(),
                };
                {
                    let mut result_store = state.cc_results.lock().unwrap();
                    result_store.update(
                        &message.case_id,
                        &self.constraint_id,
                        constraint_result,
                        info,
                    );
                    // TODO: some grace period or something?
                }
            }
            _ => {}
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
    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        for &peer_addr in self.producer_peers.iter() {
            send_delete_token(
                peer_addr,
                event.case_id.clone(),
                self.constraint_id.clone(),
                &state,
            )
            .await;
        }
    }
}

async fn send_delete_token(
    peer_addr: SocketAddr,
    trace_id: CaseId,
    constraint_id: ConstraintId,
    state: &AppState,
) {
    let message = PeerMessage::conformance(CCMessage::delete_token(
        trace_id,
        constraint_id,
        TokenId::nil(),
    ));
    let sender = {
        let peers = state.peers.lock().unwrap();
        peers.get_sender(&peer_addr)
    };
    match sender {
        Some(sender) => {
            sender.send(message).await.unwrap();
            println!("sent delete token message to {}", peer_addr);
        }
        None => println!("peer {} not connected yet", peer_addr),
    }
}
