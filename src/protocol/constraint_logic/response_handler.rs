use process_mining::core::event_data::case_centric::Event;

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
    CCMessage, CCMessageType, ConstraintId, ConstraintType, PeerId, Token, TokenId, TracedEvent,
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
            event.trace_id.clone(),
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
                let mut store = state.token_store.lock().unwrap();
                let response_satisfied =
                    !store.has_open_token(&message.case_id, &self.constraint_id);
                store.remove_out(&message.case_id, &self.constraint_id);
                // TODO: do something with the constraint result
            }
            _ => {}
        }
    }
}

pub struct ResponseConsumerHandler {
    pub constraint_id: ConstraintId,
    pub event_id: String,
    pub producer_peers: Vec<PeerId>,
}

#[async_trait]
impl ConstraintHandler for ResponseConsumerHandler {
    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        let delete_trigger_messages: Vec<CCMessage> = self
            .producer_peers
            .iter()
            .map(|&p| {
                CCMessage::delete_token(
                    state.config.node_id,
                    p,
                    event.trace_id.clone(),
                    self.constraint_id.clone(),
                    TokenId::nil(),
                )
            })
            .collect();
        // TODO: send messages
    }
    async fn handle_message(&self, _state: AppState, _message: CCMessage) {}
}
