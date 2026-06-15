use std::net::SocketAddr;

use async_trait::async_trait;
use chrono::Utc;

use crate::{
    impl_constraint_handler_fields,
    network::{
        message_types::{CCMessage, CCMessageType, PeerMessage},
        tcp_router::send_to_peer,
    },
    protocol::{
        constraint_logic::constraint_handler_trait::ConstraintHandler, result_emitter::emit_result,
    },
    state::app_state::AppState,
    types::{
        CaseId, ConstraintHandlerId, ConstraintId, ConstraintResultType, ConstraintType, EventTime,
        Token, TracedEvent,
    },
};

pub struct PrecedenceProducerHandler {
    pub handler_id: ConstraintHandlerId,
    pub constraint_id: ConstraintId,
    pub event_id: String,
}

#[async_trait]
impl ConstraintHandler for PrecedenceProducerHandler {
    impl_constraint_handler_fields!(ResponseProducerHandler);

    // produce non overwriting token
    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::info!(
            "precedence producer handler: received configured event for constraint {}; event={}",
            self.constraint_id,
            event
        );

        let token = Token::new(
            event.case_id.clone(),
            self.constraint_id.clone(),
            ConstraintType::Precedence,
            event.timestamp_utc(),
            state.config.node_id,
        );
        let inserted = {
            let mut token_out = state.token_out.lock().unwrap();
            token_out.produce_without_overwrite(token.clone(), self.handler_id)
        };

        if inserted {
            log::info!("precedence producer handler: produced token: {}", token);
            log::debug!("token={:?}", token);
        } else {
            log::info!(
                "precedence producer handler: did not produce token because there already exists one for the this case ({})",
                event.case_id
            );
        }
    }

    // check if token for constraint exists (producer event happened before consumer event) and create constraint check result
    async fn handle_message(&self, state: AppState, message: CCMessage) {
        log::info!(
            "precedence producer handler: received message for constraint {}; message={}",
            self.constraint_id,
            message
        );
        log::debug!("message={:?}", message);

        if message.message_type != CCMessageType::TriggerCheck {
            log::warn!(
                "precedence producer handler: unexpected message type {:?} for constraint {}, ignoring",
                message.message_type,
                self.constraint_id
            );
            return;
        }

        let consumer_event_time = {
            if let Some(event_time) = message.timestamp {
                event_time
            } else {
                log::error!(
                    "precedence producer handler: cc trigger message did not contain a timestamp. cannot evaluate the constraint. message={}",
                    message
                );
                return;
            }
        };

        let (constraint_result, info) = {
            let token_out = state.token_out.lock().unwrap();
            match token_out.contains_out_token_before(
                &message.case_id,
                &self.constraint_id,
                self.handler_id,
                consumer_event_time,
            ) {
                true => (
                    ConstraintResultType::Satisfied,
                    format!(
                        "producer token before consumer event time {} exists",
                        consumer_event_time
                    ),
                ),
                false => (
                    ConstraintResultType::Violated,
                    format!(
                        "no producer token before consumer event time {} exists",
                        consumer_event_time
                    ),
                ),
            }
        };
        emit_result(
            &message.case_id,
            &self.constraint_id,
            constraint_result,
            info,
            state.clone(),
        )
        .await;
    }

    async fn clean_up_state(&self, case_id: &CaseId, state: AppState) {
        log::info!(
            "precedence producer handler: clean up state for ({}/{})",
            case_id,
            self.constraint_id
        );
        {
            let mut token_in = state.token_in.lock().unwrap();
            token_in.remove_in(case_id, &self.constraint_id, self.handler_id);
        }
        {
            let mut token_out = state.token_out.lock().unwrap();
            token_out.remove_out(case_id, &self.constraint_id, self.handler_id);
        }
    }
}

pub struct PrecedenceConsumerHandler {
    pub handler_id: ConstraintHandlerId,
    pub constraint_id: ConstraintId,
    pub event_id: String,
    pub producer_peers: Vec<SocketAddr>,
}

#[async_trait]
impl ConstraintHandler for PrecedenceConsumerHandler {
    impl_constraint_handler_fields!(PrecedenceConsumerHandler);

    fn configured_peers(&self) -> Vec<SocketAddr> {
        self.producer_peers.clone()
    }

    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::info!(
            "precedence consumer handler: received configured event for constraint {}; event={}",
            self.constraint_id,
            event
        );

        let event_time = {
            if let Some(timestamp) = event.timestamp_utc() {
                timestamp
            } else {
                log::warn!(
                    "precedence consumer handler: precedence consumer event did not contain a timestamp - using current time as event time"
                );
                Utc::now()
            }
        };

        log::info!(
            "precedence consumer handler: notifying precedence producer peers that consumer event happened"
        );
        for &peer_addr in self.producer_peers.iter() {
            send_cc_trigger_message(
                peer_addr,
                event.case_id.clone(),
                self.constraint_id.clone(),
                event_time,
                state.clone(),
            )
            .await;
        }
    }
}

async fn send_cc_trigger_message(
    peer_address: SocketAddr,
    case_id: CaseId,
    constraint_id: ConstraintId,
    event_timestamp: EventTime,
    state: AppState,
) {
    let message = PeerMessage::conformance(CCMessage::trigger_check(
        case_id,
        constraint_id.clone(),
        event_timestamp,
    ));

    log::info!(
        "precedence consumer handler: sending cc trigger message for constraint {} to peer {}",
        constraint_id,
        peer_address
    );

    if let Err(e) = send_to_peer(peer_address, message, state.clone()).await {
        log::error!(
            "precedence consumer handler: error sending cc trigger message to {}: {}",
            peer_address,
            e
        );
    }
}
