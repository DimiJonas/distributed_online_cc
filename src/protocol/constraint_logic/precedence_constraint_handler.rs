use std::net::SocketAddr;

use async_trait::async_trait;
use chrono::Utc;

use crate::{
    impl_constraint_handler_fields,
    network::{
        message_types::{CCMessage, CCMessageType, PeerMessage},
        tcp_router::send_to_peer,
    },
    protocol::constraint_logic::constraint_handler_trait::ConstraintHandler,
    state::app_state::AppState,
    types::{
        CaseId, ConstraintId, ConstraintResultType, ConstraintType, EventTime, Token, TracedEvent,
    },
};

pub struct PrecedenceProducerHandler {
    pub constraint_id: ConstraintId,
    pub event_id: String,
}

#[async_trait]
impl ConstraintHandler for PrecedenceProducerHandler {
    impl_constraint_handler_fields!(ResponseProducerHandler);

    // produce non overwriting token
    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::debug!(
            "received configured event for constraint {}; event={}",
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
            let mut store = state.token_store.lock().unwrap();
            store.produce_without_overwrite(token.clone())
        };

        if inserted {
            log::info!("produced token: {}", token);
            log::debug!("produced token: {:?}", token);
        } else {
            log::info!(
                "did not produce token because there already exists one for the this case ({})",
                event.case_id
            );
        }
    }

    // check if token for constraint exists (producer event happened before consumer event) and create constraint check result
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
                log::warn!(
                    "cc trigger message did not contain a timestamp. considering current time as consumer event time. message={}",
                    message
                );
                Utc::now()
            }
        };

        log::info!(
            "creating constraint check result for constraint {}; case={}",
            self.constraint_id,
            &message.case_id
        );

        let (constraint_result, info) = {
            let store = state.token_store.lock().unwrap();
            match store.contains_out_token_before(
                &message.case_id,
                &self.constraint_id,
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

        {
            let mut result_store = state.cc_results.lock().unwrap();
            result_store.update(
                &message.case_id,
                &self.constraint_id,
                constraint_result,
                info,
            );

            // TODO: send result somewhere after some grace period and delete it instead of printing
            log::info!(
                "precedence constraint check result after termination {}",
                result_store
                    .get_constraint(&message.case_id, &self.constraint_id)
                    .unwrap()
            );
            log::debug!(
                "precedence constraint check result after termination {:?}",
                result_store
                    .get_constraint(&message.case_id, &self.constraint_id)
                    .unwrap()
            )
        }
    }
}

pub struct PrecedenceConsumerHandler {
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
        log::debug!(
            "received configured event for constraint {}; event={}",
            self.constraint_id,
            event
        );

        let event_timestamp = {
            if let Some(timestamp) = event.timestamp_utc() {
                timestamp
            } else {
                Utc::now()
            }
        };

        log::info!("notifying precedence producer peers that consumer event happened");
        log::debug!("precedence producer peers: {:?}", self.producer_peers);
        for &peer_addr in self.producer_peers.iter() {
            send_cc_trigger_message(
                peer_addr,
                event.case_id.clone(),
                self.constraint_id.clone(),
                event_timestamp,
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
        "sending cc trigger message for constraint {} to peer {}",
        constraint_id,
        peer_address
    );

    send_to_peer(peer_address, message, state.clone()).await;
}
