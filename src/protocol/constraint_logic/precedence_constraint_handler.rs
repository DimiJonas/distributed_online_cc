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
        CaseId, ConstraintHandlerId, ConstraintId, ConstraintResultType, ConstraintType, Token,
        TracedEvent,
    },
};

pub struct PrecedenceProducerHandler {
    pub handler_id: ConstraintHandlerId,
    pub constraint_id: ConstraintId,
    pub event_id: String,
    pub consumer_peers: Vec<SocketAddr>,
}

#[async_trait]
impl ConstraintHandler for PrecedenceProducerHandler {
    impl_constraint_handler_fields!(ResponseProducerHandler);

    fn configured_peers(&self) -> Vec<SocketAddr> {
        self.consumer_peers.clone()
    }

    // produce non overwriting token
    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::info!(
            "precedence producer handler: received configured event for constraint {}; event={}",
            self.constraint_id,
            event
        );

        let producer_event_time = {
            match event.timestamp_utc() {
                Some(timestamp) => timestamp,
                None => {
                    log::warn!(
                        "precedence producer handler: precedence producer event did not contain a timestamp - using current time as event time"
                    );
                    Utc::now()
                }
            }
        };

        // check if token already exists
        let token = {
            let mut token_out = state.token_out.lock().unwrap();
            if token_out.contains_out_token(&event.case_id, &self.constraint_id, self.handler_id) {
                log::debug!(
                    "precedence producer handler: token already exists for case={} constraint={}",
                    event.case_id,
                    self.constraint_id
                );
                return;
            }
            let token = Token::new(
                event.case_id.clone(),
                self.constraint_id.clone(),
                ConstraintType::Precedence,
                Some(producer_event_time),
                state.config.node_id,
            );
            token_out.produce_without_overwrite(token.clone(), self.handler_id);
            log::info!(
                "precedence producer handler: produced token for case={} constraint={}",
                event.case_id,
                self.constraint_id
            );
            token
        };

        // send token
        let message = PeerMessage::conformance(CCMessage::push_token(token));
        for peer_address in self.consumer_peers.iter() {
            if let Err(e) = send_to_peer(*peer_address, message.clone(), state.clone()).await {
                log::error!(
                    "precedence producer handler: error sending push token message to {}: {}",
                    peer_address,
                    e
                );
            }
        }
    }

    async fn clean_up_state(&self, case_id: &CaseId, _state: AppState) {
        log::info!(
            "precedence producer handler: clean up state for ({}/{})",
            case_id,
            self.constraint_id
        );
    }
}

pub struct PrecedenceConsumerHandler {
    pub handler_id: ConstraintHandlerId,
    pub constraint_id: ConstraintId,
    pub event_id: String,
}

#[async_trait]
impl ConstraintHandler for PrecedenceConsumerHandler {
    impl_constraint_handler_fields!(PrecedenceConsumerHandler);

    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::info!(
            "precedence consumer handler: received configured event for constraint {}; event={}",
            self.constraint_id,
            event
        );

        let consumer_event_time = {
            if let Some(timestamp) = event.timestamp_utc() {
                timestamp
            } else {
                log::warn!(
                    "precedence consumer handler: precedence consumer event did not contain a timestamp - using current time as event time"
                );
                Utc::now()
            }
        };

        let producer_token = {
            let token_in = state.token_in.lock().unwrap();
            token_in.get_in_cloned(&event.case_id, &self.constraint_id, self.handler_id)
        };

        let producer_event_time = match producer_token {
            Some(token) => token.event_time.unwrap(),
            None => {
                // producer event has not happened yet
                let info = "producer event has not happened yet".to_string();
                emit_result(
                    &event.case_id,
                    &self.constraint_id,
                    ConstraintResultType::Violated,
                    info,
                    state.clone(),
                )
                .await;
                return;
            }
        };

        // evaluate timestamps
        let (result, info) = {
            if consumer_event_time > producer_event_time {
                (
                    ConstraintResultType::Satisfied,
                    format!(
                        "consumer event happened after producer event: {} > {}",
                        consumer_event_time, producer_event_time
                    ),
                )
            } else {
                (
                    ConstraintResultType::Violated,
                    format!(
                        "consumer event happened before producer event: {} <= {}",
                        consumer_event_time, producer_event_time
                    ),
                )
            }
        };

        emit_result(
            &event.case_id,
            &self.constraint_id,
            result,
            info,
            state.clone(),
        )
        .await;
    }

    async fn handle_message(&self, state: AppState, message: CCMessage) {
        log::info!(
            "precedence consumer handler: received message for constraint {}; message={}",
            self.constraint_id,
            message
        );
        log::debug!("message={:?}", message);

        if message.message_type != CCMessageType::PushToken {
            log::warn!(
                "precedence consumer handler: unexpected message type {:?} for constraint {}, ignoring",
                message.message_type,
                self.constraint_id
            );
            return;
        }

        let new_token = {
            match message.token.clone() {
                Some(token) => token,
                None => {
                    log::error!(
                        "precedence consumer handler: push token message did not contain a token. message={}",
                        message
                    );
                    return;
                }
            }
        };

        let new_event_time = match new_token.event_time {
            Some(timestamp) => timestamp,
            None => {
                log::error!(
                    "precedence consumer handler: token in push token message did not contain a timestamp. message={}",
                    message
                );
                return;
            }
        };

        // save token if none exists yet or event time is earlier than the current one
        {
            let mut token_in = state.token_in.lock().unwrap();
            match token_in.get_in_mut(&message.case_id, &self.constraint_id, self.handler_id) {
                Some(existing_token) => match existing_token.event_time {
                    Some(existing_event_time) => {
                        if new_event_time < existing_event_time {
                            existing_token.event_time = Some(new_event_time);
                            log::info!(
                                "precedence consumer handler: received precedence producer token (update)"
                            );
                        } else {
                            log::info!(
                                "precedence consumer handler: received precedence producer token (no update)"
                            );
                        }
                    }
                    None => {
                        existing_token.event_time = Some(new_event_time);
                        log::info!(
                            "precedence consumer handler: received precedence producer token (insert timestamp)"
                        );
                    }
                },
                None => {
                    token_in.receive_without_overwrite(new_token, self.handler_id);
                    log::info!(
                        "precedence consumer handler: received precedence producer token (insert)"
                    );
                }
            }
        }
    }

    async fn clean_up_state(&self, case_id: &CaseId, _state: AppState) {
        log::info!(
            "precedence consumer handler: clean up state for ({}/{})",
            case_id,
            self.constraint_id
        );
    }
}
