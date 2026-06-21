use std::net::SocketAddr;

use async_trait::async_trait;
use chrono::Utc;

use crate::{
    config::node_config::{AppearanceCounterReset, MinMaxMode},
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
        CaseId, ConstraintHandlerId, ConstraintResultType, ConstraintType, Token, TokenPayload,
        TracedEvent,
    },
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
            "appearance counter handler: received configured event for constraint {}; event={}",
            self.constraint_id,
            event
        );

        let mut meta_tokens = state.meta_token_store.lock().unwrap();
        let count = match meta_tokens.get_token_mut(
            &event.case_id,
            &self.constraint_id,
            self.handler_id,
        ) {
            Some(counter_token) => {
                counter_token.payload.increment_counter();
                counter_token.payload.get_count()
            }
            None => {
                log::info!(
                    "appearance counter handler: starting counter for event_id={} constraint_id={} case_id={}",
                    self.event_id,
                    self.constraint_id,
                    event.case_id
                );

                let counter_token = Token::new_with_payload(
                    event.case_id.clone(),
                    self.constraint_id.clone(),
                    ConstraintType::Appearance,
                    None,
                    state.config.node_id,
                    TokenPayload::Counter(1),
                );
                meta_tokens.insert_meta_token(counter_token.clone(), self.handler_id);
                counter_token.payload.get_count()
            }
        };
        log::info!("appearance counter handler: counter is now at {}", count);
    }

    async fn handle_message(&self, state: AppState, message: CCMessage) {
        log::info!(
            "appearance counter handler: received message for constraint {}; message={}",
            self.constraint_id,
            message
        );
        log::debug!("message={:?}", message);

        if message.message_type != CCMessageType::TriggerCheck {
            log::warn!(
                "appearance counter handler: unexpected message type {:?} for constraint {}, ignoring",
                message.message_type,
                self.constraint_id
            );
            return;
        }

        let count: u64 = self.get_and_reset_counter(&message.case_id, state.clone());
        let (info, result) = self.evaluate_counter(count);

        emit_result(
            &message.case_id,
            &self.constraint_id,
            ConstraintType::Appearance,
            result,
            info,
            state.clone(),
        )
        .await;
    }

    async fn handle_case_end(&self, case_id: &CaseId, state: AppState) {
        log::info!(
            "appearance counter handler: received case end signal for constraint {}; case={}",
            self.constraint_id,
            case_id
        );

        let result_exists = {
            let results = state.cc_results.lock().unwrap();
            results
                .get_constraint(case_id, &self.constraint_id)
                .is_some()
        };
        if !result_exists {
            // produce result because none was emitted yet
            let count: u64 = self.get_and_reset_counter(case_id, state.clone());
            let (info, result) = self.evaluate_counter(count);

            emit_result(
                case_id,
                &self.constraint_id,
                ConstraintType::Appearance,
                result,
                info,
                state.clone(),
            )
            .await;
        }
    }

    async fn clean_up_state(&self, case_id: &CaseId, _state: AppState) {
        log::info!(
            "appearance counter handler: clean up state for ({}/{})",
            case_id,
            self.constraint_id
        );
    }
}

impl AppearanceCounterHandler {
    fn get_and_reset_counter(&self, case_id: &CaseId, state: AppState) -> u64 {
        let mut meta_tokens = state.meta_token_store.lock().unwrap();
        match meta_tokens.get_token_mut(case_id, &self.constraint_id, self.handler_id) {
            Some(token) => {
                let count = token.payload.get_count();
                // reset counter while store is locked
                if let Some(counter_token) =
                    meta_tokens.get_token_mut(case_id, &self.constraint_id, self.handler_id)
                {
                    match self.reset {
                        AppearanceCounterReset::Keep => {}
                        AppearanceCounterReset::Subtract => {
                            counter_token.payload.subtract_counter(self.frequency);
                        }
                        AppearanceCounterReset::Zero => {
                            counter_token.payload.reset_counter();
                        }
                    }
                }
                count
            }
            None => 0,
        }
    }
    fn evaluate_counter(&self, count: u64) -> (String, ConstraintResultType) {
        match self.mode {
            MinMaxMode::Min => {
                if count >= self.frequency {
                    (
                        format!(
                            "CounterMin: counter {:?} >= limit {:?}",
                            count, self.frequency
                        ),
                        ConstraintResultType::Satisfied,
                    )
                } else {
                    (
                        format!(
                            "CounterMin: counter {:?} < limit {:?}",
                            count, self.frequency
                        ),
                        ConstraintResultType::Violated,
                    )
                }
            }
            MinMaxMode::Max => {
                if count <= self.frequency {
                    (
                        format!(
                            "CounterMax: counter {:?} <= limit {:?}",
                            count, self.frequency
                        ),
                        ConstraintResultType::Satisfied,
                    )
                } else {
                    (
                        format!(
                            "CounterMax: counter {:?} > limit {:?}",
                            count, self.frequency
                        ),
                        ConstraintResultType::Violated,
                    )
                }
            }
        }
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
            "appearance trigger handler: received configured event for constraint {}; event={}",
            self.constraint_id,
            event
        );

        log::info!("appearance trigger handler: triggering counter evaluation on counter peers");

        let message = PeerMessage::conformance(CCMessage::trigger_check(
            event.case_id.clone(),
            self.constraint_id.clone(),
            event.timestamp_utc().unwrap_or(Utc::now()),
        ));
        for &peer_address in self.counter_peers.iter() {
            if let Err(e) = send_to_peer(peer_address, message.clone(), state.clone()).await {
                log::error!(
                    "appearance trigger handler: error sending cc trigger message to {}: {}",
                    peer_address,
                    e
                );
            }
        }
    }

    async fn clean_up_state(&self, case_id: &CaseId, _state: AppState) {
        log::info!(
            "appearance trigger handler: clean up state for ({}/{})",
            case_id,
            self.constraint_id
        );
    }
}
