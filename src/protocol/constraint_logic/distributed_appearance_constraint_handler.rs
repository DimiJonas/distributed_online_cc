use std::net::SocketAddr;

use async_trait::async_trait;

use crate::{
    config::node_config::{DistributedAppearanceCounterReset, MinMaxMode},
    impl_constraint_handler_fields,
    network::{
        message_types::{CCMessage, CCMessageType, PeerMessage, PeerMessagePayload},
        tcp_router::{send_and_receive, send_to_peer},
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

pub struct DistributedAppearanceCounterHandler {
    pub handler_id: ConstraintHandlerId,
    pub event_id: String,
    pub constraint_id: String,
    pub reset: DistributedAppearanceCounterReset,
}

#[async_trait]
impl ConstraintHandler for DistributedAppearanceCounterHandler {
    impl_constraint_handler_fields!(DistributedAppearanceCounterHandler);

    // increment counter
    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::info!(
            "distributed appearance counter handler: received configured event for constraint {}; event={}",
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
                    "distributed appearance counter handler: starting counter for event_id={} constraint_id={} case_id={}",
                    self.event_id,
                    self.constraint_id,
                    event.case_id
                );

                let counter_token = Token::new_with_payload(
                    event.case_id.clone(),
                    self.constraint_id.clone(),
                    ConstraintType::DistributedAppearance,
                    None,
                    state.config.node_id,
                    TokenPayload::Counter(1),
                );
                meta_tokens.insert_meta_token(counter_token.clone(), self.handler_id);
                counter_token.payload.get_count()
            }
        };
        log::info!(
            "distributed appearance counter handler: counter is now at {}",
            count
        );
    }

    // send counter value to requestor
    // reset counter
    async fn handle_message(&self, state: AppState, message: CCMessage) {
        log::info!(
            "distributed appearance counter handler: received message for constraint {}; message={}",
            self.constraint_id,
            message
        );
        log::debug!("message={:?}", message);

        if message.message_type != CCMessageType::PullTokenRequest {
            log::warn!(
                "distributed appearance counter handler: unexpected message type {:?} for constraint {}, ignoring",
                message.message_type,
                self.constraint_id
            );
            return;
        }

        let request_id = match message.request_id {
            Some(id) => id,
            None => {
                log::error!(
                    "distributed appearance counter handler: message does not contain a request id. cannot respond to it"
                );
                return;
            }
        };

        let sender_address = match message.sender_address {
            Some(addr) => addr,
            None => {
                log::error!(
                    "distributed appearance counter handler: message does not contain sender address. cannot respond to it"
                );
                return;
            }
        };

        // get token and reset counter
        let counter_token = {
            let mut meta_tokens = state.meta_token_store.lock().unwrap();
            match meta_tokens.get_token_mut(&message.case_id, &self.constraint_id, self.handler_id)
            {
                Some(token) => {
                    let token_clone = token.clone();
                    // reset counter
                    match self.reset {
                        DistributedAppearanceCounterReset::Keep => {}
                        DistributedAppearanceCounterReset::Zero => token.payload.reset_counter(),
                    }
                    token_clone
                }
                None => Token::new(
                    message.case_id.clone(),
                    self.constraint_id.clone(),
                    ConstraintType::DistributedAppearance,
                    None,
                    state.config.node_id,
                ),
            }
        };

        // send counter token
        log::info!(
            "sending counter token to {}",
            message.sender_address.unwrap()
        );
        let response =
            PeerMessage::conformance_res(request_id, CCMessage::pull_token_response(counter_token));
        if send_to_peer(sender_address, response, state.clone())
            .await
            .is_err()
        {
            log::error!(
                "distributed appearance counter handler: error responding with counter token to {}",
                sender_address
            );
        }
    }

    async fn clean_up_state(&self, case_id: &CaseId, _state: AppState) {
        log::info!(
            "distributed appearance counter handler: clean up state for ({}/{})",
            case_id,
            self.constraint_id
        );
    }
}

pub struct DistributedAppearanceTriggerHandler {
    pub handler_id: ConstraintHandlerId,
    pub event_id: String,
    pub constraint_id: String,
    pub mode: MinMaxMode,
    pub frequency: u64,
    pub counter_peers: Vec<SocketAddr>,
}

#[async_trait]
impl ConstraintHandler for DistributedAppearanceTriggerHandler {
    impl_constraint_handler_fields!(DistributedAppearanceTriggerHandler);

    fn configured_peers(&self) -> Vec<SocketAddr> {
        self.counter_peers.clone()
    }

    // pull counter values from counter peers and evaluate
    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::info!(
            "distributed appearance trigger handler: received configured event for constraint {}; event={}",
            self.constraint_id,
            event
        );

        let distributed_count: u64 = self
            .get_distributed_count(state.clone(), &event.case_id)
            .await;
        let (info, result) = self.evaluate_count(distributed_count);

        emit_result(
            &event.case_id,
            &self.constraint_id,
            ConstraintType::DistributedAppearance,
            result,
            info,
            state.clone(),
        )
        .await;
    }

    async fn handle_case_end(&self, case_id: &CaseId, state: AppState) {
        log::info!(
            "distributed appearance trigger handler: received case end signal for constraint {}; case={}",
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
            let distributed_count: u64 = self.get_distributed_count(state.clone(), case_id).await;
            let (info, result) = self.evaluate_count(distributed_count);

            emit_result(
                case_id,
                &self.constraint_id,
                ConstraintType::DistributedAppearance,
                result,
                info,
                state.clone(),
            )
            .await;
        }
    }

    async fn clean_up_state(&self, case_id: &CaseId, _state: AppState) {
        log::info!(
            "distributed appearance trigger handler: clean up state for ({}/{})",
            case_id,
            self.constraint_id
        );
    }
}

impl DistributedAppearanceTriggerHandler {
    // accumulate count
    async fn get_distributed_count(&self, state: AppState, case_id: &CaseId) -> u64 {
        let mut distributed_count: u64 = 0;
        for counter_peer in &self.counter_peers {
            let pull_token_request = PeerMessage::conformance_req(CCMessage::pull_token_request(
                case_id.clone(),
                self.constraint_id.clone(),
            ));
            match send_and_receive(*counter_peer, pull_token_request, state.clone()).await {
                Ok(response) => match response.payload {
                    PeerMessagePayload::Conformance(cc_msg) => {
                        if let Some(token) = cc_msg.token {
                            distributed_count += token.payload.get_count();
                        }
                    }
                    _ => {
                        log::error!(
                            "distributed appearance trigger handler: received wrong message type: \"{}\"; expected type: \"conformance\"",
                            response.payload
                        );
                        continue;
                    }
                },
                Err(e) => {
                    log::error!(
                        "distributed appearance trigger handler: error requesting token from {}: {}",
                        counter_peer,
                        e
                    );
                    continue;
                }
            }
        }
        distributed_count
    }

    fn evaluate_count(&self, distributed_count: u64) -> (String, ConstraintResultType) {
        match self.mode {
            MinMaxMode::Min => {
                if distributed_count >= self.frequency {
                    (
                        format!(
                            "CounterMin: counter {:?} >= limit {:?}",
                            distributed_count, self.frequency
                        ),
                        ConstraintResultType::Satisfied,
                    )
                } else {
                    (
                        format!(
                            "CounterMin: counter {:?} < limit {:?}",
                            distributed_count, self.frequency
                        ),
                        ConstraintResultType::Violated,
                    )
                }
            }
            MinMaxMode::Max => {
                if distributed_count <= self.frequency {
                    (
                        format!(
                            "CounterMax: counter {:?} <= limit {:?}",
                            distributed_count, self.frequency
                        ),
                        ConstraintResultType::Satisfied,
                    )
                } else {
                    (
                        format!(
                            "CounterMax: counter {:?} > limit {:?}",
                            distributed_count, self.frequency
                        ),
                        ConstraintResultType::Violated,
                    )
                }
            }
        }
    }
}
