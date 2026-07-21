use std::net::SocketAddr;

use async_trait::async_trait;
use uuid::Uuid;

use crate::{
    config::node_config::{DistributedAppearanceCounterReset, MinMaxMode},
    conformance_checking::{
        constraint_logic::constraint_handler_trait::ConstraintHandler, result_emitter::emit_result,
    },
    impl_constraint_handler_fields,
    network::{
        message_types::{CCMessage, CCMessageType, PeerMessage, PeerMessagePayload},
        tcp_router::{send_and_receive_conformance, send_to_conformance_peer},
    },
    state::app_state::AppState,
    types::{
        CaseId, ConstraintId, ConstraintResultType, ConstraintRole, ConstraintType, EventId,
        HandlerIndex, RequestId, Token, TokenPayload, TracedEvent,
    },
};

pub struct DistributedAppearanceCounterHandler {
    pub index: HandlerIndex,
    pub role: ConstraintRole,
    pub event_id: EventId,
    pub constraint_id: ConstraintId,
    pub reset: DistributedAppearanceCounterReset,
}

#[async_trait]
impl ConstraintHandler for DistributedAppearanceCounterHandler {
    impl_constraint_handler_fields!(DistributedAppearanceCounterHandler);

    fn counter_reset(&self) -> Option<DistributedAppearanceCounterReset> {
        Some(self.reset)
    }

    // increment counter
    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::info!(
            "distributed appearance counter handler: received configured event for constraint {}; event={}",
            self.constraint_id,
            event
        );

        let mut meta_tokens = state.meta_token_store.lock().unwrap();
        let count = match meta_tokens.get_token_mut(&event.case_id, self.index) {
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
                meta_tokens.insert_meta_token(counter_token.clone(), self.index);
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
    async fn handle_message(
        &self,
        state: AppState,
        message: CCMessage,
        request_id: Option<RequestId>,
    ) {
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

        let request_id = match request_id {
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

        // accumulate all local distributed appearance counters (if there are multiple configured for the same constraint)
        // in this case only 1 handler will receive the pulltokenrequest and answer it
        let dist_appear_counter_handlers: Vec<&dyn ConstraintHandler> = state
            .handlers
            .get_by_constraint_id(&self.constraint_id)
            .into_iter()
            .filter(|h| h.role() == ConstraintRole::DistributedAppearanceCounter)
            .collect();
        let mut total = 0u64;
        {
            let mut meta_tokens = state.meta_token_store.lock().unwrap();
            for handler in dist_appear_counter_handlers {
                if let Some(token) = meta_tokens.get_token_mut(&message.case_id, handler.index()) {
                    total += token.payload.get_count();
                    // reset counter
                    if let Some(reset) = handler.counter_reset() {
                        match reset {
                            DistributedAppearanceCounterReset::Keep => {}
                            DistributedAppearanceCounterReset::Zero => {
                                token.payload.reset_counter()
                            }
                        }
                    }
                }
            }
        }

        let token_with_total = Token::new_with_payload(
            message.case_id.clone(),
            self.constraint_id.clone(),
            ConstraintType::DistributedAppearance,
            None,
            state.config.node_id,
            TokenPayload::Counter(total),
        );

        // send counter token
        log::info!(
            "sending counter token to {}",
            message.sender_address.unwrap()
        );
        let response = PeerMessage::conformance_res(
            request_id,
            CCMessage::pull_token_response(token_with_total),
        );
        if send_to_conformance_peer(sender_address, response, state.clone())
            .await
            .is_err()
        {
            log::error!(
                "distributed appearance counter handler: error responding with counter token to {}",
                sender_address
            );
        }
    }

    async fn clean_up_state(&self, case_id: &CaseId, state: AppState) {
        log::info!(
            "distributed appearance counter handler: clean up state for ({}/{})",
            case_id,
            self.constraint_id
        );

        // delete counter token
        {
            let mut meta_tokens = state.meta_token_store.lock().unwrap();
            meta_tokens.remove_meta_token(case_id, self.index);
        }
    }
}

pub struct DistributedAppearanceTriggerHandler {
    pub index: HandlerIndex,
    pub role: ConstraintRole,
    pub event_id: EventId,
    pub constraint_id: ConstraintId,
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
        if let Some((info, result)) = self.evaluate_count(distributed_count) {
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
            emit_result(
                case_id,
                &self.constraint_id,
                ConstraintType::DistributedAppearance,
                ConstraintResultType::Satisfied,
                "counter evaluation was never triggered".to_string(),
                state.clone(),
            )
            .await;
        }
    }
}

impl DistributedAppearanceTriggerHandler {
    // accumulate count
    async fn get_distributed_count(&self, state: AppState, case_id: &CaseId) -> u64 {
        let mut distributed_count: u64 = 0;
        for counter_peer in &self.counter_peers {
            let pull_token_request = PeerMessage::conformance_req(
                Uuid::new_v4(),
                CCMessage::pull_token_request_with_creator(
                    case_id.clone(),
                    self.constraint_id.clone(),
                    self.role,
                ),
            );
            match send_and_receive_conformance(*counter_peer, pull_token_request, state.clone())
                .await
            {
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

    fn evaluate_count(&self, distributed_count: u64) -> Option<(String, ConstraintResultType)> {
        match self.mode {
            MinMaxMode::Min => {
                if distributed_count < self.frequency {
                    Some((
                        format!(
                            "CounterMin: counter {:?} < limit {:?}",
                            distributed_count, self.frequency
                        ),
                        ConstraintResultType::Violated,
                    ))
                } else {
                    None
                }
            }
            MinMaxMode::Max => {
                if distributed_count > self.frequency {
                    Some((
                        format!(
                            "CounterMax: counter {:?} > limit {:?}",
                            distributed_count, self.frequency
                        ),
                        ConstraintResultType::Violated,
                    ))
                } else {
                    None
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        config::node_config::{ConstraintConfig, MinMaxMode},
        conformance_checking::constraint_logic::{
            constraint_handler_trait::ConstraintHandler,
            distributed_appearance_constraint_handler::DistributedAppearanceTriggerHandler,
        },
        network::message_types::CCMessage,
        state::app_state::AppState,
        types::{ConstraintResultType, ConstraintRole, PeerAddress},
        util::test_tools::{
            addr, answer_one_conformance_request, make_config_with_constraints_and_port,
            make_counter_token, make_state_from_config, make_traced_event_with_fixed_timestamp,
            register_manager,
        },
    };

    const CID: &str = "dapp";
    const EID: &str = "trigger";

    /// Build a trigger-side state. The counter peer (if any) is a remote node we
    /// mock; the local node only hosts the trigger handler.
    fn trigger_state(
        node_port: u16,
        counter_port: Option<u16>,
        case: &str,
        mode: MinMaxMode,
        frequency: u64,
    ) -> (AppState, Vec<std::net::SocketAddr>) {
        let counter_peers: Vec<std::net::SocketAddr> = counter_port.into_iter().map(addr).collect();
        let config = make_config_with_constraints_and_port(
            vec![ConstraintConfig::DistributedAppearanceTrigger {
                constraint_id: CID.to_string(),
                event_id: EID.to_string(),
                mode,
                frequency,
                counter_peers: counter_peers.clone(),
            }],
            node_port,
        );
        let state = make_state_from_config(config);
        state
            .cc_results
            .lock()
            .unwrap()
            .register_case(case.to_string());
        (state, counter_peers)
    }

    fn handler(
        mode: MinMaxMode,
        frequency: u64,
        counter_peers: Vec<std::net::SocketAddr>,
    ) -> DistributedAppearanceTriggerHandler {
        DistributedAppearanceTriggerHandler {
            index: 1,
            role: ConstraintRole::DistributedAppearanceTrigger,
            event_id: EID.to_string(),
            constraint_id: CID.to_string(),
            mode,
            frequency,
            counter_peers,
        }
    }

    /// A pull-token response from the counter peer carrying `count`.
    fn counter_response(case: &str, count: u64) -> CCMessage {
        CCMessage::pull_token_response(make_counter_token(case, CID, count))
    }

    fn result_type(state: &AppState, case: &str) -> Option<ConstraintResultType> {
        let store = state.cc_results.lock().unwrap();
        store
            .get_constraint(&case.to_string(), &CID.to_string())
            .map(|r| r.constraint_result)
    }

    /// none: no counter peers, no trigger event → case end pulls 0 → Max 0<=2 → Satisfied.
    #[tokio::test]
    async fn none_satisfied_at_case_end_max() {
        let case = "c1";
        let (state, peers) = trigger_state(26000, None, case, MinMaxMode::Max, 2);
        handler(MinMaxMode::Max, 2, peers)
            .handle_case_end(&case.to_string(), state.clone())
            .await;
        assert!(matches!(
            result_type(&state, case),
            Some(ConstraintResultType::Satisfied)
        ));
    }

    /// only trigger event, distributed count 0 → evaluated against 0.
    /// Min freq 1: 0 >= 1 false → Violated.
    #[tokio::test]
    async fn only_trigger_evaluated_against_zero() {
        let case = "c2";
        let (state, peers) = trigger_state(26010, Some(26011), case, MinMaxMode::Min, 1);
        let mut mgr_rx = register_manager(&state, PeerAddress::new_conformance_address(peers[0]));

        let h = handler(MinMaxMode::Min, 1, peers);
        let ev = make_traced_event_with_fixed_timestamp(case, EID);
        let run = h.handle_event(&ev, state.clone());
        let mock =
            answer_one_conformance_request(state.clone(), &mut mgr_rx, counter_response(case, 0));
        tokio::join!(run, mock);

        assert!(matches!(
            result_type(&state, case),
            Some(ConstraintResultType::Violated)
        ));
    }

    /// both: distributed count meets Min frequency -> no violation/result.
    #[tokio::test]
    async fn both_min_no_violation() {
        let case = "c3";
        let (state, peers) = trigger_state(26020, Some(26021), case, MinMaxMode::Min, 2);
        let mut mgr_rx = register_manager(&state, PeerAddress::new_conformance_address(peers[0]));

        let h = handler(MinMaxMode::Min, 2, peers);
        let ev = make_traced_event_with_fixed_timestamp(case, EID);
        let run = h.handle_event(&ev, state.clone());
        let mock =
            answer_one_conformance_request(state.clone(), &mut mgr_rx, counter_response(case, 3));
        tokio::join!(run, mock);

        assert!(result_type(&state, case).is_none(),);
    }

    /// both: distributed count below Min frequency → Violated.
    #[tokio::test]
    async fn both_min_violated() {
        let case = "c4";
        let (state, peers) = trigger_state(26030, Some(26031), case, MinMaxMode::Min, 3);
        let mut mgr_rx = register_manager(&state, PeerAddress::new_conformance_address(peers[0]));

        let h = handler(MinMaxMode::Min, 3, peers);
        let ev = make_traced_event_with_fixed_timestamp(case, EID);
        let run = h.handle_event(&ev, state.clone());
        let mock =
            answer_one_conformance_request(state.clone(), &mut mgr_rx, counter_response(case, 2));
        tokio::join!(run, mock);

        assert!(matches!(
            result_type(&state, case),
            Some(ConstraintResultType::Violated)
        ));
    }

    /// only counter events (no trigger) → Satisfied
    #[tokio::test]
    async fn only_counter_events_satisfied_desired() {
        let case = "c5";
        let (state, peers) = trigger_state(26040, Some(26041), case, MinMaxMode::Max, 2);
        let h = handler(MinMaxMode::Max, 2, peers);
        h.handle_case_end(&case.to_string(), state.clone()).await;
        assert!(
            matches!(
                result_type(&state, case),
                Some(ConstraintResultType::Satisfied)
            ),
            "counter-only (no trigger) should be Satisfied"
        );
    }
}
