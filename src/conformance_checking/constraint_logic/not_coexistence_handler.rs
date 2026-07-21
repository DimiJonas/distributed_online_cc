use std::net::SocketAddr;

use async_trait::async_trait;
use uuid::Uuid;

use crate::{
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

pub struct NotCoexistenceAHandler {
    pub index: HandlerIndex,
    pub role: ConstraintRole,
    pub event_id: EventId,
    pub constraint_id: ConstraintId,
    pub b_peer: SocketAddr,
}

#[async_trait]
impl ConstraintHandler for NotCoexistenceAHandler {
    impl_constraint_handler_fields!(NotCoexistenceAHandler);

    fn configured_peers(&self) -> Vec<SocketAddr> {
        vec![self.b_peer]
    }

    /// check if block/execute tokens exist
    ///     - yes: evaluate
    ///     - no: produce claim token an negotiate with other_peer, then check afterwards
    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::info!(
            "not coexistence a handler: received configured event for constraint {}; event={}",
            self.constraint_id,
            event
        );

        // get claim token or create one
        let meta_token = {
            let meta_tokens = state.meta_token_store.lock().unwrap();
            meta_tokens.get_token_cloned(&event.case_id, self.index)
        };

        let mut claim_token = match meta_token {
            Some(token) => token,
            None => {
                log::info!(
                    "not coexistence a handler: no meta execution token exists yet. producing claim token and starting negotiation"
                );

                let claim_token = Token::new(
                    event.case_id.clone(),
                    self.constraint_id.clone(),
                    ConstraintType::NotCoexistence,
                    Some(event.timestamp),
                    state.config.node_id,
                );

                {
                    let mut meta_tokens = state.meta_token_store.lock().unwrap();
                    meta_tokens.insert_meta_token(claim_token.clone(), self.index);
                }

                claim_token
            }
        };

        if claim_token.payload == TokenPayload::None {
            // negotiate execution permission

            let message = PeerMessage::conformance_req(
                Uuid::new_v4(),
                CCMessage::push_claim_token(claim_token.clone(), self.role),
            );
            let other_event_time = match send_and_receive_conformance(
                self.b_peer,
                message,
                state.clone(),
            )
            .await
            {
                Ok(response) => match response.payload {
                    PeerMessagePayload::Conformance(cc_message) => match cc_message.token {
                        Some(token) => token.event_time,
                        None => None,
                    },
                    _ => {
                        log::error!(
                            "not coexistence a handler: received wrong message type: \"{}\"; expected type: \"conformance\"",
                            response.payload
                        );
                        return;
                    }
                },
                Err(e) => {
                    log::error!(
                        "not coexistence a handler: error receiving token from {}: {}",
                        self.b_peer,
                        e
                    );
                    return;
                }
            };

            // set execution permission
            match other_event_time {
                Some(other_time) => {
                    if event.timestamp <= other_time {
                        claim_token.payload.grant_execution_permission();
                        log::info!(
                            "not coexistence a handler: set execution permission to {} because {} <= {}",
                            claim_token.payload.is_execution_allowed(),
                            event.timestamp,
                            other_time
                        );
                    } else {
                        claim_token.payload.deny_execution_permission();
                        log::info!(
                            "not coexistence a handler: set execution permission to {} because {} > {}",
                            claim_token.payload.is_execution_allowed(),
                            event.timestamp,
                            other_time
                        );
                    }
                }
                None => {
                    claim_token.payload.grant_execution_permission();
                    log::info!(
                        "not coexistence a handler: set execution to {} because {} <= NONE",
                        claim_token.payload.is_execution_allowed(),
                        event.timestamp
                    );
                }
            }

            // save token with permissions
            {
                let mut meta_tokens = state.meta_token_store.lock().unwrap();
                meta_tokens.insert_meta_token(claim_token.clone(), self.index);
            }
        }

        // evaluate event execution based on the execution permission
        if !claim_token.payload.is_execution_allowed() {
            let info = format!(
                "execution of event {} is forbidden, because not coexistence B event happened first",
                self.event_id
            );
            emit_result(
                &event.case_id,
                &self.constraint_id,
                ConstraintType::NotCoexistence,
                ConstraintResultType::Violated,
                info,
                state.clone(),
            )
            .await;
        }
    }

    /// receive a claim token negotiation request and negotiate
    async fn handle_message(
        &self,
        state: AppState,
        message: CCMessage,
        request_id: Option<RequestId>,
    ) {
        if message.creating_handler == Some(self.role) {
            // silently dropping message from self
            return;
        }

        log::info!(
            "not coexistence a handler: received message for constraint {}; message={}",
            self.constraint_id,
            message
        );
        log::debug!("message={:?}", message);

        if message.message_type != CCMessageType::PushToken {
            log::warn!(
                "not coexistence a handler: unexpected message type {:?} for constraint {}, ignoring",
                message.message_type,
                self.constraint_id
            );
            return;
        }

        let request_id = match request_id {
            Some(id) => id,
            None => {
                log::error!(
                    "not coexistence a handler: message does not contain a request id. cannot respond to it"
                );
                return;
            }
        };

        // unpack event time
        let other_event_time = match message.timestamp {
            Some(event_time) => event_time,
            None => match &message.token {
                Some(token) => match token.event_time {
                    Some(event_time) => event_time,
                    None => {
                        log::error!(
                            "not coexistence a handler: PushToken message for constraint {} has no timestamp and the contained token also doesnt. cannot evaluate",
                            self.constraint_id
                        );
                        return;
                    }
                },
                None => {
                    log::error!(
                        "not coexistence a handler: PushToken message for constraint {} has no timestamp and no token. cannot evaluate",
                        self.constraint_id
                    );
                    return;
                }
            },
        };

        // create own claim token if it does not exist yet
        let mut claim_token = {
            let mut meta_tokens = state.meta_token_store.lock().unwrap();
            match meta_tokens.get_token_cloned(&message.case_id, self.index) {
                Some(token) => token,
                None => {
                    // create token without timestamp
                    log::info!(
                        "not coexistence a handler: creating new claim token for case={} constraint={}",
                        message.case_id,
                        self.constraint_id
                    );
                    let claim_token = Token::new(
                        message.case_id.clone(),
                        self.constraint_id.clone(),
                        ConstraintType::NotCoexistence,
                        None,
                        state.config.node_id,
                    );
                    meta_tokens.insert_meta_token(claim_token.clone(), self.index);
                    claim_token
                }
            }
        };

        // send claim token to requestor
        log::info!(
            "not coexistence a handler: sending claim token response to {}",
            self.b_peer
        );
        let response = PeerMessage::conformance_res(
            request_id,
            CCMessage::push_claim_token(claim_token.clone(), self.role),
        );
        if send_to_conformance_peer(self.b_peer, response, state.clone())
            .await
            .is_err()
        {
            log::error!(
                "not coexistence a handler: error responding with claim token to {}",
                self.b_peer
            );
        }

        // set execution permissions
        match claim_token.event_time {
            Some(event_time) => {
                if event_time < other_event_time {
                    claim_token.payload.grant_execution_permission();
                    log::info!(
                        "not coexistence a handler: set execution permission to {} because {} < {}",
                        claim_token.payload.is_execution_allowed(),
                        event_time,
                        other_event_time
                    );
                } else {
                    claim_token.payload.deny_execution_permission();
                    log::info!(
                        "not coexistence a handler: set execution permission to {} because {} >= {}",
                        claim_token.payload.is_execution_allowed(),
                        event_time,
                        other_event_time
                    );
                }
            }
            None => {
                claim_token.payload.deny_execution_permission();
                log::info!(
                    "not coexistence a handler: set execution permission to {} because NONE > {}",
                    claim_token.payload.is_execution_allowed(),
                    other_event_time
                );
            }
        }

        // save token with permissions
        {
            let mut meta_tokens = state.meta_token_store.lock().unwrap();
            meta_tokens.insert_meta_token(claim_token.clone(), self.index);
        }
    }

    /// emit a result if none of the events happened
    async fn handle_case_end(&self, case_id: &CaseId, state: AppState) {
        log::info!(
            "not coexistence a handler: received case end signal for constraint {}; case={}",
            self.constraint_id,
            case_id
        );

        let claim_token_exists = {
            let meta_tokens = state.meta_token_store.lock().unwrap();
            meta_tokens.get_token_cloned(case_id, self.index).is_some()
        };

        if !claim_token_exists {
            // produce positive result because none was emitted yet and both handlers did not receive any events
            emit_result(
                case_id,
                &self.constraint_id,
                ConstraintType::NotCoexistence,
                ConstraintResultType::Satisfied,
                "none of the not coexistence events happened until case end".to_string(),
                state.clone(),
            )
            .await;
        }
    }

    async fn clean_up_state(&self, case_id: &CaseId, state: AppState) {
        log::info!(
            "not coexistence a handler: clean up state for ({}/{})",
            case_id,
            self.constraint_id
        );

        // delete claim token
        {
            let mut meta_tokens = state.meta_token_store.lock().unwrap();
            meta_tokens.remove_meta_token(case_id, self.index);
        }
    }
}

pub struct NotCoexistenceBHandler {
    pub index: HandlerIndex,
    pub role: ConstraintRole,
    pub event_id: EventId,
    pub constraint_id: ConstraintId,
    pub a_peer: SocketAddr,
}

#[async_trait]
impl ConstraintHandler for NotCoexistenceBHandler {
    impl_constraint_handler_fields!(NotCoexistenceBHandler);

    fn configured_peers(&self) -> Vec<SocketAddr> {
        vec![self.a_peer]
    }

    /// check if block/execute tokens exist
    ///     - yes: evaluate
    ///     - no: produce claim token an negotiate with other_peer, then check afterwards
    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::info!(
            "not coexistence b handler: received configured event for constraint {}; event={}",
            self.constraint_id,
            event
        );

        // get claim token or create one
        let meta_token = {
            let meta_tokens = state.meta_token_store.lock().unwrap();
            meta_tokens.get_token_cloned(&event.case_id, self.index)
        };

        let mut claim_token = match meta_token {
            Some(token) => token,
            None => {
                log::info!(
                    "not coexistence b handler: no meta execution token exists yet. producing claim token and starting negotiation"
                );

                let claim_token = Token::new(
                    event.case_id.clone(),
                    self.constraint_id.clone(),
                    ConstraintType::NotCoexistence,
                    Some(event.timestamp),
                    state.config.node_id,
                );

                {
                    let mut meta_tokens = state.meta_token_store.lock().unwrap();
                    meta_tokens.insert_meta_token(claim_token.clone(), self.index);
                }

                claim_token
            }
        };

        if claim_token.payload == TokenPayload::None {
            // negotiate execution permission

            let message = PeerMessage::conformance_req(
                Uuid::new_v4(),
                CCMessage::push_claim_token(claim_token.clone(), self.role),
            );
            let other_event_time = match send_and_receive_conformance(
                self.a_peer,
                message,
                state.clone(),
            )
            .await
            {
                Ok(response) => match response.payload {
                    PeerMessagePayload::Conformance(cc_message) => match cc_message.token {
                        Some(token) => token.event_time,
                        None => None,
                    },
                    _ => {
                        log::error!(
                            "not coexistence b handler: received wrong message type: \"{}\"; expected type: \"conformance\"",
                            response.payload
                        );
                        return;
                    }
                },
                Err(e) => {
                    log::error!(
                        "not coexistence b handler: error receiving token from {}: {}",
                        self.a_peer,
                        e
                    );
                    return;
                }
            };

            // set execution permission
            match other_event_time {
                Some(other_time) => {
                    if event.timestamp <= other_time {
                        claim_token.payload.grant_execution_permission();
                        log::info!(
                            "not coexistence b handler: set execution permission to {} because {} <= {}",
                            claim_token.payload.is_execution_allowed(),
                            event.timestamp,
                            other_time
                        );
                    } else {
                        claim_token.payload.deny_execution_permission();
                        log::info!(
                            "not coexistence b handler: set execution permission to {} because {} > {}",
                            claim_token.payload.is_execution_allowed(),
                            event.timestamp,
                            other_time
                        );
                    }
                }
                None => {
                    claim_token.payload.grant_execution_permission();
                    log::info!(
                        "not coexistence b handler: set execution to {} because {} <= NONE",
                        claim_token.payload.is_execution_allowed(),
                        event.timestamp
                    );
                }
            }

            // save token with permissions
            {
                let mut meta_tokens = state.meta_token_store.lock().unwrap();
                meta_tokens.insert_meta_token(claim_token.clone(), self.index);
            }
        }

        // evaluate event execution based on the execution permission
        if !claim_token.payload.is_execution_allowed() {
            let info = format!(
                "execution of event {} is forbidden, because not coexistence A event happened first",
                self.event_id
            );
            emit_result(
                &event.case_id,
                &self.constraint_id,
                ConstraintType::NotCoexistence,
                ConstraintResultType::Violated,
                info,
                state.clone(),
            )
            .await;
        }
    }

    /// receive a claim token negotiation request and negotiate
    async fn handle_message(
        &self,
        state: AppState,
        message: CCMessage,
        request_id: Option<RequestId>,
    ) {
        if message.creating_handler == Some(self.role) {
            // silently dropping message from self
            return;
        }

        log::info!(
            "not coexistence b handler: received message for constraint {}; message={}",
            self.constraint_id,
            message
        );
        log::debug!("message={:?}", message);

        if message.message_type != CCMessageType::PushToken {
            log::warn!(
                "not coexistence b handler: unexpected message type {:?} for constraint {}, ignoring",
                message.message_type,
                self.constraint_id
            );
            return;
        }

        let request_id = match request_id {
            Some(id) => id,
            None => {
                log::error!(
                    "not coexistence b handler: message does not contain a request id. cannot respond to it"
                );
                return;
            }
        };

        // unpack event time
        let other_event_time = match message.timestamp {
            Some(event_time) => event_time,
            None => match &message.token {
                Some(token) => match token.event_time {
                    Some(event_time) => event_time,
                    None => {
                        log::error!(
                            "not coexistence b handler: PushToken message for constraint {} has no timestamp and the contained token also doesnt. cannot evaluate",
                            self.constraint_id
                        );
                        return;
                    }
                },
                None => {
                    log::error!(
                        "not coexistence b handler: PushToken message for constraint {} has no timestamp and no token. cannot evaluate",
                        self.constraint_id
                    );
                    return;
                }
            },
        };

        // create own claim token if it does not exist yet
        let mut claim_token = {
            let mut meta_tokens = state.meta_token_store.lock().unwrap();
            match meta_tokens.get_token_cloned(&message.case_id, self.index) {
                Some(token) => token,
                None => {
                    // create token without timestamp
                    log::info!(
                        "not coexistence b handler: creating new claim token for case={} constraint={}",
                        message.case_id,
                        self.constraint_id
                    );
                    let claim_token = Token::new(
                        message.case_id.clone(),
                        self.constraint_id.clone(),
                        ConstraintType::NotCoexistence,
                        None,
                        state.config.node_id,
                    );
                    meta_tokens.insert_meta_token(claim_token.clone(), self.index);
                    claim_token
                }
            }
        };

        // send claim token to requestor
        log::info!(
            "not coexistence b handler: sending claim token response to {}",
            self.a_peer
        );
        let response = PeerMessage::conformance_res(
            request_id,
            CCMessage::push_claim_token(claim_token.clone(), self.role),
        );
        if send_to_conformance_peer(self.a_peer, response, state.clone())
            .await
            .is_err()
        {
            log::error!(
                "not coexistence b handler: error responding with claim token to {}",
                self.a_peer
            );
        }

        // set execution permissions
        match claim_token.event_time {
            Some(event_time) => {
                if event_time < other_event_time {
                    claim_token.payload.grant_execution_permission();
                    log::info!(
                        "not coexistence b handler: set execution permission to {} because {} < {}",
                        claim_token.payload.is_execution_allowed(),
                        event_time,
                        other_event_time
                    );
                } else {
                    claim_token.payload.deny_execution_permission();
                    log::info!(
                        "not coexistence b handler: set execution permission to {} because {} >= {}",
                        claim_token.payload.is_execution_allowed(),
                        event_time,
                        other_event_time
                    );
                }
            }
            None => {
                claim_token.payload.deny_execution_permission();
                log::info!(
                    "not coexistence b handler: set execution permission to {} because NONE > {}",
                    claim_token.payload.is_execution_allowed(),
                    other_event_time
                );
            }
        }

        // save token with permissions
        {
            let mut meta_tokens = state.meta_token_store.lock().unwrap();
            meta_tokens.insert_meta_token(claim_token.clone(), self.index);
        }
    }

    async fn clean_up_state(&self, case_id: &CaseId, state: AppState) {
        log::info!(
            "not coexistence b handler: clean up state for ({}/{})",
            case_id,
            self.constraint_id
        );

        // delete claim token
        {
            let mut meta_tokens = state.meta_token_store.lock().unwrap();
            meta_tokens.remove_meta_token(case_id, self.index);
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, TimeZone, Utc};

    use crate::{
        config::node_config::ConstraintConfig,
        conformance_checking::constraint_logic::{
            constraint_handler_trait::ConstraintHandler,
            not_coexistence_handler::NotCoexistenceAHandler,
        },
        network::message_types::CCMessage,
        state::app_state::AppState,
        types::{ConstraintResultType, ConstraintRole, ConstraintType, PeerAddress, Token},
        util::test_tools::{
            addr, answer_one_conformance_request, make_config_with_constraints_and_port,
            make_state_from_config, make_traced_event_at, register_manager,
        },
    };

    const CID: &str = "nc";
    const EID: &str = "a_event";

    fn at(secs: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, secs).unwrap()
    }

    fn a_state(node_port: u16, b_port: u16, case: &str) -> (AppState, std::net::SocketAddr) {
        let b_peer = addr(b_port);
        let config = make_config_with_constraints_and_port(
            vec![ConstraintConfig::NotCoexistenceA {
                constraint_id: CID.to_string(),
                event_id: EID.to_string(),
                b_peer,
            }],
            node_port,
        );
        let state = make_state_from_config(config);
        state
            .cc_results
            .lock()
            .unwrap()
            .register_case(case.to_string());
        (state, b_peer)
    }

    fn handler(b_peer: std::net::SocketAddr) -> NotCoexistenceAHandler {
        NotCoexistenceAHandler {
            index: 1,
            role: ConstraintRole::NotCoexistenceA,
            event_id: EID.to_string(),
            constraint_id: CID.to_string(),
            b_peer,
        }
    }

    /// Response token carrying B's event time (or None for "B never ran").
    fn b_response(case: &str, b_time: Option<u32>) -> CCMessage {
        let token = Token::new(
            case.to_string(),
            CID.to_string(),
            ConstraintType::NotCoexistence,
            b_time.map(at),
            2,
        );
        CCMessage::push_token(token)
    }

    fn result_type(state: &AppState, case: &str) -> Option<ConstraintResultType> {
        let store = state.cc_results.lock().unwrap();
        store
            .get_constraint(&case.to_string(), &CID.to_string())
            .map(|r| r.constraint_result)
    }

    /// neither event happened → Satisfied at case end.
    #[tokio::test]
    async fn neither_satisfied_at_case_end() {
        let case = "c1";
        let (state, b_peer) = a_state(24000, 24001, case);
        handler(b_peer)
            .handle_case_end(&case.to_string(), state.clone())
            .await;
        assert!(matches!(
            result_type(&state, case),
            Some(ConstraintResultType::Satisfied)
        ));
    }

    /// only A ran (B never ran -> other time None) -> A gets execution right -> no violation/result.
    #[tokio::test]
    async fn only_a_event_no_violation() {
        let case = "c2";
        let (state, b_peer) = a_state(24010, 24011, case);
        let mut mgr_rx = register_manager(&state, PeerAddress::new_conformance_address(b_peer));

        let h = handler(b_peer);
        let ev = make_traced_event_at(case, EID, 10);
        let run = h.handle_event(&ev, state.clone());
        let mock =
            answer_one_conformance_request(state.clone(), &mut mgr_rx, b_response(case, None));
        tokio::join!(run, mock);

        assert!(result_type(&state, case).is_none());
    }

    /// both ran, A earlier than B -> A has execution right -> Satisfied on A -> no violation/result.
    #[tokio::test]
    async fn a_earlier_than_b_no_violation() {
        let case = "c3";
        let (state, b_peer) = a_state(24020, 24021, case);
        let mut mgr_rx = register_manager(&state, PeerAddress::new_conformance_address(b_peer));

        let h = handler(b_peer);
        let ev = make_traced_event_at(case, EID, 10);
        let run = h.handle_event(&ev, state.clone());
        let mock =
            answer_one_conformance_request(state.clone(), &mut mgr_rx, b_response(case, Some(20)));
        tokio::join!(run, mock);

        assert!(result_type(&state, case).is_none());
    }

    /// both ran, A later than B → B had execution right → Violated on A.
    #[tokio::test]
    async fn a_later_than_b_violated() {
        let case = "c4";
        let (state, b_peer) = a_state(24030, 24031, case);
        let mut mgr_rx = register_manager(&state, PeerAddress::new_conformance_address(b_peer));

        let h = handler(b_peer);
        let ev = make_traced_event_at(case, EID, 20);
        let run = h.handle_event(&ev, state.clone());
        let mock =
            answer_one_conformance_request(state.clone(), &mut mgr_rx, b_response(case, Some(10)));
        tokio::join!(run, mock);

        assert!(matches!(
            result_type(&state, case),
            Some(ConstraintResultType::Violated)
        ));
    }
}
