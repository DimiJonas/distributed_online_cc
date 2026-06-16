use std::net::SocketAddr;

use async_trait::async_trait;
use chrono::Utc;

use crate::{
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

pub struct NotCoexistenceHandler {
    pub handler_id: ConstraintHandlerId,
    pub event_id: String,
    pub constraint_id: String,
    pub other_peer: SocketAddr,
}

#[async_trait]
impl ConstraintHandler for NotCoexistenceHandler {
    impl_constraint_handler_fields!(NotCoexistenceHandler);

    fn configured_peers(&self) -> Vec<SocketAddr> {
        vec![self.other_peer]
    }

    /// check if block/execute tokens exist
    ///     - yes: evaluate
    ///     - no: produce claim token an negotiate with other_peer, then check afterwards
    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::info!(
            "not coexistence handler: received configured event for constraint {}; event={}",
            self.constraint_id,
            event
        );

        let event_time = match event.timestamp_utc() {
            Some(timestamp) => timestamp,
            None => {
                log::warn!(
                    "not coexistence handler: event did not contain a timestamp - using current time as event time"
                );
                Utc::now()
            }
        };

        // get claim token or create one
        let meta_token = {
            let meta_tokens = state.meta_token_store.lock().unwrap();
            meta_tokens.get_token_cloned(&event.case_id, &self.constraint_id, self.handler_id)
        };

        let mut claim_token = match meta_token {
            Some(token) => token,
            None => {
                log::info!(
                    "not coexistence handler: no meta execution token exists yet. producing claim token and starting negotiation"
                );

                let claim_token = Token::new(
                    event.case_id.clone(),
                    self.constraint_id.clone(),
                    ConstraintType::NotCoexistence,
                    Some(event_time),
                    state.config.node_id,
                );

                {
                    let mut meta_tokens = state.meta_token_store.lock().unwrap();
                    meta_tokens.insert_meta_token(claim_token.clone(), self.handler_id);
                }

                claim_token
            }
        };

        if claim_token.payload == TokenPayload::None {
            // negotiate execution permission

            let message = PeerMessage::conformance_req(CCMessage::push_token(claim_token.clone()));
            let other_event_time = match send_and_receive(self.other_peer, message, state.clone())
                .await
            {
                Ok(response) => match response.payload {
                    PeerMessagePayload::Conformance(cc_message) => match cc_message.token {
                        Some(token) => token.event_time,
                        None => None,
                    },
                    _ => {
                        log::error!(
                            "not coexistence handler: received wrong message type: \"{}\"; expected type: \"conformance\"",
                            response.payload
                        );
                        return;
                    }
                },
                Err(e) => {
                    log::error!(
                        "not coexistence handler: error receiving token from {}: {}",
                        self.other_peer,
                        e
                    );
                    return;
                }
            };

            // set execution permission
            match other_event_time {
                Some(other_time) => {
                    if event_time <= other_time {
                        claim_token.payload.grant_execution_permission();
                        log::info!(
                            "not coexistence handler: set execution permission to {} because {} <= {}",
                            claim_token.payload.execution_is_allowed(),
                            event_time,
                            other_time
                        );
                    } else {
                        claim_token.payload.deny_execution_permission();
                        log::info!(
                            "not coexistence handler: set execution permission to {} because {} > {}",
                            claim_token.payload.execution_is_allowed(),
                            event_time,
                            other_time
                        );
                    }
                }
                None => {
                    claim_token.payload.grant_execution_permission();
                    log::info!(
                        "not coexistence handler: set execution to {} because {} <= NONE",
                        claim_token.payload.execution_is_allowed(),
                        event_time
                    );
                }
            }

            // save token with permissions
            {
                let mut meta_tokens = state.meta_token_store.lock().unwrap();
                meta_tokens.insert_meta_token(claim_token.clone(), self.handler_id);
            }
        }

        // evaluate event execution based on the execution permission
        if claim_token.payload.execution_is_allowed() {
            let info = format!("execution of event {} is allowed", self.event_id);
            emit_result(
                &event.case_id,
                &self.constraint_id,
                ConstraintResultType::Satisfied,
                info,
                state.clone(),
            )
            .await;
        } else {
            let info = format!("execution of event {} is forbidden", self.event_id);
            emit_result(
                &event.case_id,
                &self.constraint_id,
                ConstraintResultType::Violated,
                info,
                state.clone(),
            )
            .await;
        }
    }

    /// receive a claim token negotiation request and negotiate
    async fn handle_message(&self, state: AppState, message: CCMessage) {
        log::info!(
            "not coexistence handler: received message for constraint {}; message={}",
            self.constraint_id,
            message
        );
        log::debug!("message={:?}", message);

        if message.message_type != CCMessageType::PushToken {
            log::warn!(
                "not coexistence handler: unexpected message type {:?} for constraint {}, ignoring",
                message.message_type,
                self.constraint_id
            );
            return;
        }

        let request_id = match message.request_id {
            Some(id) => id,
            None => {
                log::error!(
                    "not coexistence handler: message does not contain a request id. cannot respond to it"
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
                            "not coexistence handler: PushToken message for constraint {} has no timestamp and the contained token also doesnt. cannot evaluate",
                            self.constraint_id
                        );
                        return;
                    }
                },
                None => {
                    log::error!(
                        "not coexistence handler: PushToken message for constraint {} has no timestamp and no token. cannot evaluate",
                        self.constraint_id
                    );
                    return;
                }
            },
        };

        // create own claim token if it does not exist yet
        let mut claim_token = {
            let mut meta_tokens = state.meta_token_store.lock().unwrap();
            match meta_tokens.get_token_cloned(
                &message.case_id,
                &self.constraint_id,
                self.handler_id,
            ) {
                Some(token) => token,
                None => {
                    // create token without timestamp
                    log::info!(
                        "creating new claim token for case={} constraint={}",
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
                    meta_tokens.insert_meta_token(claim_token.clone(), self.handler_id);
                    claim_token
                }
            }
        };

        // send claim token to requestor
        log::info!("sending claim token response to {}", self.other_peer);
        let response =
            PeerMessage::conformance_res(request_id, CCMessage::push_token(claim_token.clone()));
        if send_to_peer(self.other_peer, response, state.clone())
            .await
            .is_err()
        {
            log::error!(
                "not coexistence handler: error responding with claim token to {}",
                self.other_peer
            );
        }

        // set execution permissions
        match claim_token.event_time {
            Some(event_time) => {
                if event_time < other_event_time {
                    claim_token.payload.grant_execution_permission();
                    log::info!(
                        "not coexistence handler: set execution permission to {} because {} < {}",
                        claim_token.payload.execution_is_allowed(),
                        event_time,
                        other_event_time
                    );
                } else {
                    claim_token.payload.deny_execution_permission();
                    log::info!(
                        "not coexistence handler: set execution permission to {} because {} >= {}",
                        claim_token.payload.execution_is_allowed(),
                        event_time,
                        other_event_time
                    );
                }
            }
            None => {
                claim_token.payload.deny_execution_permission();
                log::info!(
                    "not coexistence handler: set execution permission to {} because NONE > {}",
                    claim_token.payload.execution_is_allowed(),
                    other_event_time
                );
            }
        }

        // save token with permissions
        {
            let mut meta_tokens = state.meta_token_store.lock().unwrap();
            meta_tokens.insert_meta_token(claim_token.clone(), self.handler_id);
        }
    }

    /// emit a result if none of the events happened
    async fn handle_case_end(&self, case_id: &CaseId, _state: AppState) {
        log::info!(
            "not coexistence handler: received case end signal for constraint {}; case={}",
            self.constraint_id,
            case_id
        );
    }

    /// remove execute/block token
    /// possibly also clear claim token?
    async fn clean_up_state(&self, case_id: &CaseId, _state: AppState) {
        log::info!(
            "not coexistence handler: clean up state for ({}/{})",
            case_id,
            self.constraint_id
        );
    }
}
