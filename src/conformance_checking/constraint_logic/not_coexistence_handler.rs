use std::net::SocketAddr;

use async_trait::async_trait;
use chrono::Utc;
use uuid::Uuid;

use crate::{
    conformance_checking::{
        constraint_logic::constraint_handler_trait::ConstraintHandler,
        result_emitter::emit_result_at,
    },
    impl_constraint_handler_fields,
    measurement::record::ResultTrigger,
    network::{
        message_types::{CCMessage, CCMessageType, PeerMessage, PeerMessagePayload},
        tcp_router::{send_and_receive_conformance, send_to_conformance_peer},
    },
    state::app_state::AppState,
    types::{
        CaseId, ConstraintId, ConstraintResultType, ConstraintRole, ConstraintType, EventId,
        EventTime, HandlerIndex, RequestId, Token, TokenPayload, TracedEvent,
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

        // record the earliest local occurrence and decide whether a negotiation is needed
        let (mut claim_token, negotiate, mut decided_at) = {
            let mut meta_tokens = state.meta_token_store.lock().unwrap();
            match meta_tokens.get_token_mut(&event.case_id, self.index) {
                Some(stored) => {
                    let negotiate = match stored.event_time {
                        // token was created while answering the other sides negotiation before this handler saw its own event
                        None => {
                            stored.event_time = Some(event.timestamp);
                            true
                        }
                        Some(recorded) if event.timestamp < recorded => {
                            log::info!(
                                "not coexistence a handler: event at {} is earlier than the recorded occurrence at {} - renegotiating",
                                event.timestamp,
                                recorded
                            );
                            stored.event_time = Some(event.timestamp);
                            // the recorded permission was decided with the later time
                            stored.payload = TokenPayload::None;
                            true
                        }
                        // a later occurrence does not change the earliest one
                        Some(_) => stored.payload == TokenPayload::None,
                    };
                    (stored.clone(), negotiate, Utc::now())
                }
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
                    meta_tokens.insert_meta_token(claim_token.clone(), self.index);
                    (claim_token, true, Utc::now())
                }
            }
        };

        if negotiate {
            // negotiate execution permission
            let other_event_time = match self
                .fetch_other_event_time(&claim_token, state.clone())
                .await
            {
                Ok(timestamp) => timestamp,
                Err(()) => return,
            };
            // overwrite current decided_at timestamp because it was decided again here by acquiring the timestamp from the other side
            decided_at = Utc::now();

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
                match meta_tokens.get_token_mut(&event.case_id, self.index) {
                    Some(stored) => {
                        stored.payload = claim_token.payload.clone();
                        if stored.event_time.is_none() {
                            stored.event_time = Some(event.timestamp);
                        }
                    }
                    None => meta_tokens.insert_meta_token(claim_token.clone(), self.index),
                }
            }
        }

        // evaluate event execution based on the execution permission
        if !claim_token.payload.is_execution_allowed() {
            // take next revision number since a result will be emitted
            let revision = {
                let mut revisions = state.result_revisions.lock().unwrap();
                revisions.next_revision(&event.case_id, &self.constraint_id)
            };
            let info = format!(
                "execution of event {} is forbidden, because not coexistence B event happened first",
                self.event_id
            );
            emit_result_at(
                decided_at,
                revision,
                &event.case_id,
                &self.constraint_id,
                ConstraintType::NotCoexistence,
                ConstraintResultType::Violated,
                info,
                state.clone(),
                ResultTrigger::Event,
                Some(event.instance_id),
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
        let (mut claim_token, mut decided_at) = {
            let mut meta_tokens = state.meta_token_store.lock().unwrap();
            match meta_tokens.get_token_cloned(&message.case_id, self.index) {
                Some(token) => (token, Utc::now()),
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
                    (claim_token, Utc::now())
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
                // <=: on a timestamp tie the A side deterministically wins
                if event_time <= other_event_time {
                    claim_token.payload.grant_execution_permission();
                    log::info!(
                        "not coexistence a handler: set execution permission to {} because {} <= {}",
                        claim_token.payload.is_execution_allowed(),
                        event_time,
                        other_event_time
                    );
                } else {
                    claim_token.payload.deny_execution_permission();
                    log::info!(
                        "not coexistence a handler: set execution permission to {} because {} > {}",
                        claim_token.payload.is_execution_allowed(),
                        event_time,
                        other_event_time
                    );
                }
            }
            None => {
                // the local event has not been seen here yet so leave the payload untouched
                log::info!(
                    "not coexistence a handler: local event time not known yet, leaving the permission undecided (other side at {})",
                    other_event_time
                );
                return;
            }
        }

        // save token with permissions
        let permission_changed = {
            let mut meta_tokens = state.meta_token_store.lock().unwrap();
            match meta_tokens.get_token_mut(&message.case_id, self.index) {
                Some(stored) => {
                    let changed = stored.payload != claim_token.payload;
                    stored.payload = claim_token.payload.clone();
                    if stored.event_time.is_none() {
                        stored.event_time = claim_token.event_time;
                    }
                    if changed {
                        // overwrite decided_at since permissions were changed here
                        decided_at = Utc::now();
                    }
                    changed
                }
                None => {
                    meta_tokens.insert_meta_token(claim_token.clone(), self.index);
                    true
                }
            }
        };

        if permission_changed && !claim_token.payload.is_execution_allowed() {
            // take next revision number since a result will be emitted
            let revision = {
                let mut revisions = state.result_revisions.lock().unwrap();
                revisions.next_revision(&message.case_id, &self.constraint_id)
            };
            let info = format!(
                "execution of event {} is forbidden, because not coexistence B event happened first",
                self.event_id
            );
            emit_result_at(
                decided_at,
                revision,
                &message.case_id,
                &self.constraint_id,
                ConstraintType::NotCoexistence,
                ConstraintResultType::Violated,
                info,
                state.clone(),
                ResultTrigger::Event,
                message.triggering_event,
            )
            .await;
        }
    }

    // emit the satisfied fallback when the constraint was not violated
    async fn handle_case_end(&self, case_id: &CaseId, state: AppState) {
        log::info!(
            "not coexistence a handler: received case end signal for constraint {}; case={}",
            self.constraint_id,
            case_id
        );

        let result_exists = {
            let results = state.cc_results.lock().unwrap();
            results
                .get_constraint(case_id, &self.constraint_id)
                .is_some()
        };
        if result_exists {
            return;
        }

        let (claim_token, mut decided_at) = {
            let meta_tokens = state.meta_token_store.lock().unwrap();
            (
                meta_tokens.get_token_cloned(case_id, self.index),
                Utc::now(),
            )
        };

        // take next revision number since a result will probably be emitted
        let revision = {
            let mut revisions = state.result_revisions.lock().unwrap();
            revisions.next_revision(case_id, &self.constraint_id)
        };

        let info: String = match &claim_token {
            None => "none of the not coexistence events happened until case end".to_string(),
            Some(token) if token.event_time.is_none() => {
                "only the not coexistence B event happened until case end".to_string()
            }
            Some(token) if token.payload == TokenPayload::None => {
                // incomplete negotiation - ask the B side as a best effort
                match self.fetch_other_event_time(token, state.clone()).await {
                    Ok(None) => {
                        // overwrite decided_at since this information (may have) changed the result
                        decided_at = Utc::now();
                        "only the not coexistence A event happened until case end".to_string()
                    }
                    Ok(Some(_)) | Err(()) => {
                        log::warn!(
                            "not coexistence a handler: claim token for ({}/{}) has an undecided permission at case end; emitting no fallback",
                            case_id,
                            self.constraint_id
                        );
                        return;
                    }
                }
            }
            Some(token) if !token.payload.is_execution_allowed() => {
                // take next revision number since a result will be emitted
                let revision = {
                    let mut revisions = state.result_revisions.lock().unwrap();
                    revisions.next_revision(case_id, &self.constraint_id)
                };
                log::warn!(
                    "not coexistence a handler: claim token for ({}/{}) is denied at case end but no result was emitted",
                    case_id,
                    self.constraint_id
                );
                emit_result_at(
                    decided_at,
                    revision,
                    case_id,
                    &self.constraint_id,
                    ConstraintType::NotCoexistence,
                    ConstraintResultType::Violated,
                    format!(
                        "execution of event {} is forbidden, because not coexistence B event happened first",
                        self.event_id
                    ),
                    state.clone(),
                    ResultTrigger::CaseEnd,
                    None,
                )
                .await;
                return;
            }
            Some(_) => {
                // A happened and was allowed - the B side is responsible for
                // the remaining outcome
                return;
            }
        };

        emit_result_at(
            decided_at,
            revision,
            case_id,
            &self.constraint_id,
            ConstraintType::NotCoexistence,
            ConstraintResultType::Satisfied,
            info,
            state.clone(),
            ResultTrigger::CaseEnd,
            None,
        )
        .await;
    }

    async fn clean_up_state(&self, case_id: &CaseId, state: AppState) {
        log::debug!(
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

impl NotCoexistenceAHandler {
    /// ask the B side for its claim token and return the B events timestamp
    async fn fetch_other_event_time(
        &self,
        claim_token: &Token,
        state: AppState,
    ) -> Result<Option<EventTime>, ()> {
        let message = PeerMessage::conformance_req(
            Uuid::new_v4(),
            CCMessage::push_claim_token(claim_token.clone(), self.role),
        );
        match send_and_receive_conformance(self.b_peer, message, state.clone()).await {
            Ok(response) => match response.payload {
                PeerMessagePayload::Conformance(cc_message) => match cc_message.token {
                    Some(token) => Ok(token.event_time),
                    None => Ok(None),
                },
                _ => {
                    log::error!(
                        "not coexistence a handler: received wrong message type: \"{}\"; expected type: \"conformance\"",
                        response.payload
                    );
                    Err(())
                }
            },
            Err(e) => {
                log::error!(
                    "not coexistence a handler: error receiving token from {}: {}",
                    self.b_peer,
                    e
                );
                Err(())
            }
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

        // record the earliest local occurrence and decide whether a negotiation is needed
        let (mut claim_token, negotiate, mut decided_at) = {
            let mut meta_tokens = state.meta_token_store.lock().unwrap();
            match meta_tokens.get_token_mut(&event.case_id, self.index) {
                Some(stored) => {
                    let negotiate = match stored.event_time {
                        // the token was created while answering the other sides negotiation before this handler saw its own event
                        None => {
                            stored.event_time = Some(event.timestamp);
                            true
                        }
                        Some(recorded) if event.timestamp < recorded => {
                            log::info!(
                                "not coexistence b handler: event at {} is earlier than the recorded occurrence at {} - renegotiating",
                                event.timestamp,
                                recorded
                            );
                            stored.event_time = Some(event.timestamp);
                            // the recorded permission was decided against the later time
                            stored.payload = TokenPayload::None;
                            true
                        }
                        // a later occurrence does not change the earliest one
                        Some(_) => stored.payload == TokenPayload::None,
                    };
                    (stored.clone(), negotiate, Utc::now())
                }
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
                    meta_tokens.insert_meta_token(claim_token.clone(), self.index);
                    (claim_token, true, Utc::now())
                }
            }
        };

        if negotiate {
            // negotiate execution permission
            let message = PeerMessage::conformance_req(
                RequestId::new_v4(),
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

            // overwrite current decided_at timestamp because it was decided again here by acquiring the timestamp from the other side
            decided_at = Utc::now();

            // set execution permission
            match other_event_time {
                Some(other_time) => {
                    // strict <: on a timestamp tie the B side deterministically loses
                    if event.timestamp < other_time {
                        claim_token.payload.grant_execution_permission();
                        log::info!(
                            "not coexistence b handler: set execution permission to {} because {} < {}",
                            claim_token.payload.is_execution_allowed(),
                            event.timestamp,
                            other_time
                        );
                    } else {
                        claim_token.payload.deny_execution_permission();
                        log::info!(
                            "not coexistence b handler: set execution permission to {} because {} >= {}",
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
                match meta_tokens.get_token_mut(&event.case_id, self.index) {
                    Some(stored) => {
                        stored.payload = claim_token.payload.clone();
                        if stored.event_time.is_none() {
                            stored.event_time = Some(event.timestamp);
                        }
                    }
                    None => meta_tokens.insert_meta_token(claim_token.clone(), self.index),
                }
            }
        }

        // evaluate event execution based on the execution permission
        if !claim_token.payload.is_execution_allowed() {
            // take next revision number since a result will be emitted
            let revision = {
                let mut revisions = state.result_revisions.lock().unwrap();
                revisions.next_revision(&event.case_id, &self.constraint_id)
            };
            let info = format!(
                "execution of event {} is forbidden, because not coexistence A event happened first",
                self.event_id
            );
            emit_result_at(
                decided_at,
                revision,
                &event.case_id,
                &self.constraint_id,
                ConstraintType::NotCoexistence,
                ConstraintResultType::Violated,
                info,
                state.clone(),
                ResultTrigger::Event,
                Some(event.instance_id),
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
        let (mut claim_token, mut decided_at) = {
            let mut meta_tokens = state.meta_token_store.lock().unwrap();
            match meta_tokens.get_token_cloned(&message.case_id, self.index) {
                Some(token) => (token, Utc::now()),
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
                    (claim_token, Utc::now())
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
                // the local event has not been seen here yet so leave the payload untouched
                log::info!(
                    "not coexistence b handler: local event time not known yet, leaving the permission undecided (other side at {})",
                    other_event_time
                );
                return;
            }
        }

        // save token with permissions
        let permission_changed = {
            let mut meta_tokens = state.meta_token_store.lock().unwrap();
            match meta_tokens.get_token_mut(&message.case_id, self.index) {
                Some(stored) => {
                    let changed = stored.payload != claim_token.payload;
                    stored.payload = claim_token.payload.clone();
                    if stored.event_time.is_none() {
                        stored.event_time = claim_token.event_time;
                    }
                    if changed {
                        // overwrite decided_at since permissions were changed here
                        decided_at = Utc::now();
                    }
                    changed
                }
                None => {
                    meta_tokens.insert_meta_token(claim_token.clone(), self.index);
                    true
                }
            }
        };

        if permission_changed && !claim_token.payload.is_execution_allowed() {
            // take next revision number since a result will be emitted
            let revision = {
                let mut revisions = state.result_revisions.lock().unwrap();
                revisions.next_revision(&message.case_id, &self.constraint_id)
            };
            let info = format!(
                "execution of event {} is forbidden, because not coexistence A event happened first",
                self.event_id
            );
            emit_result_at(
                decided_at,
                revision,
                &message.case_id,
                &self.constraint_id,
                ConstraintType::NotCoexistence,
                ConstraintResultType::Violated,
                info,
                state.clone(),
                ResultTrigger::Event,
                message.triggering_event,
            )
            .await;
        }
    }

    /// mirror of the A side fallback
    async fn handle_case_end(&self, case_id: &CaseId, state: AppState) {
        log::info!(
            "not coexistence b handler: received case end signal for constraint {}; case={}",
            self.constraint_id,
            case_id
        );

        let result_exists = {
            let results = state.cc_results.lock().unwrap();
            results
                .get_constraint(case_id, &self.constraint_id)
                .is_some()
        };
        if result_exists {
            return;
        }

        let (claim_token, token_read_at) = {
            let meta_tokens = state.meta_token_store.lock().unwrap();
            (
                meta_tokens.get_token_cloned(case_id, self.index),
                Utc::now(),
            )
        };

        match &claim_token {
            Some(token) if token.event_time.is_none() => {
                // take next revision number since a result will be emitted
                let revision = {
                    let mut revisions = state.result_revisions.lock().unwrap();
                    revisions.next_revision(case_id, &self.constraint_id)
                };
                emit_result_at(
                    token_read_at,
                    revision,
                    case_id,
                    &self.constraint_id,
                    ConstraintType::NotCoexistence,
                    ConstraintResultType::Satisfied,
                    "only the not coexistence A event happened until case end".to_string(),
                    state.clone(),
                    ResultTrigger::CaseEnd,
                    None,
                )
                .await;
            }
            Some(token)
                if token.payload != TokenPayload::None && !token.payload.is_execution_allowed() =>
            {
                // take next revision number since a result will be emitted
                let revision = {
                    let mut revisions = state.result_revisions.lock().unwrap();
                    revisions.next_revision(case_id, &self.constraint_id)
                };
                log::warn!(
                    "not coexistence b handler: claim token for ({}/{}) is denied at case end but no result was emitted",
                    case_id,
                    self.constraint_id
                );
                emit_result_at(
                    token_read_at,
                    revision,
                    case_id,
                    &self.constraint_id,
                    ConstraintType::NotCoexistence,
                    ConstraintResultType::Violated,
                    format!(
                        "execution of event {} is forbidden, because not coexistence A event happened first",
                        self.event_id
                    ),
                    state.clone(),
                    ResultTrigger::CaseEnd,
                    None,
                )
                .await;
            }
            _ => {}
        }
    }

    async fn clean_up_state(&self, case_id: &CaseId, state: AppState) {
        log::debug!(
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
            not_coexistence_handler::{NotCoexistenceAHandler, NotCoexistenceBHandler},
        },
        network::message_types::CCMessage,
        state::app_state::AppState,
        types::{
            ConstraintResultType, ConstraintRole, ConstraintType, EventInstanceId, PeerAddress,
            Token,
        },
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
        CCMessage::push_token(token, EventInstanceId::new_v4())
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

    /// only A happened: the A side (token with event_time, decided
    /// permission) stays SILENT at case end - the Satisfied fallback for this
    /// state is the B side's responsibility (see b_side test below).
    #[tokio::test]
    async fn only_a_a_side_silent_at_case_end() {
        let case = "c10";
        let (state, b_peer) = a_state(24100, 24101, case);
        let mut mgr_rx = register_manager(&state, PeerAddress::new_conformance_address(b_peer));
        let h = handler(b_peer);

        // A event with mocked "B never ran" negotiation
        let ev = make_traced_event_at(case, EID, 10);
        let run = h.handle_event(&ev, state.clone());
        let mock =
            answer_one_conformance_request(state.clone(), &mut mgr_rx, b_response(case, None));
        tokio::join!(run, mock);
        assert!(result_type(&state, case).is_none());

        h.handle_case_end(&case.to_string(), state.clone()).await;
        assert!(
            result_type(&state, case).is_none(),
            "A side must stay silent; the B side emits the fallback for this state"
        );
    }

    /// only A happened, seen from the B side: B's token was created while
    /// answering A's negotiation (no event_time) -> B emits the Satisfied.
    #[tokio::test]
    async fn only_a_b_side_emits_satisfied_at_case_end() {
        let case = "c14";
        let (state, a_peer) = a_state(24140, 24141, case);
        let b = NotCoexistenceBHandler {
            index: 1,
            role: ConstraintRole::NotCoexistenceB,
            event_id: "not_coexistence_b_event".to_string(),
            constraint_id: CID.to_string(),
            a_peer,
        };

        // as created by B's handle_message while answering A
        let token = Token::new(
            case.to_string(),
            CID.to_string(),
            ConstraintType::NotCoexistence,
            None,
            2,
        );
        state
            .meta_token_store
            .lock()
            .unwrap()
            .insert_meta_token(token, b.index);

        b.handle_case_end(&case.to_string(), state.clone()).await;
        assert!(matches!(
            result_type(&state, case),
            Some(ConstraintResultType::Satisfied)
        ));
    }

    /// B side with no token at case end stays silent (the A side owns the
    /// "neither happened" fallback, avoiding duplicate rows).
    #[tokio::test]
    async fn neither_b_side_silent_at_case_end() {
        let case = "c15";
        let (state, a_peer) = a_state(24150, 24151, case);
        let b = NotCoexistenceBHandler {
            index: 1,
            role: ConstraintRole::NotCoexistenceB,
            event_id: "not_coexistence_b_event".to_string(),
            constraint_id: CID.to_string(),
            a_peer,
        };
        b.handle_case_end(&case.to_string(), state.clone()).await;
        assert!(result_type(&state, case).is_none());
    }

    /// A then B (B side emitted the violation remotely) -> case end emits NOTHING here,
    /// so the remote Violated is never contradicted.
    #[tokio::test]
    async fn a_then_b_case_end_emits_nothing() {
        let case = "c11";
        let (state, b_peer) = a_state(24110, 24111, case);
        let mut mgr_rx = register_manager(&state, PeerAddress::new_conformance_address(b_peer));
        let h = handler(b_peer);

        let ev = make_traced_event_at(case, EID, 10);
        let run = h.handle_event(&ev, state.clone());
        let mock =
            answer_one_conformance_request(state.clone(), &mut mgr_rx, b_response(case, None));
        tokio::join!(run, mock);

        // A's token has an event_time and a decided permission -> the A side
        // must stay silent without any query; if B ran later, B's node holds
        // the violation, and if B never ran, B's node emits the fallback
        h.handle_case_end(&case.to_string(), state.clone()).await;
        assert!(
            result_type(&state, case).is_none(),
            "A side must not emit a fallback once its token is decided"
        );
    }

    /// only B happened: the local token was created while answering B's
    /// negotiation (no event_time) -> Satisfied without any query.
    #[tokio::test]
    async fn only_b_satisfied_at_case_end() {
        let case = "c12";
        let (state, b_peer) = a_state(24120, 24121, case);
        let h = handler(b_peer);

        // simulate the token A's handle_message creates while answering B
        let token = Token::new(
            case.to_string(),
            CID.to_string(),
            ConstraintType::NotCoexistence,
            None,
            2,
        );
        state
            .meta_token_store
            .lock()
            .unwrap()
            .insert_meta_token(token, h.index());

        h.handle_case_end(&case.to_string(), state.clone()).await;
        assert!(matches!(
            result_type(&state, case),
            Some(ConstraintResultType::Satisfied)
        ));
    }

    /// a claim token created while answering the other side's negotiation
    /// (event_time None) gets the local event's timestamp recorded when that
    /// event arrives - otherwise the case end fallback query would wrongly
    /// conclude the event never happened.
    #[tokio::test]
    async fn handle_event_records_time_into_negotiated_token() {
        let case = "c13";
        let (state, b_peer) = a_state(24130, 24131, case);
        let mut mgr_rx = register_manager(&state, PeerAddress::new_conformance_address(b_peer));
        let b_handler = handler(b_peer);

        // as created by handle_message while answering the other side
        let mut token = Token::new(
            case.to_string(),
            CID.to_string(),
            ConstraintType::NotCoexistence,
            None,
            2,
        );
        token.payload.deny_execution_permission();
        state
            .meta_token_store
            .lock()
            .unwrap()
            .insert_meta_token(token, b_handler.index());

        // let negotiation run
        let ev = make_traced_event_at(case, EID, 30);
        let run = b_handler.handle_event(&ev, state.clone());
        let mock =
            answer_one_conformance_request(state.clone(), &mut mgr_rx, b_response(case, Some(10)));
        tokio::join!(run, mock);

        // denied -> local violation, and the token now carries the time
        assert!(matches!(
            result_type(&state, case),
            Some(ConstraintResultType::Violated)
        ));
        let stored = state
            .meta_token_store
            .lock()
            .unwrap()
            .get_token_cloned(&case.to_string(), b_handler.index())
            .unwrap();
        assert_eq!(stored.event_time, Some(at(30)));
    }

    /// exact timestamp tie: role-deterministic - the A side wins the tie in
    /// both negotiation paths, so B is denied and would emit the violation.
    #[tokio::test]
    async fn timestamp_tie_a_wins() {
        let case = "c16";
        let (state, b_peer) = a_state(24160, 24161, case);
        let mut mgr_rx = register_manager(&state, PeerAddress::new_conformance_address(b_peer));
        let h = handler(b_peer);

        // A and B carry the same timestamp; B's mocked answer reports t=10
        let ev = make_traced_event_at(case, EID, 10);
        let run = h.handle_event(&ev, state.clone());
        let mock =
            answer_one_conformance_request(state.clone(), &mut mgr_rx, b_response(case, Some(10)));
        tokio::join!(run, mock);

        // A granted on the tie -> no violation from the A side
        assert!(result_type(&state, case).is_none());
        let token = state
            .meta_token_store
            .lock()
            .unwrap()
            .get_token_cloned(&case.to_string(), h.index())
            .unwrap();
        assert!(token.payload.is_execution_allowed());
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
