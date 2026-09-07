use std::net::SocketAddr;

use async_trait::async_trait;

use crate::{
    conformance_checking::{
        constraint_logic::constraint_handler_trait::ConstraintHandler,
        result_emitter::{emit_result_stamped, stamp_decision},
    },
    impl_constraint_handler_fields,
    measurement::{
        self,
        record::{CorrectionDropReason, ResultTrigger},
    },
    network::{
        message_types::{CCMessage, CCMessageType, PeerMessage, PeerMessagePayload},
        tcp_router::{send_and_receive_conformance, send_to_conformance_peer},
    },
    state::app_state::AppState,
    types::{
        CaseId, ConstraintId, ConstraintResultType, ConstraintRole, ConstraintType, EventId,
        EventInstanceId, EventTime, HandlerIndex, RequestId, Token, TokenPayload, TracedEvent,
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
    ///     - no: produce claim token and hand the negotiation with other_peer to a background task
    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::info!(
            "not coexistence a handler: received configured event for constraint {}; event={}",
            self.constraint_id,
            event
        );

        // record the earliest local occurrence and decide whether a negotiation is needed
        let (claim_token, negotiate) = {
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
                    (stored.clone(), negotiate)
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
                    (claim_token, true)
                }
            }
        };

        if negotiate {
            tokio::spawn(
                Negotiation {
                    index: self.index,
                    role: self.role,
                    side: NotCoexSide::A,
                    constraint_id: self.constraint_id.clone(),
                    event_id: self.event_id.clone(),
                    peer: self.b_peer,
                    case_id: event.case_id.clone(),
                    triggering_event: Some(event.instance_id),
                }
                .run(state.clone()),
            );
            return;
        }

        // nothing to negotiate: the stored permission already decides this event
        let pending = if claim_token.payload.is_execution_allowed() {
            None
        } else {
            Some(stamp_decision(&state, &event.case_id, &self.constraint_id))
        };

        if let Some(stamp) = pending {
            emit_result_stamped(
                stamp,
                &event.case_id,
                &self.constraint_id,
                ConstraintType::NotCoexistence,
                ConstraintResultType::Violated,
                NotCoexSide::A.violation_info(&self.event_id),
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
                // <= so on a timestamp tie the A side deterministically wins (B has <)
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

        let pending = {
            let mut meta_tokens = state.meta_token_store.lock().unwrap();
            let permission_changed = match meta_tokens.get_token_mut(&message.case_id, self.index) {
                Some(stored) => {
                    let changed = stored.payload != claim_token.payload;
                    stored.payload = claim_token.payload.clone();
                    if stored.event_time.is_none() {
                        stored.event_time = claim_token.event_time;
                    }
                    changed
                }
                None => {
                    meta_tokens.insert_meta_token(claim_token.clone(), self.index);
                    true
                }
            };

            if permission_changed && !claim_token.payload.is_execution_allowed() {
                Some(stamp_decision(
                    &state,
                    &message.case_id,
                    &self.constraint_id,
                ))
            } else {
                None
            }
        };

        if let Some(stamp) = pending {
            let info = format!(
                "execution of event {} is forbidden, because not coexistence B event happened first",
                self.event_id
            );
            emit_result_stamped(
                stamp,
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

        let claim_token = {
            let meta_tokens = state.meta_token_store.lock().unwrap();
            let results = state.cc_results.lock().unwrap();
            if results
                .get_constraint(case_id, &self.constraint_id)
                .is_some()
            {
                return;
            }
            meta_tokens.get_token_cloned(case_id, self.index)
        };

        let (result, info) = match &claim_token {
            None => (
                ConstraintResultType::Satisfied,
                "none of the not coexistence events happened until case end".to_string(),
            ),
            Some(token) if token.event_time.is_none() => (
                ConstraintResultType::Satisfied,
                "only the not coexistence B event happened until case end".to_string(),
            ),
            Some(token) if token.payload == TokenPayload::None => {
                // incomplete negotiation - ask the B side as a best effort
                match fetch_other_event_time(self.b_peer, self.role, token, state.clone()).await {
                    Ok(None) => (
                        ConstraintResultType::Satisfied,
                        "only the not coexistence A event happened until case end".to_string(),
                    ),
                    Ok(Some(_)) | Err(()) => {
                        log::warn!(
                            "not coexistence a handler: claim token for ({}/{}) has an undecided permission at case end. emitting no fallback",
                            case_id,
                            self.constraint_id
                        );
                        return;
                    }
                }
            }
            Some(token) if !token.payload.is_execution_allowed() => {
                log::warn!(
                    "not coexistence a handler: claim token for ({}/{}) is denied at case end but no result was emitted",
                    case_id,
                    self.constraint_id
                );
                (
                    ConstraintResultType::Violated,
                    format!(
                        "execution of event {} is forbidden, because not coexistence B event happened first",
                        self.event_id
                    ),
                )
            }
            Some(_) => {
                // A happened and was allowed
                return;
            }
        };

        let stamp = stamp_decision(&state, case_id, &self.constraint_id);
        emit_result_stamped(
            stamp,
            case_id,
            &self.constraint_id,
            ConstraintType::NotCoexistence,
            result,
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
    ///     - no: produce claim token and hand the negotiation with other_peer to a background task
    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::info!(
            "not coexistence b handler: received configured event for constraint {}; event={}",
            self.constraint_id,
            event
        );

        // record the earliest local occurrence and decide whether a negotiation is needed
        let (claim_token, negotiate) = {
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
                    (stored.clone(), negotiate)
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
                    (claim_token, true)
                }
            }
        };

        if negotiate {
            tokio::spawn(
                Negotiation {
                    index: self.index,
                    role: self.role,
                    side: NotCoexSide::B,
                    constraint_id: self.constraint_id.clone(),
                    event_id: self.event_id.clone(),
                    peer: self.a_peer,
                    case_id: event.case_id.clone(),
                    triggering_event: Some(event.instance_id),
                }
                .run(state.clone()),
            );
            return;
        }

        // nothing to negotiate: the stored permission already decides this event
        let pending = if claim_token.payload.is_execution_allowed() {
            None
        } else {
            Some(stamp_decision(&state, &event.case_id, &self.constraint_id))
        };

        if let Some(stamp) = pending {
            emit_result_stamped(
                stamp,
                &event.case_id,
                &self.constraint_id,
                ConstraintType::NotCoexistence,
                ConstraintResultType::Violated,
                NotCoexSide::B.violation_info(&self.event_id),
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

        // get existing token or create one
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
                // the local event has not been seen here yet so leave the payload untouched
                log::info!(
                    "not coexistence b handler: local event time not known yet, leaving the permission undecided (other side at {})",
                    other_event_time
                );
                return;
            }
        }

        let pending = {
            let mut meta_tokens = state.meta_token_store.lock().unwrap();
            let permission_changed = match meta_tokens.get_token_mut(&message.case_id, self.index) {
                Some(stored) => {
                    let changed = stored.payload != claim_token.payload;
                    stored.payload = claim_token.payload.clone();
                    if stored.event_time.is_none() {
                        stored.event_time = claim_token.event_time;
                    }
                    changed
                }
                None => {
                    meta_tokens.insert_meta_token(claim_token.clone(), self.index);
                    true
                }
            };

            if permission_changed && !claim_token.payload.is_execution_allowed() {
                Some(stamp_decision(
                    &state,
                    &message.case_id,
                    &self.constraint_id,
                ))
            } else {
                None
            }
        };

        if let Some(stamp) = pending {
            let info = format!(
                "execution of event {} is forbidden, because not coexistence A event happened first",
                self.event_id
            );
            emit_result_stamped(
                stamp,
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

    async fn handle_case_end(&self, case_id: &CaseId, state: AppState) {
        log::info!(
            "not coexistence b handler: received case end signal for constraint {}; case={}",
            self.constraint_id,
            case_id
        );

        let pending = {
            let meta_tokens = state.meta_token_store.lock().unwrap();
            let results = state.cc_results.lock().unwrap();

            if results
                .get_constraint(case_id, &self.constraint_id)
                .is_some()
            {
                None
            } else {
                match meta_tokens.get_token_cloned(case_id, self.index) {
                    Some(token) if token.event_time.is_none() => Some((
                        stamp_decision(&state, case_id, &self.constraint_id),
                        ConstraintResultType::Satisfied,
                        "only the not coexistence A event happened until case end".to_string(),
                    )),
                    Some(token)
                        if token.payload != TokenPayload::None
                            && !token.payload.is_execution_allowed() =>
                    {
                        log::warn!(
                            "not coexistence b handler: claim token for ({}/{}) is denied at case end but no result was emitted",
                            case_id,
                            self.constraint_id
                        );
                        Some((
                            stamp_decision(&state, case_id, &self.constraint_id),
                            ConstraintResultType::Violated,
                            format!(
                                "execution of event {} is forbidden, because not coexistence A event happened first",
                                self.event_id
                            ),
                        ))
                    }
                    _ => None,
                }
            }
        };

        if let Some((stamp, result, info)) = pending {
            emit_result_stamped(
                stamp,
                case_id,
                &self.constraint_id,
                ConstraintType::NotCoexistence,
                result,
                info,
                state.clone(),
                ResultTrigger::CaseEnd,
                None,
            )
            .await;
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

/// which half of the constraint a negotiation runs for
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NotCoexSide {
    A,
    B,
}

impl NotCoexSide {
    fn label(self) -> &'static str {
        match self {
            NotCoexSide::A => "a",
            NotCoexSide::B => "b",
        }
    }

    /// decide whether the local occurrence is allowed to execute (A wins on a tie)
    fn is_allowed(self, local_time: EventTime, other_event_time: Option<EventTime>) -> bool {
        match other_event_time {
            None => true, // the other side has not seen its event
            Some(other_time) => match self {
                NotCoexSide::A => local_time <= other_time,
                NotCoexSide::B => local_time < other_time,
            },
        }
    }

    fn violation_info(self, event_id: &str) -> String {
        let other = match self {
            NotCoexSide::A => "B",
            NotCoexSide::B => "A",
        };
        format!(
            "execution of event {} is forbidden, because not coexistence {} event happened first",
            event_id, other
        )
    }
}

/// everything one execution permission negotiation needs so it can run detached in its own task
struct Negotiation {
    index: HandlerIndex,
    role: ConstraintRole,
    side: NotCoexSide,
    constraint_id: ConstraintId,
    event_id: EventId,
    peer: SocketAddr,
    case_id: CaseId,
    triggering_event: Option<EventInstanceId>,
}

impl Negotiation {
    async fn run(self, state: AppState) {
        // use the existing claim token (created in handle_event())
        let claim_token = {
            let meta_tokens = state.meta_token_store.lock().unwrap();
            meta_tokens.get_token_cloned(&self.case_id, self.index)
        };
        let Some(claim_token) = claim_token else {
            log::warn!(
                "not coexistence {} handler: the claim token for ({}/{}) is gone before the negotiation started",
                self.side.label(),
                self.case_id,
                self.constraint_id
            );
            return;
        };

        let other_event_time = match fetch_other_event_time(
            self.peer,
            self.role,
            &claim_token,
            state.clone(),
        )
        .await
        {
            Ok(timestamp) => timestamp,
            Err(()) => {
                log::error!(
                    "not coexistence {} handler: the negotiation for ({}/{}) with {} failed. the permission stays undecided",
                    self.side.label(),
                    self.case_id,
                    self.constraint_id,
                    self.peer
                );
                return;
            }
        };

        // apply the answer against the current recorded occurence
        let pending = {
            let mut meta_tokens = state.meta_token_store.lock().unwrap();
            let Some(stored) = meta_tokens.get_token_mut(&self.case_id, self.index) else {
                return;
            };
            let Some(local_time) = stored.event_time else {
                // the local event time is gone - nothing to decide
                return;
            };

            let allowed = self.side.is_allowed(local_time, other_event_time);
            if allowed {
                stored.payload.grant_execution_permission();
            } else {
                stored.payload.deny_execution_permission();
            }
            log::info!(
                "not coexistence {} handler: set execution permission for ({}/{}) to {} (local {} vs other {:?})",
                self.side.label(),
                self.case_id,
                self.constraint_id,
                allowed,
                local_time,
                other_event_time
            );

            if allowed {
                None
            } else {
                Some(stamp_decision(&state, &self.case_id, &self.constraint_id))
            }
        };

        let Some(stamp) = pending else {
            return;
        };

        // check if case was closed while the negotiation was happening
        {
            let case_state = state.case_state.lock().unwrap();
            if case_state.is_closed(&self.case_id) {
                log::warn!(
                    "not coexistence {} handler: the case of ({}/{}) closed while the negotiation was running. dropping the verdict",
                    self.side.label(),
                    self.case_id,
                    self.constraint_id
                );
                drop(case_state);
                measurement::api::correction_dropped(
                    &self.case_id,
                    &self.constraint_id,
                    ConstraintType::NotCoexistence,
                    CorrectionDropReason::CaseAlreadyClosed,
                    None,
                );
                return;
            }
        }

        emit_result_stamped(
            stamp,
            &self.case_id,
            &self.constraint_id,
            ConstraintType::NotCoexistence,
            ConstraintResultType::Violated,
            self.side.violation_info(&self.event_id),
            state.clone(),
            ResultTrigger::Event,
            self.triggering_event,
        )
        .await;
    }
}

/// ask the other side for its claim token and return the timestamp of its event
async fn fetch_other_event_time(
    peer: SocketAddr,
    role: ConstraintRole,
    claim_token: &Token,
    state: AppState,
) -> Result<Option<EventTime>, ()> {
    let message = PeerMessage::conformance_req(
        RequestId::new_v4(),
        CCMessage::push_claim_token(claim_token.clone(), role),
    );
    match send_and_receive_conformance(peer, message, state).await {
        Ok(response) => match response.payload {
            PeerMessagePayload::Conformance(cc_message) => match cc_message.token {
                Some(token) => Ok(token.event_time),
                None => Ok(None),
            },
            other => {
                log::error!(
                    "not coexistence handler: received wrong message type: \"{}\"; expected type: \"conformance\"",
                    other
                );
                Err(())
            }
        },
        Err(e) => {
            log::error!(
                "not coexistence handler: error receiving token from {}: {}",
                peer,
                e
            );
            Err(())
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

    /// handle_event hands the negotiation to a background task now, so the outcome of a mocked
    /// negotiation lands a few polls after handle_event returned
    async fn settle() {
        for _ in 0..200 {
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        for _ in 0..200 {
            tokio::task::yield_now().await;
        }
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
        settle().await;
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
        settle().await;

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
        settle().await;

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
        settle().await;

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

    /// handle_event must return before the other side has answered: the event queue processor
    /// awaits it, so a round trip inside it would cost this node one rtt of throughput per event
    #[tokio::test]
    async fn handle_event_does_not_wait_for_the_negotiation() {
        let case = "c17";
        let (state, b_peer) = a_state(24170, 24171, case);
        let _mgr_rx = register_manager(&state, PeerAddress::new_conformance_address(b_peer));
        let h = handler(b_peer);
        let ev = make_traced_event_at(case, EID, 10);

        // nobody ever answers, so this would block for the response timeout if the negotiation
        // were still inline
        tokio::time::timeout(
            std::time::Duration::from_millis(500),
            h.handle_event(&ev, state.clone()),
        )
        .await
        .expect("handle_event must not block on the negotiation");
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
        settle().await;

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
        settle().await;

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
        settle().await;

        assert!(matches!(
            result_type(&state, case),
            Some(ConstraintResultType::Violated)
        ));
    }
}
