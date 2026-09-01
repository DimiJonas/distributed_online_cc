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
        message_types::{CCMessage, CCMessageType, PeerMessage},
        tcp_router::send_to_conformance_peer,
    },
    state::app_state::AppState,
    types::{
        CaseId, ConstraintId, ConstraintResultType, ConstraintRole, ConstraintType, EventId,
        HandlerIndex, RequestId, Token, TracedEvent,
    },
};

pub struct PrecedenceProducerHandler {
    pub index: HandlerIndex,
    pub role: ConstraintRole,
    pub constraint_id: ConstraintId,
    pub event_id: EventId,
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

        // check if token already exists
        let token = {
            let mut token_out = state.token_out.lock().unwrap();
            if token_out.contains_out_token(&event.case_id, self.index) {
                log::debug!(
                    "precedence producer handler: token already exists for case={} constraint={}",
                    event.case_id,
                    self.constraint_id
                );
                return;
            }
            let producer_token = Token::new(
                event.case_id.clone(),
                self.constraint_id.clone(),
                ConstraintType::Precedence,
                Some(event.timestamp),
                state.config.node_id,
            );
            token_out.produce_without_overwrite(producer_token.clone(), self.index);
            log::info!(
                "precedence producer handler: produced token for case={} constraint={}",
                event.case_id,
                self.constraint_id
            );
            producer_token
        };

        // send token
        let message = PeerMessage::conformance(CCMessage::push_token(token, event.instance_id));
        for peer_address in self.consumer_peers.iter() {
            if let Err(e) =
                send_to_conformance_peer(*peer_address, message.clone(), state.clone()).await
            {
                log::error!(
                    "precedence producer handler: error sending push token message to {}: {}",
                    peer_address,
                    e
                );
            }
        }
    }

    async fn clean_up_state(&self, case_id: &CaseId, state: AppState) {
        log::debug!(
            "precedence producer handler: clean up state for ({}/{})",
            case_id,
            self.constraint_id
        );

        // remove producer token
        {
            let mut token_out = state.token_out.lock().unwrap();
            token_out.remove_out(case_id, self.index);
        }
    }
}

pub struct PrecedenceConsumerHandler {
    pub index: HandlerIndex,
    pub role: ConstraintRole,
    pub constraint_id: ConstraintId,
    pub event_id: EventId,
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

        let pending = {
            let token_in = state.token_in.lock().unwrap();
            let mut token_out = state.token_out.lock().unwrap();

            // keep if its the earliest
            let is_earliest = match token_out
                .get_out_cloned(&event.case_id, self.index)
                .and_then(|t| t.event_time)
            {
                Some(existing) => event.timestamp < existing,
                None => true,
            };
            if is_earliest {
                token_out.produce_with_overwrite(
                    Token::new(
                        event.case_id.clone(),
                        self.constraint_id.clone(),
                        ConstraintType::Precedence,
                        Some(event.timestamp),
                        state.config.node_id,
                    ),
                    self.index,
                );
            }

            match token_in
                .get_in_cloned(&event.case_id, self.index)
                .and_then(|t| t.event_time)
            {
                Some(producer_event_time) if event.timestamp > producer_event_time => None,

                // producer event is known but happened later: violated
                Some(producer_event_time) => Some((
                    stamp_decision(&state, &event.case_id, &self.constraint_id),
                    ConstraintResultType::Violated,
                    format!(
                        "consumer event happened before producer event: {} <= {}",
                        event.timestamp, producer_event_time
                    ),
                )),

                // producer event not seen (yet): violated for now (handle_message corrects it if the token was only delayed)
                None => Some((
                    stamp_decision(&state, &event.case_id, &self.constraint_id),
                    ConstraintResultType::Violated,
                    "producer event has not happened (yet)".to_string(),
                )),
            }
        };

        if let Some((stamp, result, info)) = pending {
            emit_result_stamped(
                stamp,
                &event.case_id,
                &self.constraint_id,
                ConstraintType::Precedence,
                result,
                info,
                state.clone(),
                ResultTrigger::Event,
                Some(event.instance_id),
            )
            .await;
        }
    }

    async fn handle_message(
        &self,
        state: AppState,
        message: CCMessage,
        _request_id: Option<RequestId>,
    ) {
        log::info!(
            "precedence consumer handler: received message for constraint {}; message={}",
            self.constraint_id,
            message
        );
        log::debug!("message={:?}", message);

        if message.message_type != CCMessageType::PushToken {
            log::warn!(
                "precedence consumer handler: unexpected message type {} for constraint {}, ignoring",
                message.message_type,
                self.constraint_id
            );
            return;
        }

        let new_producer_token = match message.token.clone() {
            Some(token) => token,
            None => {
                log::error!(
                    "precedence consumer handler: push token message did not contain a token. message={}",
                    message
                );
                return;
            }
        };

        let new_event_time = match new_producer_token.event_time {
            Some(timestamp) => timestamp,
            None => {
                log::error!(
                    "precedence consumer handler: token in push token message did not contain a timestamp. message={}",
                    message
                );
                return;
            }
        };

        let pending = {
            let mut token_in = state.token_in.lock().unwrap();
            let token_out = state.token_out.lock().unwrap();

            // save token if none exists yet or the event time is earlier than the current one
            let producer_event_time = match token_in.get_in_mut(&message.case_id, self.index) {
                Some(existing_producer_token) => match existing_producer_token.event_time {
                    Some(existing_event_time) => {
                        if new_event_time < existing_event_time {
                            existing_producer_token.event_time = Some(new_event_time);
                            log::info!(
                                "precedence consumer handler: received precedence producer token (update)"
                            );
                            new_event_time
                        } else {
                            log::info!(
                                "precedence consumer handler: received precedence producer token (no update)"
                            );
                            existing_event_time
                        }
                    }
                    None => {
                        existing_producer_token.event_time = Some(new_event_time);
                        log::info!(
                            "precedence consumer handler: received precedence producer token (insert timestamp)"
                        );
                        new_event_time
                    }
                },
                None => {
                    token_in.receive_without_overwrite(new_producer_token, self.index);
                    log::info!(
                        "precedence consumer handler: received precedence producer token (insert)"
                    );
                    new_event_time
                }
            };

            let result_already_published = {
                let results = state.cc_results.lock().unwrap();
                results
                    .get_constraint(&message.case_id, &self.constraint_id)
                    .is_some()
            };

            match token_out
                .get_out_cloned(&message.case_id, self.index)
                .and_then(|token| token.event_time)
            {
                Some(consumer_event_time) => {
                    let (result, info) = if consumer_event_time <= producer_event_time {
                        (
                            ConstraintResultType::Violated,
                            format!(
                                "consumer event happened before producer event: {} <= {} (late correction)",
                                consumer_event_time, producer_event_time
                            ),
                        )
                    } else {
                        (
                            ConstraintResultType::Satisfied,
                            format!(
                                "consumer event happened after producer event: {} > {} (late correction)",
                                consumer_event_time, producer_event_time
                            ),
                        )
                    };
                    Some((
                        stamp_decision(&state, &message.case_id, &self.constraint_id),
                        result,
                        info,
                    ))
                }
                // no consumer event time in token_out
                None => {
                    if result_already_published {
                        log::warn!(
                            "precedence consumer handler: producer token for ({}/{}) arrived after the case state was cleaned up. the published verdict stays uncorrected",
                            message.case_id,
                            self.constraint_id
                        );
                        measurement::api::correction_dropped(
                            &message.case_id,
                            &self.constraint_id,
                            ConstraintType::Precedence,
                            CorrectionDropReason::CaseAlreadyClosed,
                            message.sender_address,
                        );
                    }
                    None
                }
            }
        };

        if let Some((stamp, result, info)) = pending {
            emit_result_stamped(
                stamp,
                &message.case_id,
                &self.constraint_id,
                ConstraintType::Precedence,
                result,
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
            "precedence consumer handler: received case end signal for constraint {}; case={}",
            self.constraint_id,
            case_id
        );

        let pending = {
            let token_in = state.token_in.lock().unwrap();
            let token_out = state.token_out.lock().unwrap();
            let results = state.cc_results.lock().unwrap();

            if results
                .get_constraint(case_id, &self.constraint_id)
                .is_some()
            {
                // a result was already published
                None
            } else {
                let producer_seen = token_in.contains_in_token(case_id, self.index);
                let consumer_seen = token_out.contains_out_token(case_id, self.index);

                let (result, info) = match (producer_seen, consumer_seen) {
                    // satisfied: no violations
                    (true, true) => (
                        ConstraintResultType::Satisfied,
                        "every consumer event was preceded by the producer event".to_string(),
                    ),
                    // satisfied: nothing to precede
                    (true, false) => (
                        ConstraintResultType::Satisfied,
                        "consumer event never happened".to_string(),
                    ),
                    // no events at all
                    (false, false) => (
                        ConstraintResultType::Satisfied,
                        "neither producer nor consumer event happened".to_string(),
                    ),
                    // should never be reached because handle_event always emits violated in this situation
                    (false, true) => {
                        log::warn!(
                            "precedence consumer handler: consumer event without producer token and no published result for ({}/{})",
                            case_id,
                            self.constraint_id
                        );
                        (
                            ConstraintResultType::Violated,
                            "consumer event happened, producer event never arrived".to_string(),
                        )
                    }
                };

                Some((
                    stamp_decision(&state, case_id, &self.constraint_id),
                    result,
                    info,
                ))
            }
        };

        if let Some((stamp, result, info)) = pending {
            emit_result_stamped(
                stamp,
                case_id,
                &self.constraint_id,
                ConstraintType::Precedence,
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
            "precedence consumer handler: clean up state for ({}/{})",
            case_id,
            self.constraint_id
        );

        // remove producer token
        {
            let mut token_in = state.token_in.lock().unwrap();
            token_in.remove_in(case_id, self.index);
        }
        // remove consumer token
        {
            let mut token_out = state.token_out.lock().unwrap();
            token_out.remove_out(case_id, self.index);
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use crate::{
        config::node_config::ConstraintConfig,
        conformance_checking::constraint_logic::{
            constraint_handler_trait::ConstraintHandler,
            precedence_constraint_handler::PrecedenceConsumerHandler,
        },
        network::message_types::CCMessage,
        state::app_state::AppState,
        types::{
            ConstraintResultType, ConstraintRole, ConstraintType, EventInstanceId, Token,
            TracedEvent,
        },
        util::test_tools::{make_config_with_constraints_and_port, make_state_from_config},
    };
    use chrono::DateTime;

    const CID: &str = "prec";
    const EID: &str = "consume";

    fn at(secs: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, secs).unwrap()
    }

    fn consumer_state(port: u16, case: &str) -> AppState {
        let config = make_config_with_constraints_and_port(
            vec![ConstraintConfig::PrecedenceConsumer {
                constraint_id: CID.to_string(),
                event_id: EID.to_string(),
            }],
            port,
        );
        let state = make_state_from_config(config);
        state
            .cc_results
            .lock()
            .unwrap()
            .register_case(case.to_string());
        state
    }

    fn handler() -> PrecedenceConsumerHandler {
        PrecedenceConsumerHandler {
            index: 1,
            role: ConstraintRole::PrecedenceConsumer,
            constraint_id: CID.to_string(),
            event_id: EID.to_string(),
        }
    }

    /// Build a consumer event carrying an explicit timestamp via XES.
    fn consumer_event(case: &str, secs: i64) -> TracedEvent {
        crate::util::test_tools::make_traced_event_at(case, EID, secs)
    }

    /// Push a producer token (as it would arrive over the network) with the
    /// given event time.
    fn producer_token(case: &str, secs: u32) -> CCMessage {
        let token = Token::new(
            case.to_string(),
            CID.to_string(),
            ConstraintType::Precedence,
            Some(at(secs)),
            1,
        );
        CCMessage::push_token(token, EventInstanceId::new_v4())
    }

    fn result_type(state: &AppState, case: &str) -> Option<ConstraintResultType> {
        let store = state.cc_results.lock().unwrap();
        store
            .get_constraint(&case.to_string(), &CID.to_string())
            .map(|r| r.constraint_result)
    }

    fn result_info(state: &AppState, case: &str) -> Option<String> {
        let store = state.cc_results.lock().unwrap();
        store
            .get_constraint(&case.to_string(), &CID.to_string())
            .map(|r| r.info.clone())
    }

    fn revision_count(state: &AppState, case: &str) -> i64 {
        state
            .result_revisions
            .lock()
            .unwrap()
            .current_revision(&case.to_string(), &CID.to_string())
    }

    /// neither producer nor consumer event → Satisfied at case end.
    #[tokio::test]
    async fn neither_event_satisfied_at_case_end() {
        let case = "c1";
        let state = consumer_state(21000, case);
        handler()
            .handle_case_end(&case.to_string(), state.clone())
            .await;
        assert!(matches!(
            result_type(&state, case),
            Some(ConstraintResultType::Satisfied)
        ));
    }

    /// only producer event → Satisfied at case end (token stored, no consumer).
    #[tokio::test]
    async fn only_producer_satisfied_at_case_end() {
        let case = "c2";
        let state = consumer_state(21010, case);
        handler()
            .handle_message(state.clone(), producer_token(case, 10), None)
            .await;
        handler()
            .handle_case_end(&case.to_string(), state.clone())
            .await;
        assert!(matches!(
            result_type(&state, case),
            Some(ConstraintResultType::Satisfied)
        ));
        assert_eq!(
            result_info(&state, case).as_deref(),
            Some("consumer event never happened")
        );
    }

    /// only consumer event (no producer token) → Violated.
    #[tokio::test]
    async fn only_consumer_violated() {
        let case = "c3";
        let state = consumer_state(21020, case);
        handler()
            .handle_event(&consumer_event(case, 10), state.clone())
            .await;
        assert!(matches!(
            result_type(&state, case),
            Some(ConstraintResultType::Violated)
        ));
    }

    /// both: producer before consumer → no violation during case, Satisfied at end.
    #[tokio::test]
    async fn producer_before_consumer_satisfied() {
        let case = "c4";
        let state = consumer_state(21030, case);
        handler()
            .handle_message(state.clone(), producer_token(case, 10), None)
            .await;
        handler()
            .handle_event(&consumer_event(case, 20), state.clone())
            .await;
        // no result emitted by the consumer event itself
        assert!(result_type(&state, case).is_none());
        handler()
            .handle_case_end(&case.to_string(), state.clone())
            .await;
        assert!(matches!(
            result_type(&state, case),
            Some(ConstraintResultType::Satisfied)
        ));
        // and it says so for the right reason
        assert_eq!(
            result_info(&state, case).as_deref(),
            Some("every consumer event was preceded by the producer event")
        );
    }

    /// both: consumer before producer → Violated.
    #[tokio::test]
    async fn consumer_before_producer_violated() {
        let case = "c5";
        let state = consumer_state(21040, case);
        // producer token recorded with a later time than the consumer event
        handler()
            .handle_message(state.clone(), producer_token(case, 20), None)
            .await;
        handler()
            .handle_event(&consumer_event(case, 10), state.clone())
            .await;
        assert!(matches!(
            result_type(&state, case),
            Some(ConstraintResultType::Violated)
        ));
    }

    /// a delayed producer token corrects the published violation, and the
    /// correction gets the *higher* revision - this is the WIA regression.
    #[tokio::test]
    async fn late_producer_token_corrects_with_a_higher_revision() {
        let case = "c6";
        let state = consumer_state(21050, case);

        // consumer event arrives first, producer token is still in flight
        handler()
            .handle_event(&consumer_event(case, 20), state.clone())
            .await;
        assert!(matches!(
            result_type(&state, case),
            Some(ConstraintResultType::Violated)
        ));
        assert_eq!(revision_count(&state, case), 1);

        // the token arrives late and carries an earlier event time
        handler()
            .handle_message(state.clone(), producer_token(case, 10), None)
            .await;
        assert!(matches!(
            result_type(&state, case),
            Some(ConstraintResultType::Satisfied)
        ));
        // the correction is revision 2, not revision 1
        assert_eq!(revision_count(&state, case), 2);

        // case end must not add another verdict on top
        handler()
            .handle_case_end(&case.to_string(), state.clone())
            .await;
        assert_eq!(revision_count(&state, case), 2);
        assert!(matches!(
            result_type(&state, case),
            Some(ConstraintResultType::Satisfied)
        ));
    }
}
