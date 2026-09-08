use std::net::SocketAddr;

use async_trait::async_trait;

use crate::{
    conformance_checking::{
        constraint_logic::constraint_handler_trait::ConstraintHandler,
        result_emitter::{emit_result_stamped, stamp_decision},
    },
    impl_constraint_handler_fields,
    measurement::record::ResultTrigger,
    network::{
        message_types::{CCMessage, CCMessageType, PeerMessage},
        tcp_router::send_to_conformance_peer,
    },
    state::{app_state::AppState, result_revision_store::DecisionStamp},
    types::{
        CaseId, ConstraintId, ConstraintResultType, ConstraintRole, ConstraintType, EventId,
        EventInstanceId, EventTime, HandlerIndex, RequestId, Token, TracedEvent,
    },
};

pub struct OrderFirstHandler {
    pub index: HandlerIndex,
    pub role: ConstraintRole,
    pub event_id: EventId,
    pub constraint_id: ConstraintId,
}

#[async_trait]
impl ConstraintHandler for OrderFirstHandler {
    impl_constraint_handler_fields!(OrderFirstHandler);

    /// record event execution (remember last execution (or all executions) of event before receiving the message from order second for later check with timestamp)
    /// check if the token from order second exists
    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::info!(
            "order first handler: received configured event for constraint {}; event={}",
            self.constraint_id,
            event
        );

        let pending: Vec<(DecisionStamp, String)> = {
            let mut token_in = state.token_in.lock().unwrap();
            let mut token_out = state.token_out.lock().unwrap();

            match token_in.get_in_cloned(&event.case_id, self.index) {
                Some(blocking_token) => {
                    log::info!(
                        "order first handler: blocking token exists - order second event has already happened"
                    );
                    match blocking_token.event_time {
                        None => {
                            log::error!(
                                "order first handler: blocking token exists but it does not have the stop event time recorded - cannot evaluate - removing blocking token"
                            );
                            token_in.remove_in(&event.case_id, self.index);
                            Vec::new()
                        }
                        Some(order_second_event_time)
                            if event.timestamp >= order_second_event_time =>
                        {
                            vec![(
                                stamp_decision(&state, &event.case_id, &self.constraint_id),
                                format!(
                                    "order first event happened at {} after order second event at {}",
                                    event.timestamp, order_second_event_time
                                ),
                            )]
                        }
                        Some(_) => Vec::new(),
                    }
                }
                None => {
                    // no order-second-token received yet so just record the event time
                    log::info!(
                        "order first handler: no blocking token exists - order second event has not happened yet"
                    );

                    match token_out.get_out_mut(&event.case_id, self.index) {
                        // a token for recording the order first executions already exists so just append
                        Some(recording_token) => {
                            recording_token.payload.push_timestamp(event.timestamp)
                        }
                        // no token exists so create one
                        None => {
                            let mut recording_token = Token::new(
                                event.case_id.clone(),
                                self.constraint_id.clone(),
                                ConstraintType::Order,
                                Some(event.timestamp),
                            );
                            recording_token.payload.push_timestamp(event.timestamp);
                            token_out.produce_without_overwrite(recording_token, self.index);
                        }
                    };
                    Vec::new()
                }
            }
        };

        for (stamp, info) in pending {
            // produce negative result
            emit_result_stamped(
                stamp,
                &event.case_id,
                &self.constraint_id,
                ConstraintType::Order,
                ConstraintResultType::Violated,
                info,
                state.clone(),
                ResultTrigger::Event,
                Some(event.instance_id),
            )
            .await;
        }
    }

    /// receive token from order second with timestamp of first execution of order second (multiple order seconds possible?)
    /// check check that execution was after order first event(s)
    /// set the received (pushed) token so that all future order first events can be evaluated as a violation
    async fn handle_message(
        &self,
        state: AppState,
        message: CCMessage,
        _request_id: Option<RequestId>,
    ) {
        log::info!(
            "order first handler: received message for constraint {}; message={}",
            self.constraint_id,
            message
        );
        log::debug!("message={:?}", message);

        if message.message_type != CCMessageType::PushToken {
            log::warn!(
                "order first handler: unexpected message type {:?} for constraint {}, ignoring",
                message.message_type,
                self.constraint_id
            );
            return;
        }

        let order_second_event_time = match message.timestamp {
            Some(event_time) => event_time,
            None => {
                log::error!(
                    "order first handler: PushToken message for constraint {} has no timestamp, cannot evaluate",
                    self.constraint_id
                );
                return;
            }
        };

        // check that token was contained in the message
        match message.token {
            Some(token) => token,
            None => {
                log::error!(
                    "order first handler: pushtoken message did not contain a token! message: {}",
                    message
                );
                return;
            }
        };

        let pending: Vec<(DecisionStamp, EventTime)> = {
            // create blocking token if it doesnt exist yet
            let mut token_in = state.token_in.lock().unwrap();
            // retrieve the recording token
            let mut token_out = state.token_out.lock().unwrap();

            if !token_in.contains_in_token(&message.case_id, self.index) {
                // no token exists - create one
                let new_blocking_token = Token::new(
                    message.case_id.clone(),
                    self.constraint_id.clone(),
                    ConstraintType::Order,
                    Some(order_second_event_time),
                );
                token_in.receive_without_overwrite(new_blocking_token, self.index);
            }

            // if its the first time receiving an order second event execution notification then we have to check the order first executions that have been recorded till now
            match token_out.remove_out(&message.case_id, self.index) {
                Some(token) => {
                    // check all recorded event times until now
                    let order_first_executions: Vec<EventTime> =
                        token.payload.timestamps().to_vec();
                    order_first_executions
                        .into_iter()
                        .filter(|t| *t >= order_second_event_time)
                        .map(|order_first_time| {
                            (
                                stamp_decision(&state, &message.case_id, &self.constraint_id),
                                order_first_time,
                            )
                        })
                        .collect()
                }
                None => Vec::new(),
            }
        };

        for (stamp, order_first_time) in pending {
            // produce negative result
            emit_result_stamped(
                stamp,
                &message.case_id,
                &self.constraint_id,
                ConstraintType::Order,
                ConstraintResultType::Violated,
                format!(
                    "order first event happened at {} after order second event at {}",
                    order_first_time, order_second_event_time
                ),
                state.clone(),
                ResultTrigger::Event,
                message.triggering_event,
            )
            .await;
        }
    }

    async fn handle_case_end(&self, case_id: &CaseId, state: AppState) {
        log::info!(
            "order first handler: received case end signal for constraint {}; case={}",
            self.constraint_id,
            case_id
        );

        let pending = {
            let results = state.cc_results.lock().unwrap();
            if results
                .get_constraint(case_id, &self.constraint_id)
                .is_some()
            {
                None
            } else {
                Some(stamp_decision(&state, case_id, &self.constraint_id))
            }
        };

        if let Some(stamp) = pending {
            // produce result because none was emitted yet
            emit_result_stamped(
                stamp,
                case_id,
                &self.constraint_id,
                ConstraintType::Order,
                ConstraintResultType::Satisfied,
                "order first event never happened".to_string(),
                state.clone(),
                ResultTrigger::CaseEnd,
                None,
            )
            .await;
        }
    }

    async fn clean_up_state(&self, case_id: &CaseId, state: AppState) {
        log::debug!(
            "order first handler: clean up state for ({}/{})",
            case_id,
            self.constraint_id
        );

        // remove the blocking token
        {
            let mut token_in = state.token_in.lock().unwrap();
            token_in.remove_in(case_id, self.index);
        }

        // remove recording token
        {
            let mut token_out = state.token_out.lock().unwrap();
            token_out.remove_out(case_id, self.index);
        }
    }
}

pub struct OrderSecondHandler {
    pub index: HandlerIndex,
    pub role: ConstraintRole,
    pub event_id: EventId,
    pub constraint_id: ConstraintId,
    pub order_first_peers: Vec<SocketAddr>,
}

#[async_trait]
impl ConstraintHandler for OrderSecondHandler {
    impl_constraint_handler_fields!(OrderSecondHandler);

    fn configured_peers(&self) -> Vec<SocketAddr> {
        self.order_first_peers.clone()
    }

    /// create token and push it on the order first peers
    /// remember that a token was pushed on the order first peers by using a (meta) token and checking its existence
    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::info!(
            "order second handler: received configured event for constraint {}; event={}",
            self.constraint_id,
            event
        );

        // check if this is the first time receiving such event for the case
        // keep the token store lock for that time, but release lock before notifying order first peers
        let notify_peers = {
            let mut token_out = state.token_out.lock().unwrap();
            if !token_out.contains_out_token(&event.case_id, self.index) {
                // its the first time - create a token
                let remembering_token = Token::new(
                    event.case_id.clone(),
                    self.constraint_id.clone(),
                    ConstraintType::Order,
                    Some(event.timestamp),
                );
                token_out.produce_without_overwrite(remembering_token, self.index);
                true
            } else {
                false
            }
        };

        // notify order first peers
        // happens only when the order second event is executed the first time because the blocking token is only created once
        if notify_peers {
            log::info!(
                "order second handler: notifying order first peers that order second event happened"
            );
            for &peer_addr in self.order_first_peers.iter() {
                send_push_token_message(
                    peer_addr,
                    event.case_id.clone(),
                    self.constraint_id.clone(),
                    event.timestamp,
                    state.clone(),
                    event.instance_id,
                )
                .await;
            }
        }
    }

    // remove the blocking token
    async fn clean_up_state(&self, case_id: &CaseId, state: AppState) {
        log::debug!(
            "order second handler: clean up state for ({}/{})",
            case_id,
            self.constraint_id
        );
        // remove remembering token
        {
            let mut token_out = state.token_out.lock().unwrap();
            token_out.remove_out(case_id, self.index);
        }
    }
}

async fn send_push_token_message(
    peer_addr: SocketAddr,
    case_id: CaseId,
    constraint_id: ConstraintId,
    event_timestamp: EventTime,
    state: AppState,
    triggering_event: EventInstanceId,
) {
    // create token
    let token = Token::new(
        case_id,
        constraint_id,
        ConstraintType::Order,
        Some(event_timestamp),
    );
    // create message
    let message = PeerMessage::conformance(CCMessage::push_token(token, triggering_event));

    // send message
    log::info!(
        "order second handler: sending push token message to {}",
        peer_addr
    );
    if let Err(e) = send_to_conformance_peer(peer_addr, message, state.clone()).await {
        log::error!(
            "order second handler: error sending push token message to {}: {}",
            peer_addr,
            e
        );
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, TimeZone, Utc};
    use uuid::Uuid;

    use crate::{
        config::node_config::ConstraintConfig,
        conformance_checking::constraint_logic::{
            constraint_handler_trait::ConstraintHandler,
            order_constraint_handler::OrderFirstHandler,
        },
        network::message_types::CCMessage,
        state::app_state::AppState,
        types::{ConstraintResultType, ConstraintRole, ConstraintType, EventInstanceId, Token},
        util::test_tools::{
            make_config_with_constraints_and_port, make_state_from_config, make_traced_event_at,
        },
    };

    const CID: &str = "ord";
    const EID: &str = "first";

    fn at(secs: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, secs).unwrap()
    }

    fn first_state(port: u16, case: &str) -> AppState {
        let config = make_config_with_constraints_and_port(
            vec![ConstraintConfig::OrderFirst {
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

    fn handler() -> OrderFirstHandler {
        OrderFirstHandler {
            index: 1,
            role: ConstraintRole::OrderFirst,
            event_id: EID.to_string(),
            constraint_id: CID.to_string(),
        }
    }

    /// PushToken message from order-second, carrying the second event time.
    fn second_push(case: &str, secs: u32) -> CCMessage {
        let token = Token::new(
            case.to_string(),
            CID.to_string(),
            ConstraintType::Order,
            Some(at(secs)),
        );
        CCMessage::push_token(token, EventInstanceId::new_v4())
    }

    fn result_type(state: &AppState, case: &str) -> Option<ConstraintResultType> {
        let store = state.cc_results.lock().unwrap();
        store
            .get_constraint(&case.to_string(), &CID.to_string())
            .map(|r| r.constraint_result)
    }

    /// neither event → Satisfied at case end.
    #[tokio::test]
    async fn neither_satisfied_at_case_end() {
        let case = "c1";
        let state = first_state(22000, case);
        handler()
            .handle_case_end(&case.to_string(), state.clone())
            .await;
        assert!(matches!(
            result_type(&state, case),
            Some(ConstraintResultType::Satisfied)
        ));
    }

    /// only first events → Satisfied at case end.
    #[tokio::test]
    async fn only_first_satisfied_at_case_end() {
        let case = "c2";
        let state = first_state(22010, case);
        handler()
            .handle_event(&make_traced_event_at(case, EID, 10), state.clone())
            .await;
        handler()
            .handle_event(&make_traced_event_at(case, EID, 20), state.clone())
            .await;
        assert!(result_type(&state, case).is_none());
        handler()
            .handle_case_end(&case.to_string(), state.clone())
            .await;
        assert!(matches!(
            result_type(&state, case),
            Some(ConstraintResultType::Satisfied)
        ));
    }

    /// only second events → Satisfied at case end (blocking token, no firsts).
    #[tokio::test]
    async fn only_second_satisfied_at_case_end() {
        let case = "c3";
        let state = first_state(22020, case);
        handler()
            .handle_message(state.clone(), second_push(case, 10), Some(Uuid::new_v4()))
            .await;
        assert!(result_type(&state, case).is_none());
        handler()
            .handle_case_end(&case.to_string(), state.clone())
            .await;
        assert!(matches!(
            result_type(&state, case),
            Some(ConstraintResultType::Satisfied)
        ));
    }

    /// both, all firsts before seconds → Satisfied at case end.
    #[tokio::test]
    async fn both_first_before_second_satisfied() {
        let case = "c4";
        let state = first_state(22030, case);
        handler()
            .handle_event(&make_traced_event_at(case, EID, 10), state.clone())
            .await;
        handler()
            .handle_message(state.clone(), second_push(case, 20), Some(Uuid::new_v4()))
            .await;
        assert!(result_type(&state, case).is_none());
        handler()
            .handle_case_end(&case.to_string(), state.clone())
            .await;
        assert!(matches!(
            result_type(&state, case),
            Some(ConstraintResultType::Satisfied)
        ));
    }

    /// both, a first event happens AFTER the second already arrived → Violated
    /// (second push first creates a blocking token, the late first trips it).
    #[tokio::test]
    async fn first_after_second_violated_via_event() {
        let case = "c5";
        let state = first_state(22040, case);
        handler()
            .handle_message(state.clone(), second_push(case, 10), Some(Uuid::new_v4()))
            .await;
        handler()
            .handle_event(&make_traced_event_at(case, EID, 20), state.clone())
            .await;
        assert!(matches!(
            result_type(&state, case),
            Some(ConstraintResultType::Violated)
        ));
    }

    /// both, a first was recorded then a second arrives that predates it → Violated
    /// (checked against recorded firsts on message arrival).
    #[tokio::test]
    async fn first_after_second_violated_via_message() {
        let case = "c6";
        let state = first_state(22050, case);
        handler()
            .handle_event(&make_traced_event_at(case, EID, 20), state.clone())
            .await;
        handler()
            .handle_message(state.clone(), second_push(case, 10), Some(Uuid::new_v4()))
            .await;
        assert!(matches!(
            result_type(&state, case),
            Some(ConstraintResultType::Violated)
        ));
    }
}
