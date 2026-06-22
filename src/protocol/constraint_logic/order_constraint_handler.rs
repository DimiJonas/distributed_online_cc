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
        CaseId, ConstraintHandlerId, ConstraintId, ConstraintResultType, ConstraintType, EventTime,
        Token, TracedEvent,
    },
};

pub struct OrderFirstHandler {
    pub handler_id: ConstraintHandlerId,
    pub event_id: String,
    pub constraint_id: String,
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

        let order_first_event_time = {
            if let Some(timestamp) = event.timestamp_utc() {
                timestamp
            } else {
                log::warn!(
                    "order first handler: order first event did not contain a timestamp - using current time as event time"
                );
                Utc::now()
            }
        };

        if let Some(blocking_token) = {
            let token_in = state.token_in.lock().unwrap();
            token_in.get_in_cloned(&event.case_id, &self.constraint_id, self.handler_id)
        } {
            log::info!(
                "order first handler: blocking token exists - order second event has already happened"
            );
            let order_second_event_time = match blocking_token.event_time {
                Some(event_time) => event_time,
                None => {
                    log::error!(
                        "order first handler: blocking token exists but it does not have the stop event time recorded - cannot evaluate - removing blocking token"
                    );
                    {
                        let mut store = state.token_in.lock().unwrap();
                        store.remove_in(&event.case_id, &self.constraint_id, self.handler_id);
                    }
                    return;
                }
            };

            if order_first_event_time >= order_second_event_time {
                // produce negative result
                let info = format!(
                    "order first event happened at {} after order second event at {}",
                    order_first_event_time, order_second_event_time
                );
                emit_result(
                    &event.case_id,
                    &self.constraint_id,
                    ConstraintType::Order,
                    ConstraintResultType::Violated,
                    info,
                    state.clone(),
                )
                .await;
            }
        } else {
            // no order-second-token received yet - just record the event time in the recording token in token out token
            log::info!(
                "order first handler: no blocking token exists - order second event has not happened yet"
            );

            let mut token_out = state.token_out.lock().unwrap();

            match token_out.get_out_mut(&event.case_id, &self.constraint_id, self.handler_id) {
                // a token for recording the order first executions already exists
                Some(recording_token) => recording_token
                    .payload
                    .push_timestamp(order_first_event_time),

                None => {
                    let mut recording_token = Token::new(
                        event.case_id.clone(),
                        self.constraint_id.clone(),
                        ConstraintType::Order,
                        event.timestamp_utc(),
                        state.config.node_id,
                    );
                    recording_token
                        .payload
                        .push_timestamp(order_first_event_time);
                    token_out.produce_without_overwrite(recording_token, self.handler_id);
                }
            };
        };
    }

    /// receive token from order second with timestamp of first execution of order second (multiple order seconds possible?)
    /// check check that execution was after order first event(s)
    /// set the received (pushed) token so that all future order first events can be evaluated as a violation
    async fn handle_message(&self, state: AppState, message: CCMessage) {
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
        let message_token = match message.token {
            Some(token) => token,
            None => {
                log::error!(
                    "order first handler: pushtoken message did not contain a token! message: {}",
                    message
                );
                return;
            }
        };

        {
            // create blocking token if it doesnt exist yet
            let mut token_in = state.token_in.lock().unwrap();

            if !token_in.contains_in_token(&message.case_id, &self.constraint_id, self.handler_id) {
                // no token exists - create one
                let new_blocking_token = Token::new(
                    message.case_id.clone(),
                    self.constraint_id.clone(),
                    ConstraintType::Order,
                    Some(order_second_event_time),
                    message_token.created_by,
                );
                token_in.receive_without_overwrite(new_blocking_token, self.handler_id);
            }
        }
        let recording_token = {
            // retrieve the recording token
            let mut token_out = state.token_out.lock().unwrap();
            token_out.remove_out(&message.case_id, &self.constraint_id, self.handler_id)
        };

        // if its the first time receiving an order second event execution notification then we have to check the order first executions that have been recorded till now
        if let Some(token) = recording_token {
            // check all recorded event times until now
            let order_first_executions: Vec<EventTime> = token.payload.timestamps().to_vec();

            for order_first_time in order_first_executions {
                if order_first_time >= order_second_event_time {
                    // produce negative result
                    emit_result(
                        &message.case_id,
                        &self.constraint_id,
                        ConstraintType::Order,
                        ConstraintResultType::Violated,
                        format!(
                            "order first event happened at {} after order second event at {}",
                            order_first_time, order_second_event_time
                        ),
                        state.clone(),
                    )
                    .await;
                }
            }
        }
    }

    async fn handle_case_end(&self, case_id: &CaseId, state: AppState) {
        log::info!(
            "order first handler: received case end signal for constraint {}; case={}",
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
                ConstraintType::Order,
                ConstraintResultType::Satisfied,
                "order first event never happened".to_string(),
                state.clone(),
            )
            .await;
        }
    }

    async fn clean_up_state(&self, case_id: &CaseId, state: AppState) {
        log::info!(
            "order first handler: clean up state for ({}/{})",
            case_id,
            self.constraint_id
        );

        // remove the blocking token
        {
            let mut token_in = state.token_in.lock().unwrap();
            token_in.remove_in(case_id, &self.constraint_id, self.handler_id);
        }

        // remove recording token
        {
            let mut token_out = state.token_out.lock().unwrap();
            token_out.remove_out(case_id, &self.constraint_id, self.handler_id);
        }
    }
}

pub struct OrderSecondHandler {
    pub handler_id: ConstraintHandlerId,
    pub event_id: String,
    pub constraint_id: String,
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

        let order_second_event_time = {
            if let Some(timestamp) = event.timestamp_utc() {
                timestamp
            } else {
                log::warn!(
                    "order second handler: order second event did not contain a timestamp - using current time as event time"
                );
                Utc::now()
            }
        };

        // check if this is the first time receiving such event for the case
        // keep the token store lock for that time, but release lock before notifying order first peers
        let notify_peers = {
            let mut token_out = state.token_out.lock().unwrap();
            if !token_out.contains_out_token(&event.case_id, &self.constraint_id, self.handler_id) {
                // its the first time - create a token
                let remembering_token = Token::new(
                    event.case_id.clone(),
                    self.constraint_id.clone(),
                    ConstraintType::Order,
                    Some(order_second_event_time),
                    state.config.node_id,
                );
                token_out.produce_without_overwrite(remembering_token, self.handler_id);
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
                    order_second_event_time,
                    state.clone(),
                )
                .await;
            }
        }
    }

    // remove the blocking token
    async fn clean_up_state(&self, case_id: &CaseId, state: AppState) {
        log::info!(
            "order second handler: clean up state for ({}/{})",
            case_id,
            self.constraint_id
        );
        // remove remembering token
        {
            let mut token_out = state.token_out.lock().unwrap();
            token_out.remove_out(case_id, &self.constraint_id, self.handler_id);
        }
    }
}

async fn send_push_token_message(
    peer_addr: SocketAddr,
    case_id: CaseId,
    constraint_id: ConstraintId,
    event_timestamp: EventTime,
    state: AppState,
) {
    // create token
    let token = Token::new(
        case_id,
        constraint_id,
        ConstraintType::Order,
        Some(event_timestamp),
        state.config.node_id,
    );
    // create message
    let message = PeerMessage::conformance(CCMessage::push_token(token));

    // send message
    log::info!(
        "order second handler: sending push token message to {}",
        peer_addr
    );
    if let Err(e) = send_to_peer(peer_addr, message, state.clone()).await {
        log::error!(
            "order second handler: error sending push token message to {}: {}",
            peer_addr,
            e
        );
    }
}
