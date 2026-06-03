use std::net::SocketAddr;

use async_trait::async_trait;
use chrono::Utc;

use crate::{
    impl_constraint_handler_fields,
    network::{
        message_types::{CCMessage, CCMessageType, PeerMessage},
        tcp_router::send_to_peer,
    },
    protocol::constraint_logic::constraint_handler_trait::ConstraintHandler,
    state::app_state::AppState,
    types::{
        CaseId, ConstraintId, ConstraintResultType, ConstraintType, EventTime, Token, TracedEvent,
    },
};

pub struct OrderFirstHandler {
    pub event_id: String,
    pub constraint_id: String,
}

#[async_trait]
impl ConstraintHandler for OrderFirstHandler {
    impl_constraint_handler_fields!(OrderFirstHandler);

    /// record event execution (remember last execution (or all executions) of event before receiving the message from order second for later check with timestamp)
    /// check if the token from order second is existent
    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        let order_first_event_time = event.timestamp_utc().unwrap_or(Utc::now());
        if let Some(sperr_token) = {
            let store = state.token_store.lock().unwrap();
            store.get_in_cloned(&event.case_id, &self.constraint_id)
        } {
            log::info!("blocking token exists - order second event has already happened");
            let order_second_event_time = match sperr_token.event_time {
                Some(event_time) => event_time,
                None => sperr_token.created_at,
            };

            let mut result_store = state.cc_results.lock().unwrap();
            if order_first_event_time >= order_second_event_time {
                // produce negative result
                let info = format!(
                    "order first event happened at {} before order second event that happened at {}",
                    order_first_event_time, order_second_event_time
                );
                result_store.update(
                    &event.case_id,
                    &self.constraint_id,
                    ConstraintResultType::Violated,
                    info,
                );
            } else {
                // produce positive result
                let info = format!(
                    "all executions of event {} happened before the first execution of the order second event for constraint {} for case {}",
                    &self.event_id, &self.constraint_id, event.case_id
                );
                result_store.update(
                    &event.case_id,
                    &self.constraint_id,
                    ConstraintResultType::Satisfied,
                    info,
                );
            }

            // TODO: replace with sending the result somewhere and delete result afterwards
            log::info!(
                "order constraint check result {}",
                result_store
                    .get_constraint(&event.case_id, &self.constraint_id)
                    .unwrap()
            );
            log::debug!(
                "order constraint check result {:?}",
                result_store
                    .get_constraint(&event.case_id, &self.constraint_id)
                    .unwrap()
            )
        } else {
            // no order-second-token received yet - just record the event time in the recording token in token out token
            log::info!("no blocking token exists - order second event has not happened yet");

            let mut store = state.token_store.lock().unwrap();

            match store.get_out_mut(&event.case_id, &self.constraint_id) {
                // a token for recording the order first executions already exists
                Some(recording_token) => {
                    if let Err(e) = recording_token.push_timestamp(order_first_event_time) {
                        log::error!(
                            "tried to add event time {} to token {:?}, but an error occured while serializing payload as timestamp vector: {}",
                            order_first_event_time,
                            recording_token,
                            e
                        );
                    }
                }

                None => {
                    let mut recording_token = Token::new(
                        event.case_id.clone(),
                        self.constraint_id.clone(),
                        ConstraintType::Order,
                        event.timestamp_utc(),
                        state.config.node_id,
                        None,
                    );
                    if let Err(e) = recording_token.push_timestamp(order_first_event_time) {
                        log::error!(
                            "tried to add event time {} to token {:?}, but an error occured while serializing payload as timestamp vector: {}",
                            order_first_event_time,
                            recording_token,
                            e
                        );
                    }
                    store.produce_without_overwrite(recording_token);
                }
            };
        };
    }

    /// receive token from order second with timestamp of first execution of order second (multiple order seconds possible?)
    /// check check that execution was after order first event(s)
    /// set the received (pushed) token so that all future order first events can be evaluated as a violation
    async fn handle_message(&self, state: AppState, message: CCMessage) {
        log::info!(
            "received message for constraint {}; message={}",
            self.constraint_id,
            message
        );
        log::debug!(
            "received message for constraint {}; message={:?}",
            self.constraint_id,
            message
        );

        if message.message_type == CCMessageType::PushToken {
            let order_second_event_time = match message.timestamp {
                Some(event_time) => event_time,
                None => {
                    log::warn!(
                        "No timestamp was sent in the pushtoken message {} - using current time",
                        message
                    );
                    log::debug!("push token message: {:?}", message);
                    Utc::now()
                }
            };

            // check that token was contained in the message
            let message_token = match message.token {
                Some(token) => token,
                None => {
                    log::error!(
                        "pushtoken message did not contain a token! message: {}",
                        message
                    );
                    return;
                }
            };

            // lock token store for the whole method
            let mut token_store = state.token_store.lock().unwrap();

            // check if a blocking token already exists
            if !token_store.contains_in_token(&message.case_id, &self.constraint_id) {
                // no token exists - create one
                let new_blocking_token = Token::new(
                    message.case_id.clone(),
                    self.constraint_id.clone(),
                    ConstraintType::Order,
                    Some(order_second_event_time),
                    message_token.created_by,
                    None,
                );
                token_store.receive_without_overwrite(new_blocking_token);
            }

            // if its the first time receiving an order second event execution notification then we have to check the order first executions that have been recorded till now
            if let Some(recording_token) =
                token_store.get_out_cloned(&message.case_id, &self.constraint_id)
            {
                // do the thing with the whole recorded vector of event execution times
                if let Ok(order_first_executions) =
                    recording_token.get_payload_as_timestamp_vector()
                {
                    let mut result_store = state.cc_results.lock().unwrap();
                    let mut violation_happened = false;
                    for order_first_time in order_first_executions {
                        if order_first_time >= order_second_event_time {
                            violation_happened = true;
                            // produce negative result
                            let info = format!(
                                "order first event happened at {} before order second event that happened at {}",
                                order_first_time, order_second_event_time
                            );
                            result_store.update(
                                &message.case_id,
                                &self.constraint_id,
                                ConstraintResultType::Violated,
                                info,
                            );
                        }
                    }
                    if !violation_happened {
                        // produce positive result
                        let info = format!(
                            "all executions of event {} happened before the first execution of the order second event for constraint {} for case {}",
                            &self.event_id, &self.constraint_id, message.case_id
                        );
                        result_store.update(
                            &message.case_id,
                            &self.constraint_id,
                            ConstraintResultType::Satisfied,
                            info,
                        );
                    }
                    // delete the recording token as it is now used up
                    token_store.remove_out(&message.case_id, &self.constraint_id);

                    // TODO: replace with sending the result somewhere and delete result afterwards
                    log::info!(
                        "order constraint check result {}",
                        result_store
                            .get_constraint(&message.case_id, &self.constraint_id)
                            .unwrap()
                    );
                    log::debug!(
                        "order constraint check result {:?}",
                        result_store
                            .get_constraint(&message.case_id, &self.constraint_id)
                            .unwrap()
                    )
                } else {
                    log::error!(
                        "error trying to serialize the timestamp vector from token {}",
                        recording_token
                    );
                }
            }
        }
    }

    // remove the blocking token
    async fn handle_case_end(&self, case_id: &CaseId, state: AppState) {
        // TODO: grace period/revocable?
        let mut token_store = state.token_store.lock().unwrap();
        token_store.remove_in(case_id, &self.constraint_id);
    }
}

pub struct OrderSecondHandler {
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
        log::debug!(
            "received configured event for constraint {}; event={}",
            self.constraint_id,
            event
        );

        let order_second_event_time = event.timestamp_utc().unwrap_or(Utc::now());

        // check if this is the first time receiving such event for the case
        // keep the token store lock for that time, but release lock before notifying order first peers
        let notify_peers = {
            let mut notify = false;
            let mut token_store = state.token_store.lock().unwrap();
            if !token_store.contains_out_token(&event.case_id, &self.constraint_id) {
                // its the first time - create a token
                notify = true;

                let remembering_token = Token::new(
                    event.case_id.clone(),
                    self.constraint_id.clone(),
                    ConstraintType::Order,
                    Some(order_second_event_time),
                    state.config.node_id,
                    None,
                );
                token_store.produce_without_overwrite(remembering_token);
            }
            notify
        };

        // notify order first peers
        // happens only when the order second event is executed the first time because the blocking token is only created once
        if notify_peers {
            log::info!("notifying order first peers that order second event happened");
            log::debug!("order first peers: {:?}", self.order_first_peers);
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
    async fn handle_case_end(&self, case_id: &CaseId, state: AppState) {
        // TODO: grace period/revocable?
        let mut token_store = state.token_store.lock().unwrap();
        token_store.remove_out(case_id, &self.constraint_id);
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
        None,
    );
    // create message
    let message = PeerMessage::conformance(CCMessage::push_token(token));

    // send message
    log::info!("sending push token message to {}", peer_addr);
    send_to_peer(peer_addr, message, state.clone()).await;
}
