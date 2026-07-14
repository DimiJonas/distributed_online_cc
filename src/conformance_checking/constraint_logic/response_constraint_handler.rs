use std::net::SocketAddr;

use crate::impl_constraint_handler_fields;
use crate::network::tcp_router::send_to_conformance_peer;
/*
    a Response constraint defines that after Event/Activity A happened, Event/Activity B must also happen at some point after that

    implementation:
        node A observes event/activity A is executed
        node A produces a token in its token-out area that indicates for case X the event/activity A happened
        a node B that observes event/activity B for case X triggers the deletion of the token on node A
        after termination of case X node A checks if there is still a token in its token-out area

    configuration:
        on node A:
            [[constraints]]
            constraint_role = "ResponseProducer"
            constraint_id = "response_test"
            event_id = "response_producer_event"
        on node B:
            [[constraints]]
            constraint_role = "ResponseConsumer"
            constraint_id = "response_test"
            event_id = "response_consumer_event"
            producer_peers = ["127.0.0.1:8001"]

*/
use crate::conformance_checking::constraint_logic::constraint_handler_trait::ConstraintHandler;
use crate::conformance_checking::result_emitter::emit_result;
use crate::network::message_types::{CCMessage, CCMessageType, PeerMessage};
use crate::state::app_state::AppState;
use crate::types::{
    CaseId, ConstraintHandlerId, ConstraintId, ConstraintResultType, ConstraintType, EventTime,
    RequestId, Token, TracedEvent,
};
use async_trait::async_trait;
use chrono::Utc;

pub struct ResponseProducerHandler {
    pub handler_id: ConstraintHandlerId,
    pub constraint_id: ConstraintId,
    pub event_id: String,
}

#[async_trait]
impl ConstraintHandler for ResponseProducerHandler {
    impl_constraint_handler_fields!(ResponseProducerHandler);

    // create token that event happened
    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::info!(
            "response producer handler: received configured event for constraint {}; event={}",
            self.constraint_id,
            event
        );

        let producer_event_time = {
            if let Some(timestamp) = event.timestamp_utc() {
                timestamp
            } else {
                log::warn!(
                    "response producer handler: response producer event did not contain a timestamp - using current time as event time"
                );
                Utc::now()
            }
        };

        let producer_token = Token::new(
            event.case_id.clone(),
            self.constraint_id.clone(),
            ConstraintType::Response,
            Some(producer_event_time),
            state.config.node_id,
        );
        {
            let mut token_out = state.token_out.lock().unwrap();
            token_out.produce_with_overwrite(producer_token.clone(), self.handler_id);
        }

        log::info!(
            "response producer handler: produced token: {}",
            producer_token
        );
        log::debug!("token={:?}", producer_token);

        // produce result if case already ended
        let case_ended = {
            let case_state = state.case_state.lock().unwrap();
            case_state.is_ended(&event.case_id)
        };
        if case_ended {
            // produce negative result because token was produced after case end
            emit_result(
                &event.case_id,
                &self.constraint_id,
                ConstraintType::Response,
                ConstraintResultType::Violated,
                format!(
                    "producer token from {} for {}/{} was not consumed",
                    producer_event_time, event.case_id, self.constraint_id,
                ),
                state.clone(),
            )
            .await;
        }
    }

    // delete produced token of events that happened before the timestamp contained in the message
    async fn handle_message(
        &self,
        state: AppState,
        message: CCMessage,
        _request_id: Option<RequestId>,
    ) {
        log::info!(
            "response producer handler: received message for constraint {}; message={}",
            self.constraint_id,
            message
        );
        log::debug!("message={:?}", message);

        if message.message_type != CCMessageType::DeleteToken {
            log::warn!(
                "response producer handler: unexpected message type {:?} for constraint {}, ignoring",
                message.message_type,
                self.constraint_id
            );
            return;
        }

        let consumer_event_time = {
            if let Some(event_time) = message.timestamp {
                event_time
            } else {
                log::error!(
                    "response producer handler: delete token message did not contain a timestamp. not deleting any tokens. message={}",
                    message
                );
                return;
            }
        };

        if let Some(token) = {
            let mut token_out = state.token_out.lock().unwrap();
            token_out.remove_out_before(
                &message.case_id,
                &self.constraint_id,
                self.handler_id,
                consumer_event_time,
            )
        } {
            log::info!("response producer handler: removed token {}", token);
            log::debug!("token={:?}", token);
        }

        // produce result if case already ended
        let case_ended = {
            let case_state = state.case_state.lock().unwrap();
            case_state.is_ended(&message.case_id)
        };
        if case_ended {
            // produce positive result because token was consumed after case end
            emit_result(
                &message.case_id,
                &self.constraint_id,
                ConstraintType::Response,
                ConstraintResultType::Satisfied,
                format!(
                    "all producer tokens for {}/{} were consumed",
                    message.case_id, self.constraint_id,
                ),
                state.clone(),
            )
            .await;
        }
    }

    // check if token still exists and set constraint result accordingly
    async fn handle_case_end(&self, case_id: &CaseId, state: AppState) {
        log::info!(
            "response producer handler: received case end signal for constraint {}; case={}",
            self.constraint_id,
            case_id
        );

        // create constraint result
        let (constraint_result, info) = {
            let mut token_out = state.token_out.lock().unwrap();
            match token_out.remove_out(case_id, &self.constraint_id, self.handler_id) {
                Some(token) => (
                    ConstraintResultType::Violated,
                    format!(
                        "producer token from {} was not consumed",
                        token.event_time.unwrap_or(token.created_at),
                    ),
                ),
                None => (
                    ConstraintResultType::Satisfied,
                    "all producer tokens were consumed".to_string(),
                ),
            }
        };

        emit_result(
            case_id,
            &self.constraint_id,
            ConstraintType::Response,
            constraint_result,
            info,
            state.clone(),
        )
        .await;
    }

    async fn clean_up_state(&self, case_id: &CaseId, state: AppState) {
        log::info!(
            "response producer handler: clean up state for ({}/{})",
            case_id,
            self.constraint_id
        );

        // remove producer token
        {
            let mut token_out = state.token_out.lock().unwrap();
            token_out.remove_out(case_id, &self.constraint_id, self.handler_id);
        }
    }
}

pub struct ResponseConsumerHandler {
    pub handler_id: ConstraintHandlerId,
    pub constraint_id: ConstraintId,
    pub event_id: String,
    pub producer_peers: Vec<SocketAddr>,
}

#[async_trait]
impl ConstraintHandler for ResponseConsumerHandler {
    impl_constraint_handler_fields!(ResponseConsumerHandler);

    fn configured_peers(&self) -> Vec<SocketAddr> {
        self.producer_peers.clone()
    }

    // trigger token deletion on all configured response producers
    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::info!(
            "response consumer handler: received configured event for constraint {}; event={}",
            self.constraint_id,
            event
        );

        let event_timestamp = {
            if let Some(timestamp) = event.timestamp_utc() {
                timestamp
            } else {
                log::warn!(
                    "response consumer handler: response consumer event did not contain a timestamp - using current time as event time"
                );
                Utc::now()
            }
        };

        log::info!(
            "response consumer handler: triggering token deletion on response producer peers"
        );
        for &peer_addr in self.producer_peers.iter() {
            send_delete_token(
                peer_addr,
                event.case_id.clone(),
                self.constraint_id.clone(),
                event_timestamp,
                state.clone(),
            )
            .await;
        }
    }
}

// helper method to create and send a message to delete the response consumer token
async fn send_delete_token(
    peer_addr: SocketAddr,
    trace_id: CaseId,
    constraint_id: ConstraintId,
    event_timestamp: EventTime,
    state: AppState,
) {
    // create message
    let message = PeerMessage::conformance(CCMessage::delete_token(
        trace_id,
        constraint_id,
        event_timestamp,
    ));

    // send message
    log::info!(
        "response consumer handler: sending delete token message to {}",
        peer_addr
    );

    if let Err(e) = send_to_conformance_peer(peer_addr, message, state.clone()).await {
        log::error!(
            "response consumer handler: error sending cc trigger message to {}: {}",
            peer_addr,
            e
        );
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use crate::{
        config::node_config::ConstraintConfig,
        conformance_checking::constraint_logic::{
            constraint_handler_trait::ConstraintHandler,
            response_constraint_handler::{ResponseConsumerHandler, ResponseProducerHandler},
        },
        network::{
            message_types::{CCMessage, PeerMessagePayload},
            peer_manager_task::PeerManagerMessage,
        },
        state::app_state::AppState,
        types::ConstraintHandlerId,
        util::test_tools::{
            addr, make_config_with_constraints_and_port, make_state_from_config, peer_addr,
            register_manager,
        },
    };

    use crate::util::test_tools::make_traced_event;

    fn make_producer_state(port: u16) -> AppState {
        let config = make_config_with_constraints_and_port(
            vec![ConstraintConfig::ResponseProducer {
                constraint_id: "rp".to_string(),
                event_id: "produce".to_string(),
            }],
            port,
        );
        make_state_from_config(config)
    }

    // -- ResponseProducerHandler --

    /// handle_event creates a token in token_out.
    #[tokio::test]
    async fn producer_handle_event_creates_token() {
        let state = make_producer_state(20000);
        {
            let mut store = state.cc_results.lock().unwrap();
            store.register_case("c1".to_string());
        }

        let handler = ResponseProducerHandler {
            handler_id: ConstraintHandlerId::ResponseProducer,
            constraint_id: "rp".to_string(),
            event_id: "produce".to_string(),
        };
        handler
            .handle_event(&make_traced_event("c1", "produce"), state.clone())
            .await;

        assert!(state.token_out.lock().unwrap().has_open_token(
            &"c1".to_string(),
            &"rp".to_string(),
            handler.handler_id()
        ));
    }

    /// handle_message with DeleteToken removes the token.
    #[tokio::test]
    async fn producer_handle_message_delete_token_removes_token() {
        let state = make_producer_state(20010);
        {
            let mut store = state.cc_results.lock().unwrap();
            store.register_case("c2".to_string());
        }

        let handler = ResponseProducerHandler {
            handler_id: ConstraintHandlerId::ResponseProducer,
            constraint_id: "rp".to_string(),
            event_id: "produce".to_string(),
        };

        handler
            .handle_event(&make_traced_event("c2", "produce"), state.clone())
            .await;
        assert!(state.token_out.lock().unwrap().has_open_token(
            &"c2".to_string(),
            &"rp".to_string(),
            handler.handler_id()
        ));

        let delete_msg = CCMessage::delete_token("c2".to_string(), "rp".to_string(), Utc::now());
        handler
            .handle_message(state.clone(), delete_msg, None)
            .await;

        assert!(!state.token_out.lock().unwrap().has_open_token(
            &"c2".to_string(),
            &"rp".to_string(),
            handler.handler_id()
        ));
    }

    /// handle_case_end with an open token → result is Violated.
    #[tokio::test]
    async fn producer_handle_case_end_violated_when_token_exists() {
        let state = make_producer_state(20020);
        {
            let mut store = state.cc_results.lock().unwrap();
            store.register_case("c3".to_string());
        }

        let handler = ResponseProducerHandler {
            handler_id: ConstraintHandlerId::ResponseProducer,
            constraint_id: "rp".to_string(),
            event_id: "produce".to_string(),
        };

        handler
            .handle_event(&make_traced_event("c3", "produce"), state.clone())
            .await;
        handler
            .handle_case_end(&"c3".to_string(), state.clone())
            .await;

        let store = state.cc_results.lock().unwrap();
        let result = store
            .get_constraint(&"c3".to_string(), &"rp".to_string())
            .unwrap();
        assert!(matches!(
            result.constraint_result,
            crate::types::ConstraintResultType::Violated
        ));
    }

    /// handle_case_end without a token → result is Satisfied.
    #[tokio::test]
    async fn producer_handle_case_end_satisfied_when_no_token() {
        let state = make_producer_state(20030);
        {
            let mut store = state.cc_results.lock().unwrap();
            store.register_case("c4".to_string());
        }

        let handler = ResponseProducerHandler {
            handler_id: ConstraintHandlerId::ResponseProducer,
            constraint_id: "rp".to_string(),
            event_id: "produce".to_string(),
        };

        // No produce event → no token.
        handler
            .handle_case_end(&"c4".to_string(), state.clone())
            .await;

        let store = state.cc_results.lock().unwrap();
        let result = store
            .get_constraint(&"c4".to_string(), &"rp".to_string())
            .unwrap();
        assert!(matches!(
            result.constraint_result,
            crate::types::ConstraintResultType::Satisfied
        ));
    }

    /// A non-DeleteToken message does NOT remove the token.
    #[tokio::test]
    async fn producer_handle_message_non_delete_type_leaves_token() {
        let state = make_producer_state(20040);
        {
            let mut store = state.cc_results.lock().unwrap();
            store.register_case("c5".to_string());
        }

        let handler = ResponseProducerHandler {
            handler_id: ConstraintHandlerId::ResponseProducer,
            constraint_id: "rp".to_string(),
            event_id: "produce".to_string(),
        };

        handler
            .handle_event(&make_traced_event("c5", "produce"), state.clone())
            .await;

        let trigger = CCMessage::trigger_check("c5".to_string(), "rp".to_string(), Utc::now());
        handler.handle_message(state.clone(), trigger, None).await;

        assert!(state.token_out.lock().unwrap().has_open_token(
            &"c5".to_string(),
            &"rp".to_string(),
            handler.handler_id()
        ));
    }

    // -- Response scenarios (producer-side evaluator) --
    //
    // The producer side is where the Response result is emitted: a consumer
    // event turns into a DeleteToken sent to the producer peer, and the
    // producer decides Satisfied/Violated at case end based on whether an
    // unconsumed token remains.

    /// both ran, producer followed by consumer → Satisfied at case end.
    /// produce@t10, consume(delete)@t20 removes the token, case end → Satisfied.
    #[tokio::test]
    async fn producer_followed_by_consumer_is_satisfied() {
        use crate::util::test_tools::make_traced_event_at;
        let state = make_producer_state(20080);
        {
            let mut store = state.cc_results.lock().unwrap();
            store.register_case("c8".to_string());
        }
        let handler = ResponseProducerHandler {
            handler_id: ConstraintHandlerId::ResponseProducer,
            constraint_id: "rp".to_string(),
            event_id: "produce".to_string(),
        };

        handler
            .handle_event(&make_traced_event_at("c8", "produce", 10), state.clone())
            .await;
        // consumer event time is strictly after the producer token time.
        let consumer_ts = chrono::TimeZone::with_ymd_and_hms(&Utc, 2024, 1, 1, 0, 0, 20).unwrap();
        let delete = CCMessage::delete_token("c8".to_string(), "rp".to_string(), consumer_ts);
        handler.handle_message(state.clone(), delete, None).await;
        handler
            .handle_case_end(&"c8".to_string(), state.clone())
            .await;

        let store = state.cc_results.lock().unwrap();
        let result = store
            .get_constraint(&"c8".to_string(), &"rp".to_string())
            .unwrap();
        assert!(
            matches!(
                result.constraint_result,
                crate::types::ConstraintResultType::Satisfied
            ),
            "producer followed by consumer should be Satisfied"
        );
    }

    /// both ran, but consumer happened BEFORE the producer → the delete does
    /// not match (token time not earlier than consumer time), token remains,
    /// case end → Violated. Encodes "a producer must be followed by a consumer".
    #[tokio::test]
    async fn consumer_before_producer_is_violated() {
        use crate::util::test_tools::make_traced_event_at;
        let state = make_producer_state(20090);
        {
            let mut store = state.cc_results.lock().unwrap();
            store.register_case("c9".to_string());
        }
        let handler = ResponseProducerHandler {
            handler_id: ConstraintHandlerId::ResponseProducer,
            constraint_id: "rp".to_string(),
            event_id: "produce".to_string(),
        };

        // produce at t20, consumer delete carries an earlier time t10.
        handler
            .handle_event(&make_traced_event_at("c9", "produce", 20), state.clone())
            .await;
        let consumer_ts = chrono::TimeZone::with_ymd_and_hms(&Utc, 2024, 1, 1, 0, 0, 10).unwrap();
        let delete = CCMessage::delete_token("c9".to_string(), "rp".to_string(), consumer_ts);
        handler.handle_message(state.clone(), delete, None).await;
        handler
            .handle_case_end(&"c9".to_string(), state.clone())
            .await;

        let store = state.cc_results.lock().unwrap();
        let result = store
            .get_constraint(&"c9".to_string(), &"rp".to_string())
            .unwrap();
        assert!(
            matches!(
                result.constraint_result,
                crate::types::ConstraintResultType::Violated
            ),
            "a consumer occurring before the producer must not satisfy the response"
        );
    }

    /// multiple producers, one later consumer → Satisfied. Two produce events
    /// (the second overwrites the open token); one delete after both consumes
    /// the outstanding token; case end → Satisfied.
    #[tokio::test]
    async fn multiple_producers_one_consumer_is_satisfied() {
        use crate::util::test_tools::make_traced_event_at;
        let state = make_producer_state(20100);
        {
            let mut store = state.cc_results.lock().unwrap();
            store.register_case("c10".to_string());
        }
        let handler = ResponseProducerHandler {
            handler_id: ConstraintHandlerId::ResponseProducer,
            constraint_id: "rp".to_string(),
            event_id: "produce".to_string(),
        };

        handler
            .handle_event(&make_traced_event_at("c10", "produce", 5), state.clone())
            .await;
        handler
            .handle_event(&make_traced_event_at("c10", "produce", 15), state.clone())
            .await;
        let consumer_ts = chrono::TimeZone::with_ymd_and_hms(&Utc, 2024, 1, 1, 0, 0, 30).unwrap();
        let delete = CCMessage::delete_token("c10".to_string(), "rp".to_string(), consumer_ts);
        handler.handle_message(state.clone(), delete, None).await;
        handler
            .handle_case_end(&"c10".to_string(), state.clone())
            .await;

        let store = state.cc_results.lock().unwrap();
        let result = store
            .get_constraint(&"c10".to_string(), &"rp".to_string())
            .unwrap();
        assert!(
            matches!(
                result.constraint_result,
                crate::types::ConstraintResultType::Satisfied
            ),
            "multiple producers consumed by one later consumer should be Satisfied"
        );
    }

    /// only the consumer ran (no producer token ever created) → at case end the
    /// producer side reports Satisfied. The stray delete finds no token.
    #[tokio::test]
    async fn only_consumer_is_satisfied_at_case_end() {
        let state = make_producer_state(20110);
        {
            let mut store = state.cc_results.lock().unwrap();
            store.register_case("c11".to_string());
        }
        let handler = ResponseProducerHandler {
            handler_id: ConstraintHandlerId::ResponseProducer,
            constraint_id: "rp".to_string(),
            event_id: "produce".to_string(),
        };

        let consumer_ts = chrono::TimeZone::with_ymd_and_hms(&Utc, 2024, 1, 1, 0, 0, 10).unwrap();
        let delete = CCMessage::delete_token("c11".to_string(), "rp".to_string(), consumer_ts);
        handler.handle_message(state.clone(), delete, None).await;
        handler
            .handle_case_end(&"c11".to_string(), state.clone())
            .await;

        let store = state.cc_results.lock().unwrap();
        let result = store
            .get_constraint(&"c11".to_string(), &"rp".to_string())
            .unwrap();
        assert!(
            matches!(
                result.constraint_result,
                crate::types::ConstraintResultType::Satisfied
            ),
            "only a consumer (no producer) should be Satisfied at case end"
        );
    }

    // -- ResponseConsumerHandler --

    /// handle_event sends a DeleteToken conformance message to each producer peer.
    #[tokio::test]
    async fn consumer_handle_event_sends_delete_token_to_producers() {
        let producer_addr = peer_addr(20051);
        let config = make_config_with_constraints_and_port(
            vec![ConstraintConfig::ResponseConsumer {
                constraint_id: "rp".to_string(),
                event_id: "consume".to_string(),
                producer_peers: vec![producer_addr.address],
            }],
            20050,
        );
        let (tx, _rx) = tokio::sync::mpsc::channel(16);
        let (tx2, _rx2) = tokio::sync::mpsc::channel(16);
        let (tx3, _rx3) = tokio::sync::mpsc::channel(16);
        let (tx4, _rx4) = tokio::sync::mpsc::channel(16);
        let (tx5, _rx5) = tokio::sync::mpsc::channel(16);
        let state = AppState::new(config, tx, tx2, tx3, tx4, tx5);

        let mut mgr_rx = register_manager(&state, producer_addr);

        let handler = ResponseConsumerHandler {
            handler_id: ConstraintHandlerId::ResponseProducer,
            constraint_id: "rp".to_string(),
            event_id: "consume".to_string(),
            producer_peers: vec![producer_addr.address],
        };

        handler
            .handle_event(&make_traced_event("c6", "consume"), state)
            .await;

        let received = tokio::time::timeout(std::time::Duration::from_millis(200), mgr_rx.recv())
            .await
            .expect("timed out")
            .expect("channel closed");

        if let PeerManagerMessage::Outbound(boxed) = received {
            assert!(
                matches!(boxed.payload, PeerMessagePayload::Conformance(_)),
                "expected a Conformance payload"
            );
        } else {
            panic!("expected Outbound message");
        }
    }

    /// When multiple producer peers are configured, a message is sent to each.
    #[tokio::test]
    async fn consumer_sends_to_all_producer_peers() {
        let p1 = peer_addr(20060);
        let p2 = peer_addr(20061);

        let config = make_config_with_constraints_and_port(
            vec![ConstraintConfig::ResponseConsumer {
                constraint_id: "rp".to_string(),
                event_id: "consume".to_string(),
                producer_peers: vec![p1.address, p2.address],
            }],
            20062,
        );
        let (tx, _rx) = tokio::sync::mpsc::channel(16);
        let (tx2, _rx2) = tokio::sync::mpsc::channel(16);
        let (tx3, _rx3) = tokio::sync::mpsc::channel(16);
        let (tx4, _rx4) = tokio::sync::mpsc::channel(16);
        let (tx5, _rx5) = tokio::sync::mpsc::channel(16);
        let state = AppState::new(config, tx, tx2, tx3, tx4, tx5);

        let mut rx1 = register_manager(&state, p1);
        let mut rx2 = register_manager(&state, p2);

        let handler = ResponseConsumerHandler {
            handler_id: ConstraintHandlerId::ResponseProducer,
            constraint_id: "rp".to_string(),
            event_id: "consume".to_string(),
            producer_peers: vec![p1.address, p2.address],
        };

        handler
            .handle_event(&make_traced_event("c7", "consume"), state)
            .await;

        let timeout = std::time::Duration::from_millis(200);
        tokio::time::timeout(timeout, rx1.recv())
            .await
            .expect("p1 timed out")
            .unwrap();
        tokio::time::timeout(timeout, rx2.recv())
            .await
            .expect("p2 timed out")
            .unwrap();
    }

    /// configured_peers returns the list of producer peers.
    #[test]
    fn consumer_configured_peers() {
        let peers = vec![addr(20070), addr(20071)];
        let handler = ResponseConsumerHandler {
            handler_id: ConstraintHandlerId::ResponseProducer,
            constraint_id: "rp".to_string(),
            event_id: "consume".to_string(),
            producer_peers: peers.clone(),
        };
        assert_eq!(handler.configured_peers(), peers);
    }
}
