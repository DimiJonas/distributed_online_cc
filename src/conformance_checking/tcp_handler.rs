/*
    note: tcp handlers have to use the outbound router queue to send messages back to other peers using tcp
    this is needed to break a cycle that would emerge when tcp handlers would call tcp channel methods since tcp channels also call the tcp handlers when receiving a message
*/

use std::net::SocketAddr;

use crate::network::message_types::{
    CCMessage, CEGossipMessage, CliTestMessage, MessageKind, OverlayMessage, PeerMessage,
};
use crate::network::tcp_router::{send_to_conformance_peer, send_to_overlay_peer};
use crate::state::app_state::AppState;
use crate::types::{MessageId, RequestId};

pub async fn handle_cli_message(
    msg: CliTestMessage,
    state: AppState,
    msg_id: MessageId,
    kind: MessageKind,
    request_id: Option<RequestId>,
) {
    log::debug!(
        "handle_cli_message: received cli message id={} msg={}",
        msg_id,
        msg
    );

    match kind {
        MessageKind::Request => {
            log::info!(
                "handle_cli_message: received request from peer {} saying: \"{}\"",
                msg.sender_addr,
                msg.message
            );

            let request_id = match request_id {
                Some(id) => id,
                None => {
                    log::error!(
                        "handle_cli_message: request did not contain a request id. cannot create a response. message={}",
                        msg
                    );
                    return;
                }
            };
            let dest_peer = msg.sender_addr;

            // send response
            let response = PeerMessage::cli_res(
                request_id,
                CliTestMessage {
                    message: format!("ACK '{}'", msg.message),
                    sender_addr: state.config.tcp_listen_address,
                },
            );

            match send_to_conformance_peer(dest_peer, response, state.clone()).await {
                Ok(()) => log::info!("handle_cli_message: sent response to {}", dest_peer),
                Err(e) => log::error!(
                    "handle_cli_message: error sending response to peer {}: {}",
                    dest_peer,
                    e
                ),
            }
        }
        MessageKind::Response => {
            // do nothing - responses are already handled in the tcp channel instead of dispatching it somewhere
        }
        MessageKind::FireAndForget => {
            // do nothing - there are no fire and forget messages yet for cli
        }
    }
}

pub async fn handle_cc_message(
    msg: CCMessage,
    state: AppState,
    msg_id: MessageId,
    request_id: Option<RequestId>,
) {
    log::debug!(
        "handle_cc_message: handling conformance message with id={} msg={}",
        &msg_id,
        msg
    );

    // is the case already closed? -> drop message
    // is it a new case? -> register it in case state and result store
    {
        let mut case_state = state.case_state.lock().unwrap();
        if case_state.is_closed(&msg.case_id) {
            log::warn!(
                "the case of message {} is already closed. dropping message",
                msg
            );
            return;
        }
        if !case_state.has_case(&msg.case_id) {
            log::info!(
                "event queue processor: new case detected; id={}",
                &msg.case_id
            );
            case_state.open_case(msg.case_id.clone());
        }
    }
    {
        let mut result_store = state.cc_results.lock().unwrap();
        if !result_store.has_case(&msg.case_id) {
            result_store.register_case(msg.case_id.clone());
        }
    }

    let constraint_handler = state.handlers.get_by_constraint_id(&msg.constraint_id);
    if constraint_handler.is_empty() {
        log::warn!(
            "handle_cc_message: the received cc message did not match any configured constraints: message={}",
            msg
        );
    } else {
        // dispatch case to the handlers
        for handler in constraint_handler {
            handler
                .handle_message(state.clone(), msg.clone(), request_id)
                .await;
        }
    }
}

pub async fn handle_ce_gossip_message(msg: CEGossipMessage, state: AppState, msg_id: MessageId) {
    log::debug!(
        "handle_ce_gossip_message: handling case end gossip message message with id={} msg={}",
        &msg_id,
        msg
    );

    // pass message to case end gossip manager
    match state.gossip_msg_q.send(msg).await {
        Ok(()) => {}
        Err(e) => {
            log::error!(
                "handle_ce_gossip_message: unable to pass case end gossip message to case end gossip manager: {}",
                e
            );
        }
    }
}

pub async fn handle_overlay_message(
    msg: OverlayMessage,
    state: AppState,
    msg_id: MessageId,
    kind: MessageKind,
    request_id: Option<RequestId>,
) {
    log::debug!(
        "handle_overlay_message: handling overlay message with id={} msg={}",
        &msg_id,
        msg
    );

    if kind != MessageKind::Request {
        log::error!(
            "handle_overlay_message: received wrong message kind {:?}, expected {:?}. ignoring message",
            kind,
            MessageKind::Request
        );
        return;
    }

    // received a shuffle request
    log::info!(
        "cyclon overlay: received shuffle request from {} with request id {}",
        msg.sender_address,
        request_id.unwrap_or_default()
    );
    log::debug!(
        "cyclon overlay: subset received from P: {:?}",
        msg.peer_batch
    );

    // pick random subset from peer cache
    let subset = {
        let mut peer_cache = state.peer_cache.lock().unwrap();
        let subset =
            peer_cache.select_random_subset(state.config.overlay.shuffle_length, &mut rand::rng());
        let sent: Vec<SocketAddr> = subset.keys().copied().collect();

        // update peer cache with received cache entries
        peer_cache.update(
            state.config.overlay.listen_address,
            &sent,
            msg.peer_batch,
            state.config.overlay.max_peer_cache_size,
        );
        subset
    };

    let request_id = match request_id {
        Some(id) => id,
        None => {
            log::error!(
                "handle_overlay_message: received request without request id. cannot respond to it"
            );
            return;
        }
    };

    log::info!(
        "cyclon overlay: answering request from {} with subset of size {}",
        msg.sender_address,
        subset.len()
    );
    log::debug!("cyclon overlay: response subset: {:?}", subset);

    // build response
    let response = PeerMessage::overlay_res(
        request_id,
        OverlayMessage::new(state.config.overlay.listen_address, subset),
    );

    // respond to request
    if let Err(e) = send_to_overlay_peer(msg.sender_address, response, state.clone()).await {
        log::error!(
            "cyclon overlay: error responding to shuffle request from {}: {}",
            msg.sender_address,
            e
        );
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        config::node_config::ConstraintConfig,
        conformance_checking::tcp_handler::{handle_cc_message, handle_cli_message},
        network::{
            message_types::{CCMessage, CliTestMessage, MessageKind, PeerMessagePayload},
            peer_manager_task::PeerManagerMessage,
        },
        state::app_state::AppState,
        types::{ConstraintHandlerId, MessageId, RequestId},
        util::test_tools::{
            addr, make_config_with_constraints_and_port, make_state, peer_addr, register_manager,
        },
    };
    use chrono::Utc;
    use uuid::Uuid;

    // ----------------------------------------------------------------
    // handle_cli_message — Request
    // ----------------------------------------------------------------

    /// A Request message sends an ACK response back to the sender's peer manager.
    #[tokio::test]
    async fn cli_request_sends_ack_to_peer_manager() {
        let state = make_state(21000);
        let sender_addr = peer_addr(21001);
        let mut mgr_rx = register_manager(&state, sender_addr);
        let request_id = Uuid::new_v4();

        let msg = CliTestMessage {
            message: "hello".to_string(),
            sender_addr: sender_addr.address,
        };
        handle_cli_message(
            msg,
            state.clone(),
            MessageId::new_v4(),
            MessageKind::Request,
            Some(request_id),
        )
        .await;

        let received = tokio::time::timeout(std::time::Duration::from_millis(200), mgr_rx.recv())
            .await
            .expect("timed out waiting for ACK")
            .expect("manager channel closed");

        match received {
            PeerManagerMessage::Outbound(boxed) => match boxed.payload {
                PeerMessagePayload::Cli(m) => {
                    assert!(
                        m.message.starts_with("ACK"),
                        "response should be an ACK, got: {}",
                        m.message
                    );
                }
                other => panic!("expected Cli payload, got {:?}", other),
            },
            other => panic!("expected Outbound, got {:?}", other),
        }
    }

    /// A Request without a request_id logs an error and does not send anything.
    #[tokio::test]
    async fn cli_request_missing_request_id_does_not_send() {
        let state = make_state(21010);
        let sender_addr = peer_addr(21011);
        let mut mgr_rx = register_manager(&state, sender_addr);

        let msg = CliTestMessage {
            message: "hello".to_string(),
            sender_addr: sender_addr.address,
        };
        handle_cli_message(
            msg,
            state.clone(),
            MessageId::new_v4(),
            MessageKind::Request,
            None,
        )
        .await;

        let result =
            tokio::time::timeout(std::time::Duration::from_millis(100), mgr_rx.recv()).await;
        assert!(
            result.is_err(),
            "no message should be sent when request_id is missing"
        );
    }

    /// A Request with no registered peer manager does not panic.
    #[tokio::test]
    async fn cli_request_no_manager_does_not_panic() {
        let state = make_state(21020);
        let msg = CliTestMessage {
            message: "hello".to_string(),
            sender_addr: addr(21021),
        };
        handle_cli_message(
            msg,
            state,
            MessageId::new_v4(),
            MessageKind::Request,
            Some(Uuid::new_v4()),
        )
        .await;
        // reaching here without panic is the assertion
    }

    /// The ACK response echoes back the same request_id.
    #[tokio::test]
    async fn cli_request_response_contains_original_request_id() {
        let state = make_state(21030);
        let sender_addr = peer_addr(21031);
        let mut mgr_rx = register_manager(&state, sender_addr);
        let request_id = Uuid::new_v4();

        let msg = CliTestMessage {
            message: "ping".to_string(),
            sender_addr: sender_addr.address,
        };
        handle_cli_message(
            msg,
            state.clone(),
            MessageId::new_v4(),
            MessageKind::Request,
            Some(request_id),
        )
        .await;

        let received = tokio::time::timeout(std::time::Duration::from_millis(200), mgr_rx.recv())
            .await
            .expect("timed out")
            .unwrap();

        if let PeerManagerMessage::Outbound(boxed) = received {
            assert_eq!(
                boxed.request_id,
                Some(request_id),
                "response must echo the original request_id"
            );
        } else {
            panic!("expected Outbound");
        }
    }

    // ----------------------------------------------------------------
    // handle_cli_message — Response
    // ----------------------------------------------------------------

    /// A Response message is a no-op in the handler (already resolved in dispatch_message).
    #[tokio::test]
    async fn cli_response_is_noop_in_handler() {
        let state = make_state(21040);
        let sender_addr = peer_addr(21041);
        let mut mgr_rx = register_manager(&state, sender_addr);

        let msg = CliTestMessage {
            message: "ACK 'hello'".to_string(),
            sender_addr: sender_addr.address,
        };
        handle_cli_message(
            msg,
            state.clone(),
            MessageId::new_v4(),
            MessageKind::Response,
            Some(Uuid::new_v4()),
        )
        .await;

        let result =
            tokio::time::timeout(std::time::Duration::from_millis(100), mgr_rx.recv()).await;
        assert!(
            result.is_err(),
            "Response kind should produce no outbound message from handler"
        );
    }

    // ----------------------------------------------------------------
    // handle_cli_message — FireAndForget
    // ----------------------------------------------------------------

    /// A FireAndForget message is a no-op in the handler.
    #[tokio::test]
    async fn cli_fire_and_forget_is_noop_in_handler() {
        let state = make_state(21050);
        let sender_addr = peer_addr(21051);
        let mut mgr_rx = register_manager(&state, sender_addr);

        let msg = CliTestMessage {
            message: "broadcast".to_string(),
            sender_addr: sender_addr.address,
        };
        handle_cli_message(
            msg,
            state.clone(),
            MessageId::new_v4(),
            MessageKind::FireAndForget,
            None,
        )
        .await;

        let result =
            tokio::time::timeout(std::time::Duration::from_millis(100), mgr_rx.recv()).await;
        assert!(
            result.is_err(),
            "FireAndForget should produce no outbound message from handler"
        );
    }

    // ----------------------------------------------------------------
    // handle_cc_message
    // ----------------------------------------------------------------

    /// A CCMessage for a new case registers it in the result store.
    #[tokio::test]
    async fn cc_message_registers_new_case() {
        let state = make_state(21060);
        let msg = CCMessage::delete_token("new-case".to_string(), "rp".to_string(), Utc::now());
        handle_cc_message(
            msg,
            state.clone(),
            MessageId::new_v4(),
            Some(RequestId::new_v4()),
        )
        .await;

        let store = state.cc_results.lock().unwrap();
        assert!(
            store.has_case(&"new-case".to_string()),
            "case should be registered on first message"
        );
    }

    /// A CCMessage is dispatched to the correct handler and applies its effect.
    #[tokio::test]
    async fn cc_message_dispatched_to_correct_handler() {
        let config = make_config_with_constraints_and_port(
            vec![ConstraintConfig::ResponseProducer {
                constraint_id: "rp".to_string(),
                event_id: "e".to_string(),
            }],
            21070,
        );
        let (tx, _rx) = tokio::sync::mpsc::channel(16);
        let (tx2, _rx2) = tokio::sync::mpsc::channel(16);
        let (tx3, _rx3) = tokio::sync::mpsc::channel(16);
        let (tx4, _rx4) = tokio::sync::mpsc::channel(16);
        let (tx5, _rx5) = tokio::sync::mpsc::channel(16);
        let state = AppState::new(config, tx, tx2, tx3, tx4, tx5);

        {
            let mut store = state.cc_results.lock().unwrap();
            store.register_case("c1".to_string());
        }
        {
            use crate::types::{ConstraintType, Token};
            let token = Token::new(
                "c1".to_string(),
                "rp".to_string(),
                ConstraintType::Response,
                Some(Utc::now()),
                1,
            );
            state
                .token_out
                .lock()
                .unwrap()
                .produce_with_overwrite(token, ConstraintHandlerId::ResponseProducer);
        }

        let delete_msg = CCMessage::delete_token("c1".to_string(), "rp".to_string(), Utc::now());
        handle_cc_message(
            delete_msg,
            state.clone(),
            MessageId::new_v4(),
            Some(RequestId::new_v4()),
        )
        .await;

        assert!(
            !state.token_out.lock().unwrap().has_open_token(
                &"c1".to_string(),
                &"rp".to_string(),
                ConstraintHandlerId::ResponseProducer
            ),
            "token should be deleted after handling"
        );
    }

    /// A CCMessage for an unknown constraint_id logs a warning but does not panic.
    #[tokio::test]
    async fn cc_message_unknown_constraint_does_not_panic() {
        let state = make_state(21080);
        let msg = CCMessage::delete_token("c".to_string(), "ghost".to_string(), Utc::now());
        handle_cc_message(msg, state, MessageId::new_v4(), Some(RequestId::new_v4())).await;
    }
}
