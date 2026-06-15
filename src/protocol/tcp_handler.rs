/*
    note: tcp handlers have to use the outbound router queue to send messages back to other peers using tcp
    this is needed to break a cycle that would emerge when tcp handlers would call tcp channel methods since tcp channels also call the tcp handlers when receiving a message
*/

use crate::network::message_types::{
    CCMessage, CliTestMessage, MessageKind, OverlayMessage, PeerMessage,
    TerminationDetectionMessage,
};
use crate::network::tcp_router::send_to_peer;
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
        "tcp handler: received cli message id={} msg={}",
        msg_id,
        msg
    );

    match kind {
        MessageKind::Request => {
            log::info!(
                "received request from peer {} saying: \"{}\"",
                msg.sender_addr,
                msg.message
            );

            let request_id = match request_id {
                Some(id) => id,
                None => {
                    log::error!(
                        "request did not contain a request id. cannot create a response. message={}",
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

            match send_to_peer(dest_peer, response, state.clone()).await {
                Ok(()) => log::info!("sent response to {}", dest_peer),
                Err(e) => log::error!("error sending response to peer {}: {}", dest_peer, e),
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

pub async fn handle_cc_message(msg: CCMessage, state: AppState, msg_id: MessageId) {
    log::debug!(
        "tcp handler: handling conformance message with id={} msg={}",
        &msg_id,
        msg
    );

    // is it a new case? -> register it in result store
    {
        let mut result_store = state.cc_results.lock().unwrap();
        if !result_store.has_case(&msg.case_id) {
            log::info!("tcp handler: new case detected; id={}", &msg.case_id);
            let constraints = state.config.build_constraint_list();
            result_store.register_case(msg.case_id.clone(), &constraints);
        }
    }

    let constraint_handler = state.handlers.get_by_constraint_id(&msg.constraint_id);
    if constraint_handler.is_empty() {
        log::warn!(
            "tcp handler: the received cc message did not match any configured constraints: message={}",
            msg
        );
    } else {
        // dispatch case to the handlers
        for handler in constraint_handler {
            handler.handle_message(state.clone(), msg.clone()).await;
        }
    }
}

pub async fn handle_td_message(
    msg: TerminationDetectionMessage,
    _state: AppState,
    msg_id: MessageId,
    _kind: MessageKind,
    _request_id: Option<RequestId>,
) {
    log::debug!(
        "tcp handler: handling termination detection message message with id={} msg={}",
        &msg_id,
        msg
    );
    // TODO
}

pub async fn handle_overlay_message(
    msg: OverlayMessage,
    _state: AppState,
    msg_id: MessageId,
    _kind: MessageKind,
    _request_id: Option<RequestId>,
) {
    log::debug!(
        "tcp handler: handling overlay message with id={} msg={}",
        &msg_id,
        msg
    );
    // TODO
}

#[cfg(test)]
mod tests {
    use crate::{
        config::node_config::ConstraintConfig,
        network::{
            message_types::{
                CCMessage, CliTestMessage, MessageKind, OverlayMessage, PeerMessagePayload,
                TerminationDetectionMessage,
            },
            peer_manager_task::PeerManagerMessage,
        },
        protocol::tcp_handler::{
            handle_cc_message, handle_cli_message, handle_overlay_message, handle_td_message,
        },
        state::app_state::AppState,
        types::{ConstraintHandlerId, ConstraintType, MessageId},
        util::test_tools::{
            addr, make_config_with_constraints_and_port, make_state, register_manager,
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
        let sender_addr = addr(21001);
        let mut mgr_rx = register_manager(&state, sender_addr);
        let request_id = Uuid::new_v4();

        let msg = CliTestMessage {
            message: "hello".to_string(),
            sender_addr,
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
        let sender_addr = addr(21011);
        let mut mgr_rx = register_manager(&state, sender_addr);

        let msg = CliTestMessage {
            message: "hello".to_string(),
            sender_addr,
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
        let sender_addr = addr(21031);
        let mut mgr_rx = register_manager(&state, sender_addr);
        let request_id = Uuid::new_v4();

        let msg = CliTestMessage {
            message: "ping".to_string(),
            sender_addr,
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
        let sender_addr = addr(21041);
        let mut mgr_rx = register_manager(&state, sender_addr);

        let msg = CliTestMessage {
            message: "ACK 'hello'".to_string(),
            sender_addr,
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
        let sender_addr = addr(21051);
        let mut mgr_rx = register_manager(&state, sender_addr);

        let msg = CliTestMessage {
            message: "broadcast".to_string(),
            sender_addr,
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
        handle_cc_message(msg, state.clone(), MessageId::new_v4()).await;

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
        let state = AppState::new(config, tx, tx2);

        {
            let mut store = state.cc_results.lock().unwrap();
            store.register_case(
                "c1".to_string(),
                &[("rp".to_string(), ConstraintType::Response)],
            );
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
        handle_cc_message(delete_msg, state.clone(), Uuid::new_v4()).await;

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
        handle_cc_message(msg, state, Uuid::new_v4()).await;
    }

    // ----------------------------------------------------------------
    // stub handlers
    // ----------------------------------------------------------------

    /// Stub handlers must not panic regardless of kind.
    #[tokio::test]
    async fn td_and_overlay_handlers_do_not_panic() {
        let state = make_state(21090);
        handle_td_message(
            TerminationDetectionMessage {},
            state.clone(),
            Uuid::new_v4(),
            MessageKind::FireAndForget,
            None,
        )
        .await;
        handle_overlay_message(
            OverlayMessage {},
            state,
            Uuid::new_v4(),
            MessageKind::FireAndForget,
            None,
        )
        .await;
    }
}
