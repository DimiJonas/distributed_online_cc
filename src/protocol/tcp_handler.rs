/*
    note: tcp handlers have to use the outbound router queue to send messages back to other peers using tcp
    this is needed to break a cycle that would emerge when tcp handlers would call tcp channel methods since tcp channels also call the tcp handlers when receiving a message
*/

use crate::network::message_types::{
    CCMessage, CliTestMessage, OverlayMessage, PeerMessage, TerminationDetectionMessage,
};
use crate::network::peer_manager_task::PeerManagerMessage;
use crate::state::app_state::AppState;
use uuid::Uuid;

pub async fn handle_cli_message(msg: CliTestMessage, state: AppState, msg_id: Uuid) {
    log::debug!("received cli message id={} msg={}", msg_id, msg);

    if msg.message.starts_with("ACK") {
        // its a response

        // remove pending request
        let (sender, len) = {
            let mut pending = state.pending.lock().unwrap();
            (pending.remove(&msg.request_id), pending.len())
        };

        if let Some(waiting_channel) = sender {
            log::info!(
                "matched response with pending request. {} pending requests left",
                len
            );
            // notofy task that is waiting for response
            waiting_channel.send(PeerMessage::cli(msg)).unwrap();
        } else {
            log::warn!(
                "no pending request found for response with id {}",
                msg.request_id
            );
        }
    } else {
        // its a request

        // send response
        let response = PeerMessage::cli(CliTestMessage {
            request_id: msg.request_id,
            message: format!("ACK '{}'", msg.message),
            sender_addr: state.config.tcp_listen_address,
        });

        let manager_tx = {
            let managers = state.peer_managers.lock().unwrap();
            managers.get(&msg.sender_addr)
        };
        match manager_tx {
            Some(tx) => {
                if let Err(e) = tx
                    .send(PeerManagerMessage::Outbound(Box::new(response)))
                    .await
                {
                    log::error!("failed to enqueue cli response: {}", e);
                }
            }
            None => {
                log::error!(
                    "no peer manager for {} found to send cli response to",
                    msg.sender_addr
                );
            }
        }
    }
}

pub async fn handle_cc_message(msg: CCMessage, state: AppState, msg_id: Uuid) {
    log::debug!(
        "handling conformance message with id={} msg={}",
        &msg_id,
        msg
    );

    if let Some(handler) = state.handlers.get_by_constraint_id(&msg.constraint_id) {
        handler.handle_message(state.clone(), msg).await;
    }
}

pub async fn handle_td_message(msg: TerminationDetectionMessage, _state: AppState, msg_id: Uuid) {
    log::debug!(
        "handling termination detection message message with id={} msg={}",
        &msg_id,
        msg
    );
    // TODO
}

pub async fn handle_overlay_message(msg: OverlayMessage, _state: AppState, msg_id: Uuid) {
    log::debug!("handling overlay message with id={} msg={}", &msg_id, msg);
    // TODO
}

#[cfg(test)]
mod tests {
    use tokio::sync::oneshot;
    use uuid::Uuid;

    use crate::{
        config::node_config::ConstraintConfig,
        network::{
            message_types::{
                CCMessage, CliTestMessage, OverlayMessage, PeerMessagePayload,
                TerminationDetectionMessage,
            },
            peer_manager_task::PeerManagerMessage,
        },
        protocol::tcp_handler::{
            handle_cc_message, handle_cli_message, handle_overlay_message, handle_td_message,
        },
        state::app_state::AppState,
        types::ConstraintType,
        util::test_tools::{
            addr, make_config_with_constraints_and_port, make_state, register_manager,
        },
    };

    // -- handle_cli_message --

    /// An ACK response is matched to a pending request and the oneshot fires.
    #[tokio::test]
    async fn cli_ack_resolves_pending_request() {
        let state = make_state(21000);

        let request_id: u64 = 42;
        let (tx, rx) = oneshot::channel();
        {
            state.pending.lock().unwrap().insert(request_id, tx);
        }

        let ack_msg = CliTestMessage {
            request_id,
            message: "ACK 'hello'".to_string(),
            sender_addr: addr(21001),
        };
        handle_cli_message(ack_msg, state.clone(), Uuid::new_v4()).await;

        assert!(state.pending.lock().unwrap().is_empty());

        let resp = rx.await.expect("oneshot was not sent");
        assert!(matches!(resp.payload, PeerMessagePayload::Cli(_)));
    }

    /// An ACK for an unknown request_id is silently ignored.
    #[tokio::test]
    async fn cli_ack_unknown_request_does_not_panic() {
        let state = make_state(21010);
        let msg = CliTestMessage {
            request_id: 999,
            message: "ACK 'x'".to_string(),
            sender_addr: addr(21011),
        };
        handle_cli_message(msg, state, Uuid::new_v4()).await;
    }

    /// A non-ACK message triggers a response via the peer manager.
    #[tokio::test]
    async fn cli_request_sends_ack_response_to_peer_manager() {
        let state = make_state(21020);
        let sender_addr = addr(21021);
        let mut mgr_rx = register_manager(&state, sender_addr);

        let msg = CliTestMessage {
            request_id: 1,
            message: "hello world".to_string(),
            sender_addr,
        };
        handle_cli_message(msg, state.clone(), Uuid::new_v4()).await;

        let received = tokio::time::timeout(std::time::Duration::from_millis(200), mgr_rx.recv())
            .await
            .expect("timed out")
            .unwrap();

        if let PeerManagerMessage::Outbound(boxed) = received {
            if let PeerMessagePayload::Cli(m) = boxed.payload {
                assert!(m.message.starts_with("ACK"));
            } else {
                panic!("expected Cli payload");
            }
        } else {
            panic!("expected Outbound");
        }
    }

    /// A non-ACK when no peer manager exists logs an error but must not panic.
    #[tokio::test]
    async fn cli_request_no_manager_does_not_panic() {
        let state = make_state(21030);
        let msg = CliTestMessage {
            request_id: 1,
            message: "hello".to_string(),
            sender_addr: addr(21031),
        };
        handle_cli_message(msg, state, Uuid::new_v4()).await;
    }

    // -- handle_cc_message --

    /// A DeleteToken CCMessage is dispatched to the correct handler and removes the token.
    #[tokio::test]
    async fn cc_message_dispatched_to_correct_handler() {
        let config = make_config_with_constraints_and_port(
            vec![ConstraintConfig::ResponseProducer {
                constraint_id: "rp".to_string(),
                event_id: "e".to_string(),
            }],
            21040,
        );
        let (tx, _rx) = tokio::sync::mpsc::channel(16);
        let state = AppState::new(config, tx);

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
                1,
                None,
            );
            state.token_store.lock().unwrap().produce(token);
        }

        let delete_msg =
            CCMessage::delete_token("c1".to_string(), "rp".to_string(), uuid::Uuid::nil());
        handle_cc_message(delete_msg, state.clone(), Uuid::new_v4()).await;

        assert!(
            !state
                .token_store
                .lock()
                .unwrap()
                .has_open_token(&"c1".to_string(), &"rp".to_string())
        );
    }

    /// A CCMessage for an unknown constraint_id is silently ignored.
    #[tokio::test]
    async fn cc_message_unknown_constraint_does_not_panic() {
        let state = make_state(21050);
        let msg = CCMessage::delete_token("c".to_string(), "ghost".to_string(), uuid::Uuid::nil());
        handle_cc_message(msg, state, Uuid::new_v4()).await;
    }

    // -- stub handlers --

    /// Stub handlers (td / overlay) must not panic.
    #[tokio::test]
    async fn td_and_overlay_message_handlers_do_not_panic() {
        let state = make_state(21060);
        handle_td_message(
            TerminationDetectionMessage {},
            state.clone(),
            Uuid::new_v4(),
        )
        .await;
        handle_overlay_message(OverlayMessage {}, state, Uuid::new_v4()).await;
    }
}
