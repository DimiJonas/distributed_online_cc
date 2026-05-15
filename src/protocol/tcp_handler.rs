use uuid::Uuid;

use crate::network::message_types::{
    CCMessage, CliTestMessage, OverlayMessage, PeerMessage, TerminationDetectionMessage,
};
use crate::network::tcp_channel::send_to_peer;
use crate::state::app_state::AppState;

pub async fn handle_cli_message(msg: CliTestMessage, state: AppState, msg_id: Uuid) {
    log::debug!("received cli message id={} msg={}", msg_id, msg);

    if msg.message.starts_with("ACK") {
        // its a response

        // remove pending request
        let (sender, len) = {
            let mut pending = state.pending.lock().unwrap();
            (pending.remove(&msg.request_id), pending.len())
        };

        if sender.is_some() {
            log::info!(
                "matched response with pending request. {} pending requests left",
                len
            );
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
            sender_addr: state.config.listen_address,
        });

        send_to_peer(msg.sender_addr, response, state.clone()).await;
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
