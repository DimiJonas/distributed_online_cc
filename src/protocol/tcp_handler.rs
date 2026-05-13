use uuid::Uuid;

use crate::state::app_state::AppState;
use crate::types::{
    CCMessage, CliTestMessage, OverlayMessage, PeerMessage, TerminationDetectionMessage,
};

pub async fn handle_cli_message(msg: CliTestMessage, state: AppState, msg_id: Uuid) {
    println!(
        "received cli message; {}",
        serde_json::to_string_pretty(&msg).unwrap()
    );

    if msg.message.starts_with("ACK") {
        // its a response

        // remove pending request
        let (sender, len) = {
            let mut pending = state.pending.lock().unwrap();
            (pending.remove(&msg.request_id), pending.len())
        };

        if sender.is_some() {
            println!(
                "matched response with pending request. {} pending requests left",
                len
            );
        } else {
            println!(
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

        let sender = {
            let peers = state.peers.lock().unwrap();
            peers.get_sender(&msg.sender_addr)
        };
        if let Some(sender) = sender {
            sender.send(response).await.unwrap();
        }
    }
}

pub async fn handle_cc_message(msg: CCMessage, state: AppState, msg_id: Uuid) {
    println!("handling conformance message with id {}", &msg_id);

    if let Some(handler) = state.handlers.get_by_constraint_id(&msg.constraint_id) {
        handler.handle_message(state.clone(), msg).await;
    }
}

pub async fn handle_td_message(msg: TerminationDetectionMessage, state: AppState, msg_id: Uuid) {}

pub async fn handle_overlay_message(msg: OverlayMessage, state: AppState, msg_id: Uuid) {}
