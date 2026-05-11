use crate::state::app_state::AppState;
use crate::types::{
    CCMessage, CliTestMessage, OverlayMessage, PeerMessage, PeerMessagePayload,
    TerminationDetectionMessage,
};

pub async fn handle_cli_message(msg: CliTestMessage, state: AppState) {
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

        if let Some(_) = sender {
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
        let resp_payload = PeerMessagePayload::Cli(CliTestMessage {
            request_id: msg.request_id,
            message: format!("ACK '{}'", msg.message),
            sender_addr: state.config.listen_address.clone(),
        });
        let response = PeerMessage::new(resp_payload);

        let sender = {
            let peers = state.peers.lock().unwrap();
            peers.get_sender(&msg.sender_addr)
        };
        if let Some(sender) = sender {
            sender.send(response).await.unwrap();
        }
    }
}

pub async fn handle_cc_message(msg: CCMessage, state: AppState) {}

pub async fn handle_td_message(msg: TerminationDetectionMessage, state: AppState) {}

pub async fn handle_overlay_message(msg: OverlayMessage, state: AppState) {}
