use crate::state::app_state::AppState;
use crate::types::{PeerId, PeerMessage};

pub async fn handle_message(peer_id: PeerId, msg: PeerMessage, state: AppState) {
    match msg {
        PeerMessage::Request { id, payload } => {
            println!(
                "received request; peer_id: {}, request_id: {}, payload: {}",
                peer_id, id, payload
            );

            // send response
            let response = PeerMessage::Response {
                id,
                payload: format!("ACK '{}'", payload),
            };

            let sender = {
                let peers = state.peers.lock().unwrap();
                peers.get_sender(&peer_id)
            };
            if let Some(sender) = sender {
                sender.send(response).await.unwrap();
            }
        }
        PeerMessage::Response { id, payload } => {
            println!(
                "received response; peer_id: {}, request_id: {}, payload: {}",
                peer_id, id, payload
            );

            let (sender, len) = {
                let mut pending = state.pending.lock().unwrap();
                (pending.remove(&id), pending.len())
            };

            if let Some(tx) = sender {
                tx.send(PeerMessage::Response { id, payload }).unwrap();
                println!(
                    "matched response with pending request. {} pending requests left",
                    len
                );
            } else {
                println!("no pending request found for response with id {}", id);
            }
        }
    }
}
