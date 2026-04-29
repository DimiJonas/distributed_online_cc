use crate::state::peer_manager::PeerManager;
use crate::state::pending_reuests::PendingRequestManager;
use crate::types::{PeerId, PeerMessage};
use std::sync::Arc;
use tokio::sync::Mutex;

pub async fn handle_message(
    peer_id: PeerId,
    msg: PeerMessage,
    peer_manager: Arc<Mutex<PeerManager>>,
    pending_request_manager: Arc<Mutex<PendingRequestManager>>,
) {
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
                let pm = peer_manager.lock().await;
                pm.get_sender(&peer_id)
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

            let sender = {
                let mut prm = pending_request_manager.lock().await;
                prm.remove(&id)
            };
            if let Some(tx) = sender {
                tx.send(PeerMessage::Response { id, payload }).unwrap();
                println!(
                    "matched response with pending request. {} pending requests left",
                    pending_request_manager.lock().await.len()
                );
            } else {
                println!("no pending request found for response with id {}", id);
            }
        }
    }
}
