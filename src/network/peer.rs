use crate::state::peer_manager::PeerManager;
use crate::state::pending_reuests::PendingRequestManager;
use crate::types::{PeerId, PeerMessage};
use bytes::BytesMut;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc};

// spawn tasks that communicate with a certain peer
pub async fn peer_task(
    peer_id: PeerId,
    mut stream: TcpStream,
    mut rx: mpsc::Receiver<PeerMessage>,
    peer_manager: Arc<Mutex<PeerManager>>,
    pending_request_manager: Arc<Mutex<PendingRequestManager>>,
) {
    let mut read_buf = BytesMut::with_capacity(1024);

    loop {
        tokio::select! {
            // send msg
            Some(msg) = rx.recv() => {
                let framed_msg = crate::network::framing::encode(&msg);

                if stream.write_all(&framed_msg).await.is_err() {
                    eprintln!("error sending to peer {}", peer_id);
                    break;
                }
            }
            // receive msg
            result = stream.read_buf(&mut read_buf) => {
                match result {
                    Ok(0) => {
                        // connection closed
                        println!("got zero bytes. peer {} disconnected", peer_id);
                        println!("removing peer {} from peer manager", peer_id);
                        peer_manager.lock().await.remove(&peer_id);
                        break;
                    }
                    Ok(_) => {
                        // some bytes received
                        loop {
                            if !crate::network::framing::check_frame(&read_buf) {
                                break;
                            }

                            let msg: PeerMessage = crate::network::framing::decode(&mut read_buf);
                            println!("decoded message from peer {}: {}", peer_id, serde_json::to_string_pretty(&msg).unwrap());
                            handle_message(peer_id, msg, peer_manager.clone(), pending_request_manager.clone()).await;
                        }
                    }
                    Err(e) => {
                        eprintln!("error reading from peer: {}", e);
                        break;
                    }
                }
            }
        }
    }

    println!("Peer connection closed");
}

async fn handle_message(
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
