use crate::protocol::handler::handle_message;
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
