/*
    this module exposes an api for the rest of the application to send tcp messages
*/
use crate::network::tcp_channel::initiate_tcp_connection;
use crate::{network::message_types::PeerMessage, state::app_state::AppState};
use std::net::SocketAddr;
use tokio::{net::TcpStream, sync::mpsc};

// build a tcp channel to another peer
pub async fn connect_peer(peer_address: SocketAddr, state: AppState) {
    let already_connected = {
        let peers = state.peers.lock().unwrap();
        peers.contains(&peer_address)
    };
    if !already_connected {
        match TcpStream::connect(&peer_address).await {
            Ok(stream) => {
                tokio::spawn(initiate_tcp_connection(peer_address, stream, state.clone()));
            }
            Err(e) => {
                log::error!("error connecting to peer {}: {}", peer_address, e);
            }
        }
    } else {
        log::info!(
            "tried to establish tcp channel to {}, but that channel already exists",
            peer_address
        );
    }
}

// send to another peer via tcp
pub async fn send_to_peer(peer_address: SocketAddr, msg: PeerMessage, state: AppState) {
    // retrieve correct tcp channel and send it
    let sender = {
        let peers = state.peers.lock().unwrap();
        peers.get_sender(&peer_address)
    };
    match sender {
        Some(sender) => {
            send_or_log(
                peer_address,
                &sender,
                msg,
                &format!("send_to_peer {}", peer_address),
            )
            .await;
        }
        None => {
            log::warn!(
                "peer {} not connected yet, attempting to connect",
                peer_address
            );
            connect_peer(peer_address, state.clone()).await;

            // Re-check after connecting; connect_peer may have failed silently.
            let sender = {
                let peers = state.peers.lock().unwrap();
                peers.get_sender(&peer_address)
            };
            match sender {
                Some(sender) => {
                    send_or_log(
                        peer_address,
                        &sender,
                        msg.clone(),
                        &format!("send_to_peer retry {}", peer_address),
                    )
                    .await;
                }
                None => {
                    log::error!(
                        "could not obtain sender for {} after connect attempt, dropping message {}",
                        peer_address,
                        msg
                    );
                }
            }
        }
    }
}

// helper method that either sends or logs an error
async fn send_or_log(
    to: SocketAddr,
    sender: &mpsc::Sender<PeerMessage>,
    msg: PeerMessage,
    context: &str,
) {
    // just create a display string to not have to copy the whole message with payload for the info log statement
    let msg_display_string = msg.display_string();
    if let Err(e) = sender.send(msg).await {
        log::error!("tcp channel send failed ({}): {}", context, e);
    } else {
        log::info!("sent message {} to {}", msg_display_string, to);
    }
}
