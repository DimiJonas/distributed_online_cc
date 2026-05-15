use crate::network::framing::*;
use crate::network::message_types::{HelloMessage, PeerMessage, PeerMessagePayload};
use crate::protocol::tcp_handler::*;
use crate::state::app_state::AppState;
use crate::state::peer_manager::PeerHandle;
use bytes::BytesMut;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

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

// convenience method to build tcp channel to another peer
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
    }
}

// convenience method to send something to a peer with some error handling
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

// initiate a tcp channel to another peer - send hello
async fn initiate_tcp_connection(peer_address: SocketAddr, mut stream: TcpStream, state: AppState) {
    let my_address = state.config.listen_address;

    // create hello message
    let hello = PeerMessage::hello(HelloMessage {
        listening_address: my_address,
    });

    // encode hello message
    let encoded = match encode(&hello) {
        Ok(b) => b,
        Err(e) => {
            log::error!("failed to encode hello for peer {}: {}", peer_address, e);
            return;
        }
    };

    // send hello message
    if stream.write_all(&encoded).await.is_err() {
        log::error!("error sending hello to peer {}", peer_address);
        return;
    }

    let (tx, rx) = mpsc::channel(state.config.tcp_channel_queue_size);

    // save new peer in peer manager
    {
        let mut peers = state.peers.lock().unwrap();
        peers.insert(peer_address, PeerHandle::new(tx));
    }

    log::info!("tcp channel to {} initiated", peer_address);

    // create tcp channel handling task
    tcp_channel(
        peer_address,
        stream,
        rx,
        state,
        BytesMut::with_capacity(1024),
    )
    .await;
}

// accept a tcp channel from another peer - receive hello
pub async fn accept_incoming_tcp_connection(mut stream: TcpStream, state: AppState) {
    let mut read_buf = BytesMut::with_capacity(1024);

    // receive hello message
    let peer_address = loop {
        match stream.read_buf(&mut read_buf).await {
            Ok(0) => {
                log::error!("peer disconnected before sending Hello");
                return;
            }
            Ok(_) => {}
            Err(e) => {
                log::error!("read error while waiting for Hello: {}", e);
                return;
            }
        }

        if !check_frame(&read_buf) {
            continue;
        }

        let msg = match decode(&mut read_buf) {
            Ok(m) => m,
            Err(e) => {
                log::error!("failed to decode Hello frame: {}", e);
                return;
            }
        };

        match msg.payload {
            PeerMessagePayload::Hello(hello_msg) => break hello_msg.listening_address,
            _ => {
                log::error!("expected Hello as the first message, dropping connection");
                return;
            }
        }
    };

    log::info!("accepted tcp connection from peer {}", peer_address);

    // check if already connected to same address
    let already_exists = {
        let peers = state.peers.lock().unwrap();
        peers.contains(&peer_address)
    };
    if already_exists {
        log::warn!(
            "there already exists a tcp channel to {}, dropping connection",
            peer_address
        );
        return;
    }

    // save new connection
    let (tx, rx) = mpsc::channel(state.config.tcp_channel_queue_size);

    {
        let mut peers = state.peers.lock().unwrap();
        peers.insert(peer_address, PeerHandle::new(tx));
    }

    // create tcp channel handling task
    tcp_channel(peer_address, stream, rx, state, read_buf).await;
}

// task that communicates with a certain peer
pub async fn tcp_channel(
    peer_address: SocketAddr,
    mut stream: TcpStream,
    mut rx: mpsc::Receiver<PeerMessage>,
    state: AppState,
    mut read_buf: BytesMut,
) {
    loop {
        tokio::select! {
            // send msg
            Some(msg) = rx.recv() => {
                let encoded = match encode(&msg) {
                    Ok(b) => b,
                    Err(e) => {
                        log::error!("failed to encode message for peer {}: {}", peer_address, e);
                        continue;
                    }
                };

                if stream.write_all(&encoded).await.is_err() {
                    log::error!("error sending to peer {}", peer_address);
                    break;
                }
            }
            // receive msg
            result = stream.read_buf(&mut read_buf) => {
                match result {
                    Ok(0) => {
                        // connection closed
                        log::info!("got zero bytes. peer {} probably disconnected. removing peer from peer manager", peer_address);
                        let mut peers = state.peers.lock().unwrap();
                        peers.remove(&peer_address);
                        break;
                    }
                    Ok(_) => {
                        // some bytes received
                        loop {
                            if !check_frame(&read_buf) {
                                break;
                            }

                            let msg = match decode(&mut read_buf) {
                                Ok(m) => m,
                                Err(e) => {
                                    log::error!(
                                        "failed to decode message from peer {}: {}, closing connection and removing peer from peer manager",
                                        peer_address, e
                                    );
                                    // deserialization failure probably means the stream is
                                    // corrupted so it should be closed
                                    let mut peers = state.peers.lock().unwrap();
                                    peers.remove(&peer_address);
                                    return;
                                }
                            };

                            log::info!(
                                "decoded message from peer {}: {}",
                                peer_address,
                                msg
                            );

                            // dispatch message based on payload type of the peer message
                            match msg.payload {
                                PeerMessagePayload::Hello(_) => {
                                    log::warn!("unexpected Hello from peer {} (connection is already established)", peer_address);
                                },
                                PeerMessagePayload::Cli(cli_msg) => {
                                    tokio::spawn(handle_cli_message(cli_msg, state.clone(), msg.message_id));
                                },
                                PeerMessagePayload::Conformance(cc_msg) => {
                                    tokio::spawn(handle_cc_message(cc_msg, state.clone(), msg.message_id));
                                },
                                PeerMessagePayload::Overlay(overlay_msg) => {
                                    tokio::spawn(handle_overlay_message(overlay_msg, state.clone(), msg.message_id));
                                }
                                PeerMessagePayload::TerminationDetection(td_msg) => {
                                    tokio::spawn(handle_td_message(td_msg, state.clone(), msg.message_id));
                                }
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("error reading from peer {}: {}", peer_address, e);
                        break;
                    }
                }
            }
        }
    }

    log::info!("peer connection to {} closed", peer_address);
}
