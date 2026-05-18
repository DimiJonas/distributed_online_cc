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

// initiate a tcp channel to another peer
// performs the minimal handshake - sends hello
pub async fn initiate_tcp_connection(
    peer_address: SocketAddr,
    mut stream: TcpStream,
    state: AppState,
) {
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

// accept a tcp channel from another peer
// performs the minimal handshake - receives hello
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
