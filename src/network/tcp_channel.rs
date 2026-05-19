use crate::network::framing::*;
use crate::network::message_types::{HelloMessage, PeerMessage, PeerMessagePayload};
use crate::network::peer_manager_task::{PeerManagerMessage, peer_manager_task};
use crate::protocol::tcp_handler::*;
use crate::state::app_state::AppState;
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
    tcp_channel_sender: mpsc::Sender<PeerMessage>,
    tcp_channel_receiver: mpsc::Receiver<PeerMessage>,
    state: AppState,
) {
    // create hello message
    let hello = PeerMessage::hello(HelloMessage {
        listening_address: state.config.listen_address,
    });

    // encode hello message
    let encoded = match encode(&hello) {
        Ok(b) => b,
        Err(e) => {
            log::error!("failed to encode hello for peer {}: {}", peer_address, e);
            notify_manager(
                state.clone(),
                peer_address,
                PeerManagerMessage::ConnectionLost,
            )
            .await;
            return;
        }
    };

    // send hello message
    if stream.write_all(&encoded).await.is_err() {
        log::error!("error sending hello to peer {}", peer_address);
        notify_manager(
            state.clone(),
            peer_address,
            PeerManagerMessage::ConnectionLost,
        )
        .await;
        return;
    }

    log::info!("tcp channel to {} initiated", peer_address);
    notify_manager(
        state.clone(),
        peer_address,
        PeerManagerMessage::ConnectionReady(tcp_channel_sender),
    )
    .await;

    // create tcp channel handling task
    tcp_channel(
        peer_address,
        stream,
        tcp_channel_receiver,
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

    // save new connection
    let (tcp_channel_sender, tcp_channel_receiver) =
        mpsc::channel(state.config.tcp_channel_queue_size);

    // manager needs to be created if this is the first time connecting with this peer
    ensure_manager_exists(state.clone(), peer_address);

    notify_manager(
        state.clone(),
        peer_address,
        PeerManagerMessage::ConnectionReady(tcp_channel_sender),
    )
    .await;

    // go into tcp channel routine
    tcp_channel(peer_address, stream, tcp_channel_receiver, state, read_buf).await;
}

// communicate with a certain peer (send/receive tcp)
async fn tcp_channel(
    peer_address: SocketAddr,
    mut stream: TcpStream,
    mut tcp_channel_receiver: mpsc::Receiver<PeerMessage>,
    state: AppState,
    mut read_buf: BytesMut,
) {
    loop {
        tokio::select! {
            // send msg
            Some(msg) = tcp_channel_receiver.recv() => {
                if msg.is_expired() {
                    log::warn!("dropping expired message {} for {}", msg.message_id, peer_address);
                    continue;
                }
                let encoded = match encode(&msg) {
                    Ok(b) => b,
                    Err(e) => {
                        log::error!("failed to encode message for peer {}: {}", peer_address, e);
                        continue;
                    }
                };

                if stream.write_all(&encoded).await.is_err() {
                    log::error!("writing to tcp stream failed to peer {}", peer_address);
                    break;
                }
            }
            // receive msg
            result = stream.read_buf(&mut read_buf) => {
                match result {
                    Ok(0) => {
                        // connection closed
                        log::info!("got zero bytes. peer {} probably disconnected", peer_address);
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
                                    notify_manager(state.clone(), peer_address, PeerManagerMessage::ConnectionLost).await;
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
    notify_manager(
        state.clone(),
        peer_address,
        PeerManagerMessage::ConnectionLost,
    )
    .await;
}

async fn notify_manager(state: AppState, peer_address: SocketAddr, msg: PeerManagerMessage) {
    let manager_sender_opt = {
        let managers = state.peer_managers.lock().unwrap();
        managers.get(&peer_address)
    };
    match manager_sender_opt {
        Some(manager_sender) => {
            if let Err(e) = manager_sender.send(msg).await {
                log::error!("failed notifying manager for peer {}: {}", peer_address, e);
            }
        }
        None => {
            log::warn!("no manager found for peer {}", peer_address);
        }
    }
}

fn ensure_manager_exists(state: AppState, peer_address: SocketAddr) {
    let mut managers = state.peer_managers.lock().unwrap();
    if managers.has_manager(&peer_address) {
        return;
    }
    // TODO: configurable channel size?
    let (manager_sender, manager_receiver) = mpsc::channel(256);
    managers.insert(peer_address, manager_sender);
    tokio::spawn(peer_manager_task(
        peer_address,
        manager_receiver,
        state.clone(),
    ));
    log::info!("created manager task for peer {}", peer_address);
}
