use std::net::SocketAddr;

use crate::network::framing::*;
use crate::network::message_types::{HelloMessage, PeerMessage, PeerMessagePayload};
use crate::protocol::tcp_handler::*;
use crate::state::app_state::AppState;
use crate::state::peer_manager::PeerHandle;
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

// convenience method to build tcp channel to another peer
pub async fn connect_peer(peer_address: SocketAddr, state: AppState) {
    match TcpStream::connect(&peer_address).await {
        Ok(stream) => {
            tokio::spawn(peer_task_initiated(peer_address, stream, state.clone()));
        }
        Err(e) => {
            println!("error connecting to peer {}: {}", peer_address, e);
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
            sender.send(msg).await.unwrap();
            println!("sent message to {}", peer_address);
        }
        None => {
            println!("peer {} not connected yet", peer_address);
            println!("connecting to peer {}", peer_address);
            connect_peer(peer_address, state.clone()).await;
            let sender = {
                let peers = state.peers.lock().unwrap();
                peers.get_sender(&peer_address).unwrap()
            };
            sender.send(msg).await.unwrap();
            println!("sent message to {}", peer_address);
        }
    }
}

// initiate a tcp channel to another peer - send hello
pub async fn peer_task_initiated(peer_address: SocketAddr, mut stream: TcpStream, state: AppState) {
    let my_address = state.config.listen_address;
    let hello = PeerMessage::hello(HelloMessage {
        listening_address: my_address,
    });

    if stream.write_all(&encode(&hello)).await.is_err() {
        eprintln!("error sending hello to peer {}", peer_address);
        return;
    }

    let (tx, rx) = mpsc::channel(state.config.tcp_channel_queue_size);

    {
        let mut peers = state.peers.lock().unwrap();
        peers.insert(peer_address, PeerHandle::new(tx));
    }

    println!("tcp channel to {} initiated", peer_address);

    peer_task(
        peer_address,
        stream,
        rx,
        state,
        BytesMut::with_capacity(1024),
    )
    .await;
}

// accept a tcp channel from another peer - receive hello
pub async fn peer_task_accepted(mut stream: TcpStream, state: AppState) {
    let mut read_buf = BytesMut::with_capacity(1024);

    let peer_address = loop {
        stream.read_buf(&mut read_buf).await.unwrap();
        if !check_frame(&read_buf) {
            continue;
        }
        let msg: PeerMessage = decode(&mut read_buf);
        match msg.payload {
            PeerMessagePayload::Hello(hello_msg) => break hello_msg.listening_address,
            _ => {
                eprintln!("expected Hello as the first message, dropping connection");
                return;
            }
        }
    };
    println!("accepted tcp connection from peer {}", peer_address);

    let already_exists = {
        let peers = state.peers.lock().unwrap();
        peers.contains(&peer_address)
    };
    if already_exists {
        eprintln!(
            "there already exists a tcp channel to {}, dropping connection",
            peer_address
        );
        return;
    }

    let (tx, rx) = mpsc::channel(state.config.tcp_channel_queue_size);

    {
        let mut peers = state.peers.lock().unwrap();
        peers.insert(peer_address, PeerHandle::new(tx));
    }

    peer_task(peer_address, stream, rx, state, read_buf).await;
}

// task that communicates with a certain peer
pub async fn peer_task(
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
                let framed_msg = encode(&msg);

                if stream.write_all(&framed_msg).await.is_err() {
                    eprintln!("error sending to peer {}", peer_address);
                    break;
                }
            }
            // receive msg
            result = stream.read_buf(&mut read_buf) => {
                match result {
                    Ok(0) => {
                        // connection closed
                        println!("got zero bytes. peer {} disconnected", peer_address);
                        println!("removing peer {} from peer manager", peer_address);
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

                            let msg: PeerMessage = decode(&mut read_buf);
                            println!("decoded message from peer {}: {}", peer_address, serde_json::to_string_pretty(&msg).unwrap());

                            // dispatch message based on payload type of the peer message
                            match msg.payload {
                                PeerMessagePayload::Hello(_) => {
                                    eprintln!("unexpected Hello from peer {} as connection is already established", peer_address);
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
                        eprintln!("error reading from peer: {}", e);
                        break;
                    }
                }
            }
        }
    }

    println!("Peer connection closed");
}
