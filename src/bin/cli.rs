use clap::Parser;
// use distributed_online_cc::cli_command::*;
// use distributed_online_cc::peer_message::*;
use distributed_online_cc::network::peer::peer_task;
use distributed_online_cc::state::peer_manager::{PeerHandle, PeerManager};
use distributed_online_cc::state::pending_reuests::PendingRequestManager;
use distributed_online_cc::types::{CliCommand, PeerId, PeerMessage, RequestId};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc, oneshot};

// parse command line arguments
#[derive(Parser)]
struct Input {
    id: PeerId,
}

struct RequestIdContainer {
    next_id: RequestId,
}

impl RequestIdContainer {
    fn new(initial_value: RequestId) -> Self {
        Self {
            next_id: initial_value,
        }
    }

    fn get_next_id(&mut self) -> RequestId {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}

fn get_address_from_id(id: PeerId) -> String {
    format!("127.0.0.1:{}", 7000 + id as u16)
}

fn get_id_from_address(addr: &std::net::SocketAddr) -> PeerId {
    (addr.port() - 7000) as PeerId
}

#[tokio::main]
async fn main() {
    let args = Input::parse();
    let host_id = args.id;
    let host_address = get_address_from_id(host_id);

    let request_id_container = Arc::new(Mutex::new(RequestIdContainer::new(host_id as u64 * 100)));
    let peer_manager = Arc::new(Mutex::new(PeerManager::new()));
    let pending_request_manager = Arc::new(Mutex::new(PendingRequestManager::new()));

    // peer listener task
    let listener = TcpListener::bind(&host_address).await.unwrap();
    println!("listening for peer messages on {}", host_address);
    let pm_clone = peer_manager.clone();
    let prm_clone = pending_request_manager.clone();
    tokio::spawn(async move {
        loop {
            let (stream, addr) = listener.accept().await.unwrap();
            println!("new peer connection from {}", addr);

            let peer_id = get_id_from_address(&addr);

            let (tx, rx) = mpsc::channel(100);
            pm_clone
                .lock()
                .await
                .insert(peer_id, PeerHandle::new(tx.clone()));
            tokio::spawn(peer_task(
                peer_id,
                stream,
                rx,
                pm_clone.clone(),
                prm_clone.clone(),
            ));
        }
    });

    // cli command processing task
    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    let (cmd_tx, mut cmd_rx) = mpsc::channel(100);

    tokio::spawn(async move {
        while let Ok(Some(line)) = lines.next_line().await {
            let parts = match shell_words::split(&line) {
                Ok(parts) => parts,
                Err(e) => {
                    println!("error parsing command: {}", e);
                    continue;
                }
            };

            match parts.as_slice() {
                [cmd, id_str, msg] if cmd == "send" => {
                    let command = CliCommand::Send {
                        peer_id: id_str.parse().unwrap(),
                        msg: msg.to_string(),
                    };
                    // send message
                    cmd_tx.send(command).await.unwrap();
                }
                [cmd, id_str] if cmd == "connect" => {
                    let command = CliCommand::Connect {
                        peer_id: id_str.parse().unwrap(),
                    };
                    cmd_tx.send(command).await.unwrap();
                }
                _ => println!("unknown command"),
            }
        }
    });

    // task that receives the command and interprets it to send a message to the peer
    let prm_clone_clone = pending_request_manager.clone();
    tokio::spawn(async move {
        while let Some(command) = cmd_rx.recv().await {
            match command {
                CliCommand::Send { peer_id, msg } => {
                    let request_id = request_id_container.lock().await.get_next_id();
                    let message = PeerMessage::Request {
                        id: request_id,
                        payload: msg.clone(),
                    };
                    // waiting channel for response
                    let (tx, rx) = oneshot::channel();

                    // send msg to peer_address
                    let sender = {
                        let pm = peer_manager.lock().await;
                        pm.get_sender(&peer_id)
                    };
                    if let Some(sender) = sender {
                        prm_clone_clone.lock().await.insert(request_id, tx);
                        sender.send(message).await.unwrap();
                        println!(
                            "sent request. now {} pending requests in total",
                            pending_request_manager.lock().await.len()
                        );
                        match rx.await {
                            Ok(resp) => println!("got response: {:?}", resp),
                            Err(_) => println!(
                                "error when trying to receive response for request id {}",
                                request_id
                            ),
                        }
                    } else {
                        println!("peer {} not connected yet", peer_id);
                    }
                }
                CliCommand::Connect { peer_id } => {
                    if peer_id == host_id {
                        println!("cannot connect to self");
                        continue;
                    }

                    let peer_address = get_address_from_id(peer_id);

                    let stream = TcpStream::connect(&peer_address).await.unwrap();
                    println!("connected to peer {}", peer_id);

                    let (tx, rx) = mpsc::channel(100);
                    peer_manager
                        .lock()
                        .await
                        .insert(peer_id, PeerHandle::new(tx.clone()));
                    tokio::spawn(peer_task(
                        peer_id,
                        stream,
                        rx,
                        peer_manager.clone(),
                        pending_request_manager.clone(),
                    ));
                }
            }
        }
    });

    // keeps main alive until i shut it down
    tokio::signal::ctrl_c().await.unwrap();
    println!("shutting down main");
}
