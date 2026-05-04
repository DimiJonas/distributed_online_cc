use crate::network::peer::peer_task;
use crate::state::app_state::AppState;
use crate::state::peer_manager::PeerHandle;
use crate::types::{CliCommand, PeerId, PeerMessage};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};

fn get_address_from_id(id: PeerId) -> String {
    format!("127.0.0.1:{}", 7000 + id as u16)
}

pub fn spawn_cli(state: AppState, host_id: PeerId) {
    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    // connects command listener task and command interpreter task
    let (cmd_tx, mut cmd_rx) = mpsc::channel(100);

    // command listener task
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

    // command interpreter task
    let cmd_interpreter_state = state.clone();
    tokio::spawn(async move {
        while let Some(command) = cmd_rx.recv().await {
            match command {
                CliCommand::Send { peer_id, msg } => {
                    let request_id = {
                        let mut request_ids = cmd_interpreter_state.request_ids.lock().unwrap();
                        request_ids.get_next_id()
                    };

                    let message = PeerMessage::Request {
                        id: request_id,
                        payload: msg.clone(),
                    };
                    // waiting channel for response
                    let (tx, rx) = oneshot::channel();

                    // send msg to peer_address
                    let sender = {
                        let peers = state.peers.lock().unwrap();
                        peers.get_sender(&peer_id)
                    };
                    if let Some(sender) = sender {
                        let len = {
                            let mut pending = cmd_interpreter_state.pending.lock().unwrap();
                            pending.insert(request_id, tx);
                            pending.len()
                        };

                        sender.send(message).await.unwrap();
                        println!("sent request. now {} pending requests in total", len);
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

                    match TcpStream::connect(&peer_address).await {
                        Ok(stream) => {
                            println!("connected to peer {}", peer_id);

                            let (tx, rx) = mpsc::channel(100);
                            {
                                let mut peers = cmd_interpreter_state.peers.lock().unwrap();
                                peers.insert(peer_id, PeerHandle::new(tx.clone()));
                            }

                            tokio::spawn(peer_task(
                                peer_id,
                                stream,
                                rx,
                                cmd_interpreter_state.clone(),
                            ));
                        }
                        Err(e) => {
                            println!("error connecting to peer {}: {}", peer_id, e);
                        }
                    }
                }
            }
        }
    });
}
