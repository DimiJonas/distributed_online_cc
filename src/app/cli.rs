use crate::network::peer::connect_peer;
use crate::state::app_state::AppState;
use crate::types::{CliCommand, CliTestMessage, PeerMessage};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::{mpsc, oneshot};

pub fn spawn_cli(state: AppState) {
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
                [cmd, addr_str, msg] if cmd == "send" => {
                    let command = CliCommand::Send {
                        peer_address: addr_str.parse().unwrap(),
                        msg: msg.to_string(),
                    };
                    // send message
                    cmd_tx.send(command).await.unwrap();
                }
                [cmd, addr_str] if cmd == "connect" => {
                    let command = CliCommand::Connect {
                        peer_address: addr_str.parse().unwrap(),
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
                CliCommand::Send { peer_address, msg } => {
                    let request_id = {
                        let mut request_ids = cmd_interpreter_state.request_ids.lock().unwrap();
                        request_ids.get_next_id()
                    };

                    let message = PeerMessage::cli(CliTestMessage {
                        request_id,
                        message: msg.clone(),
                        sender_addr: cmd_interpreter_state.config.listen_address,
                    });
                    // waiting channel for response
                    let (tx, rx) = oneshot::channel();

                    // send msg to peer_address
                    let sender = {
                        let peers = state.peers.lock().unwrap();
                        peers.get_sender(&peer_address)
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
                        println!("peer {} not connected yet", peer_address);
                    }
                }
                CliCommand::Connect { peer_address } => {
                    if peer_address == state.config.listen_address {
                        println!("cannot connect to self");
                        continue;
                    }
                    connect_peer(peer_address, state.clone()).await;
                }
            }
        }
    });
}
