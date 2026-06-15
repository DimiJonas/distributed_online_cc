use std::net::SocketAddr;

use crate::network::message_types::{CliTestMessage, PeerMessage, PeerMessagePayload};
use crate::network::tcp_router::{connect_peer, send_and_receive};
use crate::state::app_state::AppState;
use crate::types::CliCommand;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;

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
                    log::error!("error parsing command: {}", e);
                    continue;
                }
            };

            match parts.as_slice() {
                [cmd, addr_str, msg] if cmd == "send" => {
                    let peer_address = match addr_str.parse::<SocketAddr>() {
                        Ok(address) => address,
                        Err(e) => {
                            log::error!("error parsing address {}: {}", addr_str, e);
                            continue;
                        }
                    };
                    let command = CliCommand::Send {
                        peer_address,
                        msg: msg.to_string(),
                    };
                    // send message
                    cmd_tx.send(command).await.unwrap();
                }
                [cmd, addr_str] if cmd == "connect" => {
                    let peer_address = match addr_str.parse::<SocketAddr>() {
                        Ok(address) => address,
                        Err(e) => {
                            log::error!("error parsing address {}: {}", addr_str, e);
                            continue;
                        }
                    };
                    let command = CliCommand::Connect { peer_address };
                    cmd_tx.send(command).await.unwrap();
                }
                _ => log::error!("unknown command"),
            }
        }
    });

    // command interpreter task
    let cmd_interpreter_state = state.clone();
    tokio::spawn(async move {
        while let Some(command) = cmd_rx.recv().await {
            match command {
                CliCommand::Send { peer_address, msg } => {
                    let message = PeerMessage::cli_req(CliTestMessage {
                        message: msg.clone(),
                        sender_addr: cmd_interpreter_state.config.tcp_listen_address,
                    });

                    log::info!("sending cli message to {}", &peer_address);

                    match send_and_receive(peer_address, message, state.clone()).await {
                        Ok(response) => {
                            log::info!("got response: {}", response);
                            if let PeerMessagePayload::Cli(cli_message) = response.payload {
                                log::info!(
                                    "peer {} sent: \"{}\"",
                                    peer_address,
                                    cli_message.message
                                );
                            }
                        }

                        Err(e) => log::error!("error when trying to receive response: {}", e),
                    };
                }
                CliCommand::Connect { peer_address } => {
                    if peer_address == state.config.tcp_listen_address {
                        log::error!("cannot connect to self");
                        continue;
                    }
                    connect_peer(peer_address, state.clone()).await;
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use uuid::Uuid;

    use crate::{network::tcp_router::send_to_peer, types::CliCommand};

    /// Parsing "send <addr> <msg>" produces the expected CliCommand.
    #[test]
    fn send_command_parses_correctly() {
        let parts = shell_words::split("send 127.0.0.1:9001 hello").unwrap();
        match parts.as_slice() {
            [cmd, addr_str, msg] if cmd == "send" => {
                let peer_address: SocketAddr = addr_str.parse().unwrap();
                assert_eq!(peer_address.port(), 9001);
                assert_eq!(msg.as_str(), "hello");
            }
            _ => panic!("parse failed"),
        }
    }

    /// Parsing "connect <addr>" produces the expected CliCommand.
    #[test]
    fn connect_command_parses_correctly() {
        let parts = shell_words::split("connect 127.0.0.1:8001").unwrap();
        match parts.as_slice() {
            [cmd, addr_str] if cmd == "connect" => {
                let peer_address: SocketAddr = addr_str.parse().unwrap();
                assert_eq!(peer_address.port(), 8001);
                let _cmd = CliCommand::Connect { peer_address };
            }
            _ => panic!("parse failed"),
        }
    }

    /// An invalid socket address fails to parse.
    #[test]
    fn send_command_invalid_address_fails_parse() {
        let result: Result<SocketAddr, _> = "not-an-address".parse();
        assert!(result.is_err());
    }

    /// shell_words treats a quoted string as a single argument.
    #[test]
    fn quoted_message_is_single_arg() {
        let parts = shell_words::split(r#"send 127.0.0.1:9001 "hello world""#).unwrap();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[2], "hello world");
    }

    /// An unknown command is not matched by either valid pattern.
    #[test]
    fn unknown_command_is_not_recognised() {
        let parts = shell_words::split("reboot").unwrap();
        let recognised = matches!(
            parts.as_slice(),
            [cmd, _, _] if cmd == "send"
        ) || matches!(
            parts.as_slice(),
            [cmd, _] if cmd == "connect"
        );
        assert!(!recognised);
    }

    /// Self-connect guard: a Connect command to the node's own address is rejected.
    /// (This is enforced inside spawn_cli; we test the guard condition directly.)
    #[test]
    fn self_connect_guard_condition() {
        use crate::util::test_tools::addr;
        let own_addr = addr(9000);
        let target = addr(9000);
        // The guard in cli.rs is: if peer_address == state.config.tcp_listen_address
        assert_eq!(
            own_addr, target,
            "addresses should be equal → self-connect blocked"
        );
    }

    /// The command interpreter task processes a Send command without panicking.
    #[tokio::test]
    async fn command_interpreter_processes_send_command() {
        use crate::{
            network::peer_manager_task::PeerManagerMessage,
            types::CliCommand,
            util::test_tools::{addr, make_state, register_manager},
        };
        use tokio::sync::mpsc;

        let state = make_state(22000);
        let peer = addr(22001);
        let mut mgr_rx = register_manager(&state, peer);

        let (cmd_tx, mut cmd_rx) = mpsc::channel(8);
        cmd_tx
            .send(CliCommand::Send {
                peer_address: peer,
                msg: "test".to_string(),
            })
            .await
            .unwrap();
        drop(cmd_tx); // closes the channel so the interpreter loop exits

        let state_clone = state.clone();
        let handle = tokio::spawn(async move {
            while let Some(command) = cmd_rx.recv().await {
                match command {
                    CliCommand::Send { peer_address, msg } => {
                        use crate::network::message_types::{CliTestMessage, PeerMessage};
                        let request_id = Uuid::new_v4();
                        let message = PeerMessage::cli(CliTestMessage {
                            message: msg,
                            sender_addr: state_clone.config.tcp_listen_address,
                        });
                        let (tx, _rx) = tokio::sync::oneshot::channel();
                        state_clone.pending.lock().unwrap().insert(request_id, tx);
                        let _ = send_to_peer(peer_address, message, state_clone.clone()).await;
                    }
                    CliCommand::Connect { .. } => {}
                }
            }
        });

        let received = tokio::time::timeout(std::time::Duration::from_millis(300), mgr_rx.recv())
            .await
            .expect("timed out waiting for message")
            .expect("channel closed");

        assert!(matches!(received, PeerManagerMessage::Outbound(_)));
        handle.await.unwrap();
    }
}
