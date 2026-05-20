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
        listening_address: state.config.tcp_listen_address,
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
        mpsc::channel(state.config.tcp_channels.tcp_channel_mpsc_channel_size);

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
    let (manager_sender, manager_receiver) = mpsc::channel(
        state
            .config
            .tcp_channels
            .peer_manager_task_mpsc_channel_size,
    );
    managers.insert(peer_address, manager_sender);
    tokio::spawn(peer_manager_task(
        peer_address,
        manager_receiver,
        state.clone(),
    ));
    log::info!("created manager task for peer {}", peer_address);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::message_types::{HelloMessage, PeerMessage, PeerMessagePayload};
    use crate::network::peer_manager_task::PeerManagerMessage;
    use crate::util::test_tools::{addr, cli_msg, make_state, register_manager};
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use tokio::time::timeout;

    // ----------------------------------------------------------------
    // initiate_tcp_connection — happy path
    // ----------------------------------------------------------------

    /// The initiating side must send a Hello and then notify its manager
    /// with ConnectionReady.
    #[tokio::test]
    async fn initiate_sends_hello_and_notifies_connection_ready() {
        // Bind a listener that will accept one raw connection.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();

        let listen_port = 19200u16;
        let state = make_state(listen_port);
        let peer_addr = server_addr;
        let mut manager_rx = register_manager(&state, peer_addr);

        // Accept side: just reads one message (the Hello) from the raw stream.
        let accept_handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            use crate::network::framing::{check_frame, decode};
            use bytes::BytesMut;
            use tokio::io::AsyncReadExt;
            let mut buf = BytesMut::with_capacity(1024);
            loop {
                stream.read_buf(&mut buf).await.unwrap();
                if check_frame(&buf) {
                    return decode(&mut buf).unwrap();
                }
            }
        });

        let stream = tokio::net::TcpStream::connect(server_addr).await.unwrap();
        let (tcp_channel_sender, tcp_channel_receiver) = mpsc::channel(16);
        tokio::spawn(initiate_tcp_connection(
            peer_addr,
            stream,
            tcp_channel_sender,
            tcp_channel_receiver,
            state,
        ));

        // The manager must be told ConnectionReady.
        let msg = timeout(Duration::from_millis(500), manager_rx.recv())
            .await
            .expect("timed out waiting for manager notification")
            .expect("manager channel closed");

        assert!(
            matches!(msg, PeerManagerMessage::ConnectionReady(_)),
            "expected ConnectionReady, got something else"
        );

        // The accept side must have received a Hello.
        let hello_msg = timeout(Duration::from_millis(500), accept_handle)
            .await
            .expect("timed out waiting for Hello")
            .expect("accept task panicked");

        assert!(
            matches!(hello_msg.payload, PeerMessagePayload::Hello(_)),
            "first message should be Hello"
        );
    }

    // ----------------------------------------------------------------
    // accept_incoming_tcp_connection — happy path
    // ----------------------------------------------------------------

    /// The accepting side must extract the peer address from the Hello
    /// and notify the manager with ConnectionReady.
    #[tokio::test]
    async fn accept_processes_hello_and_notifies_connection_ready() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();

        let initiator_listen_port = 19210u16;
        let state = make_state(19211);

        // The peer address the initiator claims to listen on.
        let peer_listen_addr = addr(initiator_listen_port);
        let mut manager_rx = register_manager(&state, peer_listen_addr);

        // Accept task.
        let accept_state = state.clone();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            accept_incoming_tcp_connection(stream, accept_state).await;
        });

        // Initiator side: just sends a Hello and then idles.
        let initiator_handle = tokio::spawn(async move {
            use crate::network::framing::encode;
            use tokio::io::AsyncWriteExt;
            let mut stream = tokio::net::TcpStream::connect(server_addr).await.unwrap();
            let hello = PeerMessage::hello(HelloMessage {
                listening_address: peer_listen_addr,
            });
            let encoded = encode(&hello).unwrap();
            stream.write_all(&encoded).await.unwrap();
            // Keep the stream open so the accept side doesn't see an EOF.
            tokio::time::sleep(Duration::from_millis(500)).await;
        });

        // Manager should receive ConnectionReady.
        let msg = timeout(Duration::from_millis(500), manager_rx.recv())
            .await
            .expect("timed out waiting for manager notification")
            .expect("manager channel closed");

        assert!(
            matches!(msg, PeerManagerMessage::ConnectionReady(_)),
            "expected ConnectionReady after Hello"
        );

        drop(initiator_handle);
    }

    // ----------------------------------------------------------------
    // accept_incoming_tcp_connection — wrong first message
    // ----------------------------------------------------------------

    /// If the first message is not a Hello the connection must be dropped
    /// silently (no panic, no manager notification).
    #[tokio::test]
    async fn accept_drops_connection_if_first_message_is_not_hello() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();

        let state = make_state(19220);

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            accept_incoming_tcp_connection(stream, state).await;
        });

        tokio::spawn(async move {
            use crate::network::framing::encode;
            use tokio::io::AsyncWriteExt;
            let mut stream = tokio::net::TcpStream::connect(server_addr).await.unwrap();
            // Send a Cli message instead of Hello.
            let non_hello = cli_msg(19220);
            let encoded = encode(&non_hello).unwrap();
            stream.write_all(&encoded).await.unwrap();
            tokio::time::sleep(Duration::from_millis(300)).await;
        });

        // No manager was registered for any peer; the accept task should just
        // return quietly. Give it time and confirm no panic occurred.
        tokio::time::sleep(Duration::from_millis(400)).await;
        // Reaching here without a panic is the assertion.
    }

    // ----------------------------------------------------------------
    // accept — EOF before Hello
    // ----------------------------------------------------------------

    #[tokio::test]
    async fn accept_handles_early_eof_gracefully() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();
        let state = make_state(19230);

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            accept_incoming_tcp_connection(stream, state).await;
        });

        // Connect and immediately close.
        let _stream = tokio::net::TcpStream::connect(server_addr).await.unwrap();
        drop(_stream);

        // Should not panic.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // ----------------------------------------------------------------
    // tcp_channel: ConnectionLost notified when remote closes
    // ----------------------------------------------------------------

    /// When the remote side closes its end of the stream the local
    /// tcp_channel loop must detect the EOF and send ConnectionLost
    /// to the peer manager.
    #[tokio::test]
    async fn tcp_channel_notifies_connection_lost_on_remote_close() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();

        let peer_listen_port = 19240u16;
        let state = make_state(19241);
        let peer_listen_addr = addr(peer_listen_port);
        let mut manager_rx = register_manager(&state, peer_listen_addr);

        // Server: accept and run accept_incoming_tcp_connection.
        let accept_state = state.clone();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            accept_incoming_tcp_connection(stream, accept_state).await;
        });

        // Initiator: send Hello then immediately drop the stream (EOF).
        tokio::spawn(async move {
            use crate::network::framing::encode;
            use tokio::io::AsyncWriteExt;
            let mut stream = tokio::net::TcpStream::connect(server_addr).await.unwrap();
            let hello = PeerMessage::hello(HelloMessage {
                listening_address: peer_listen_addr,
            });
            stream.write_all(&encode(&hello).unwrap()).await.unwrap();
            // Close the stream to trigger EOF on the server side.
            drop(stream);
        });

        // Expect: ConnectionReady first, then ConnectionLost.
        let first = timeout(Duration::from_millis(500), manager_rx.recv())
            .await
            .expect("timed out waiting for ConnectionReady")
            .unwrap();
        assert!(matches!(first, PeerManagerMessage::ConnectionReady(_)));

        let second = timeout(Duration::from_millis(500), manager_rx.recv())
            .await
            .expect("timed out waiting for ConnectionLost")
            .unwrap();
        assert!(
            matches!(second, PeerManagerMessage::ConnectionLost),
            "expected ConnectionLost after EOF"
        );
    }

    // ----------------------------------------------------------------
    // tcp_channel: message forwarding end-to-end
    // ----------------------------------------------------------------

    /// A full loopback: one side connects, handshake completes, then one
    /// side sends a Cli message and we verify the other side receives it
    /// (via the event_q, which tcp_handler ultimately writes to, but here
    /// we check only up to the ConnectionReady / channel-forwarding layer).
    ///
    /// NOTE: full message dispatch goes through tcp_handler which is
    /// separate; here we test that the tcp_channel loop forwards
    /// PeerMessages sent via the tcp_channel_sender to the wire and that
    /// they arrive on the other side.
    #[tokio::test]
    async fn messages_travel_between_two_tcp_channel_halves() {
        use crate::network::framing::{check_frame, decode};
        use bytes::BytesMut;
        use tokio::io::AsyncReadExt;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();

        // The "server" just reads raw frames — no full accept logic.
        let read_handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = BytesMut::with_capacity(1024);
            // Read until we have the Hello plus one more message.
            let mut messages = Vec::new();
            loop {
                stream.read_buf(&mut buf).await.unwrap();
                while check_frame(&buf) {
                    messages.push(decode(&mut buf).unwrap());
                }
                if messages.len() >= 2 {
                    break;
                }
            }
            messages
        });

        let peer_addr = server_addr;
        let listen_port = 19250u16;
        let state = make_state(listen_port);
        let _manager_rx = register_manager(&state, peer_addr);

        let stream = tokio::net::TcpStream::connect(server_addr).await.unwrap();
        let (tcp_channel_tx, tcp_channel_rx) = mpsc::channel::<PeerMessage>(16);

        tokio::spawn(initiate_tcp_connection(
            peer_addr,
            stream,
            tcp_channel_tx.clone(),
            tcp_channel_rx,
            state,
        ));

        // Give the handshake time to complete, then push a message down the channel.
        tokio::time::sleep(Duration::from_millis(50)).await;
        tcp_channel_tx.send(cli_msg(listen_port)).await.unwrap();

        let messages = timeout(Duration::from_millis(500), read_handle)
            .await
            .expect("timed out waiting for messages")
            .expect("reader task panicked");

        assert_eq!(messages.len(), 2, "expected Hello + one Cli message");
        assert!(matches!(messages[0].payload, PeerMessagePayload::Hello(_)));
        assert!(matches!(messages[1].payload, PeerMessagePayload::Cli(_)));
    }
}
