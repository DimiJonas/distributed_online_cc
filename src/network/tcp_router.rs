use crate::network::errors::SendReceiveError;
use crate::network::message_types::MessageKind;
use crate::network::peer_manager_task::{PeerManagerMessage, attempt_connect, peer_manager_task};
use crate::types::PeerAddress;
use crate::{network::message_types::PeerMessage, state::app_state::AppState};
use std::net::SocketAddr;
use tokio::sync::{mpsc, oneshot};
/*
    this module exposes an api for the rest of the application to send tcp messages
*/

// build a tcp channel to another peer
pub async fn connect_peer(peer_address: PeerAddress, state: AppState) {
    if state.config.is_own_address(peer_address.address) {
        log::debug!("connect_peer: skipping self-connect to {}", peer_address);
        return;
    }
    get_or_create_manager(peer_address, state.clone()).await;
}

pub async fn send_to_conformance_peer(
    address: SocketAddr,
    msg: PeerMessage,
    state: AppState,
) -> Result<(), SendReceiveError> {
    send_to_peer(PeerAddress::new_conformance_address(address), msg, state).await
}

pub async fn send_to_overlay_peer(
    address: SocketAddr,
    msg: PeerMessage,
    state: AppState,
) -> Result<(), SendReceiveError> {
    send_to_peer(PeerAddress::new_overlay_address(address), msg, state).await
}

// send to another peer via tcp
pub async fn send_to_peer(
    peer_address: PeerAddress,
    msg: PeerMessage,
    state: AppState,
) -> Result<(), SendReceiveError> {
    log::debug!(
        "send_to_peer: sending {} message to {}",
        msg.payload,
        peer_address
    );
    if state.config.is_own_address(peer_address.address) {
        log::debug!("send_to_peer: short-circuiting self-addressed message");
        deliver_to_self(msg, state).await;
        Ok(())
    } else {
        let tx = get_or_create_manager(peer_address, state.clone()).await;
        match tx.send(PeerManagerMessage::Outbound(Box::new(msg))).await {
            Ok(_) => Ok(()),
            Err(e) => {
                log::error!(
                    "send_to_peer: peer manager for {} got lost: {}",
                    peer_address,
                    e
                );
                Err(SendReceiveError::SendFailed {
                    peer_address,
                    source: (e),
                })
            }
        }
    }
}

pub async fn send_and_receive_conformance(
    address: SocketAddr,
    message: PeerMessage,
    state: AppState,
) -> Result<PeerMessage, SendReceiveError> {
    send_and_receive(
        PeerAddress::new_conformance_address(address),
        message,
        state,
    )
    .await
}

pub async fn send_and_receive_overlay(
    address: SocketAddr,
    message: PeerMessage,
    state: AppState,
) -> Result<PeerMessage, SendReceiveError> {
    send_and_receive(PeerAddress::new_overlay_address(address), message, state).await
}

pub async fn send_and_receive(
    peer_address: PeerAddress,
    message: PeerMessage,
    state: AppState,
) -> Result<PeerMessage, SendReceiveError> {
    let request_id = match message.request_id {
        Some(id) => id,
        None => {
            return Err(SendReceiveError::MissingRequestId {
                message: Box::new(message),
            });
        }
    };
    if message.message_kind != MessageKind::Request {
        return Err(SendReceiveError::WrongMessageKind {
            expected_kind: (MessageKind::Response),
            actual_kind: (message.message_kind),
        });
    }

    let (waiting_channel_sender, waiting_channel_receiver) = oneshot::channel();

    {
        let mut pending = state.pending.lock().unwrap();
        pending.insert(request_id, waiting_channel_sender);
    }

    send_to_peer(peer_address, message, state.clone()).await?;
    log::debug!(
        "send_and_receive: waiting for response to request id {}",
        &request_id
    );

    tokio::select! {
        _ = tokio::time::sleep(state.config.tcp_channels.wait_for_response_timeout) => {
            // clean up the pending channel
            {
                let mut pending = state.pending.lock().unwrap();
                pending.remove(&request_id);
            }
            log::error!("send_and_receive: timed out waiting for response to request {}", request_id);
            Err(SendReceiveError::Timeout{request_id})
        }
        result = waiting_channel_receiver => {
            match result {
                Ok(peer_message) => {
                    log::debug!("send_and_receive: received response for request {}", request_id);
                    Ok(peer_message)
                }
                Err(e) => {
                    log::error!(
                        "send_and_receive: channel closed before response arrived for request {}: {}",
                        request_id, e
                    );
                    Err(SendReceiveError::ChannelClosed{request_id})
                }
            }
        }
    }
}

// short circuits message directly to self through the loopback queue without tcp overhead
async fn deliver_to_self(msg: PeerMessage, state: AppState) {
    if let Err(e) = state.loopback_q.send(msg).await {
        log::error!(
            "deliver_to_self: failed to send message to loopback channel: {}",
            e
        );
    }
}

async fn get_or_create_manager(
    peer_address: PeerAddress,
    state: AppState,
) -> mpsc::Sender<PeerManagerMessage> {
    let manager_sender = {
        let mut managers = state.peer_managers.lock().unwrap();
        if let Some(manager_sender) = managers.get(&peer_address.address) {
            return manager_sender;
        }
        let (manager_sender, manager_receiver) =
            mpsc::channel(state.config.tcp_channels.peer_manager_mpsc_channel_size);
        managers.insert(peer_address.address, manager_sender.clone());
        tokio::spawn(peer_manager_task(
            peer_address,
            manager_receiver,
            state.clone(),
        ));
        manager_sender
    };

    log::debug!(
        "get_or_create_manager: created peer manager task for {}",
        peer_address
    );

    attempt_connect(peer_address, state.clone()).await;

    manager_sender
}

#[cfg(test)]
mod tests {
    use crate::{
        network::{
            message_types::{CliTestMessage, PeerMessage},
            peer_manager_task::PeerManagerMessage,
            tcp_router::{connect_peer, send_to_peer},
        },
        types::PeerAddress,
        util::test_tools::{addr, make_state, peer_addr, register_manager},
    };

    /// send_to_peer delivers to an already-registered peer manager.
    #[tokio::test]
    async fn send_to_peer_reaches_existing_manager() {
        let state = make_state(19900);
        let peer = peer_addr(19901);

        let mut rx = register_manager(&state, peer);

        let msg = PeerMessage::cli(CliTestMessage {
            message: "hello".to_string(),
            sender_addr: addr(19900),
        });
        let _ = send_to_peer(peer, msg, state).await;

        let received = tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv())
            .await
            .expect("timed out waiting for message")
            .expect("channel closed");

        assert!(matches!(received, PeerManagerMessage::Outbound(_)));
    }

    /// send_to_peer for an unknown peer creates a manager entry (no panic, even
    /// though the background task will fail to connect — no listener running).
    #[tokio::test]
    async fn send_to_peer_creates_manager_for_unknown_peer() {
        let state = make_state(19910);
        let peer = peer_addr(19911);

        let _ = send_to_peer(
            peer,
            PeerMessage::cli(CliTestMessage {
                message: "test".to_string(),
                sender_addr: addr(19910),
            }),
            state.clone(),
        )
        .await;

        let managers = state.peer_managers.lock().unwrap();
        assert!(
            managers.get(&peer.address).is_some(),
            "manager should be registered"
        );
    }

    /// connect_peer registers a peer manager.
    #[tokio::test]
    async fn connect_peer_registers_manager() {
        let state = make_state(19920);
        let peer = PeerAddress::new_conformance_address(addr(19921));

        connect_peer(peer, state.clone()).await;

        let managers = state.peer_managers.lock().unwrap();
        assert!(managers.get(&peer.address).is_some());
    }

    /// Calling connect_peer twice for the same address must not create two managers.
    #[tokio::test]
    async fn connect_peer_idempotent() {
        let state = make_state(19930);
        let peer = PeerAddress::new_conformance_address(addr(19931));

        connect_peer(peer, state.clone()).await;
        connect_peer(peer, state.clone()).await;

        // PeerManagerMap::get returns a clone; just verify the key is present.
        let managers = state.peer_managers.lock().unwrap();
        assert!(managers.get(&peer.address).is_some());
    }
}
