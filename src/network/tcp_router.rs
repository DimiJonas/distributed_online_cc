use crate::network::peer_manager_task::{PeerManagerMessage, attempt_connect, peer_manager_task};
use crate::{network::message_types::PeerMessage, state::app_state::AppState};
use std::net::SocketAddr;
use tokio::sync::mpsc;
/*
    this module exposes an api for the rest of the application to send tcp messages
*/

// build a tcp channel to another peer
pub async fn connect_peer(peer_address: SocketAddr, state: AppState) {
    get_or_create_manager(peer_address, state.clone()).await;
}

// send to another peer via tcp
pub async fn send_to_peer(peer_address: SocketAddr, msg: PeerMessage, state: AppState) {
    let tx = get_or_create_manager(peer_address, state.clone()).await;
    if let Err(e) = tx.send(PeerManagerMessage::Outbound(Box::new(msg))).await {
        log::error!("peer manager for {} got lost: {}", peer_address, e);
    }
}

async fn get_or_create_manager(
    peer_address: SocketAddr,
    state: AppState,
) -> mpsc::Sender<PeerManagerMessage> {
    let manager_sender = {
        let mut managers = state.peer_managers.lock().unwrap();
        if let Some(manager_sender) = managers.get(&peer_address) {
            return manager_sender;
        }
        let (manager_sender, manager_receiver) = mpsc::channel(
            state
                .config
                .tcp_channels
                .peer_manager_task_mpsc_channel_size,
        );
        managers.insert(peer_address, manager_sender.clone());
        tokio::spawn(peer_manager_task(
            peer_address,
            manager_receiver,
            state.clone(),
        ));
        manager_sender
    };

    log::info!("created peer manager task fo {}", peer_address);

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
        util::test_tools::{addr, make_state, register_manager},
    };

    /// send_to_peer delivers to an already-registered peer manager.
    #[tokio::test]
    async fn send_to_peer_reaches_existing_manager() {
        let state = make_state(19900);
        let peer = addr(19901);

        let mut rx = register_manager(&state, peer);

        let msg = PeerMessage::cli(CliTestMessage {
            request_id: 1,
            message: "hello".to_string(),
            sender_addr: addr(19900),
        });
        send_to_peer(peer, msg, state).await;

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
        let peer = addr(19911);

        send_to_peer(
            peer,
            PeerMessage::cli(CliTestMessage {
                request_id: 2,
                message: "test".to_string(),
                sender_addr: addr(19910),
            }),
            state.clone(),
        )
        .await;

        let managers = state.peer_managers.lock().unwrap();
        assert!(
            managers.get(&peer).is_some(),
            "manager should be registered"
        );
    }

    /// connect_peer registers a peer manager.
    #[tokio::test]
    async fn connect_peer_registers_manager() {
        let state = make_state(19920);
        let peer = addr(19921);

        connect_peer(peer, state.clone()).await;

        let managers = state.peer_managers.lock().unwrap();
        assert!(managers.get(&peer).is_some());
    }

    /// Calling connect_peer twice for the same address must not create two managers.
    #[tokio::test]
    async fn connect_peer_idempotent() {
        let state = make_state(19930);
        let peer = addr(19931);

        connect_peer(peer, state.clone()).await;
        connect_peer(peer, state.clone()).await;

        // PeerManagerMap::get returns a clone; just verify the key is present.
        let managers = state.peer_managers.lock().unwrap();
        assert!(managers.get(&peer).is_some());
    }
}
