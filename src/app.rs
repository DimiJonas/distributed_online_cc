use crate::network::peer::peer_task;
use crate::state::app_state::AppState;
use crate::state::peer_manager::{PeerHandle, PeerManager};
use crate::types::PeerId;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

fn get_address_from_id(id: PeerId) -> String {
    format!("127.0.0.1:{}", 7000 + id as u16)
}

fn get_id_from_address(addr: &std::net::SocketAddr) -> PeerId {
    (addr.port() - 7000) as PeerId
}

pub fn spawn_listener(
    peers: Arc<Mutex<PeerManager>>,
    pending: Arc<Mutex<PendingRequestManager>>,
    host_address: String,
) {
    tokio::spawn(async move {
        let listener = TcpListener::bind(&host_address).await.unwrap();
        println!("listening for peer messages on {}", host_address);
        loop {
            let (stream, addr) = listener.accept().await.unwrap();
            println!("new peer connection from {}", addr);

            let peer_id = get_id_from_address(&addr);

            let (tx, rx) = mpsc::channel(100);
            peers
                .lock()
                .await
                .insert(peer_id, PeerHandle::new(tx.clone()));
            tokio::spawn(peer_task(
                peer_id,
                stream,
                rx,
                peers.clone(),
                pending.clone(),
            ));
        }
    });
}

pub fn spawn_listener2(state: AppState, host_address: String) {
    tokio::spawn(async move {
        let listener = TcpListener::bind(&host_address).await.unwrap();
        println!("listening on {}", host_address);

        loop {
            let (stream, addr) = listener.accept().await.unwrap();
            println!("new connection from {}", addr);

            let peer_id = (addr.port() - 7000) as PeerId;

            let (tx, rx) = mpsc::channel(100);

            {
                let mut peers = state.peers.lock().await;
                peers.insert(peer_id, PeerHandle::new(tx.clone()));
            }

            tokio::spawn(peer_task(peer_id, stream, rx, state.clone()));
        }
    });
}
