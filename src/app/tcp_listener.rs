use crate::network::peer::peer_task;
use crate::state::app_state::AppState;
use crate::state::peer_manager::PeerHandle;
use crate::types::PeerId;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

fn get_id_from_address(addr: &std::net::SocketAddr) -> PeerId {
    (addr.port() - 7000) as PeerId
}

pub fn spawn_listener(state: AppState, host_address: String) {
    tokio::spawn(async move {
        let listener = TcpListener::bind(&host_address).await.unwrap();
        println!("listening for peer messages on {}", host_address);

        loop {
            let (stream, addr) = listener.accept().await.unwrap();
            println!("new peer connection from {}", addr);
            let peer_id = get_id_from_address(&addr);
            let (tx, rx) = mpsc::channel(100);

            // drop lock before spawning tasks
            {
                let mut peers = state.peers.lock().unwrap();
                peers.insert(peer_id, PeerHandle::new(tx.clone()));
            }

            tokio::spawn(peer_task(peer_id, stream, rx, state.clone()));
        }
    });
}
