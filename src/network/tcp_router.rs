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
