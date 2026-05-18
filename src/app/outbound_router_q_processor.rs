/*
    this task receives messages fromthe tcp handlers and forwards them to the tcp channels

    this task is needed to decouple the tcp channels from the tcp handler methods
    without this task there would be a cycle since the tcp channel calls the tcp handler methods when receiving a message and some handler methods send messages via tcp channels again
*/
use crate::network::tcp_router::send_to_peer;
use crate::{network::message_types::PeerMessage, state::app_state::AppState};
use std::net::SocketAddr;
use tokio::sync::mpsc;

pub fn spawn_outbound_router_processor(
    mut outbound_router_q_rx: mpsc::Receiver<(SocketAddr, PeerMessage)>,
    state: AppState,
) {
    tokio::spawn(async move {
        while let Some((dest, msg)) = outbound_router_q_rx.recv().await {
            send_to_peer(dest, msg, state.clone()).await;
        }
        log::warn!("outbound router channel closed, router task exiting");
    });
}
