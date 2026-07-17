use tokio::sync::mpsc;

use crate::{
    network::{
        message_types::{
            PeerMessage,
            PeerMessagePayload::{CEGossip, Cli, Conformance, Hello, Overlay, Result},
        },
        tcp_channel::dispatch_message,
    },
    state::app_state::AppState,
};

/// this task operates on the loopback queue
/// this queue decouples sending and receiving in the case of messages meant for the same host they are sent from
/// these messages dont go through the tcp stack, but are enqueued in the loopback q and then dispatched by this loopback queue processor task
pub fn spawn_loopback_q_processor(state: AppState, mut loopback_q: mpsc::Receiver<PeerMessage>) {
    tokio::spawn(async move {
        while let Some(msg) = loopback_q.recv().await {
            match msg.payload {
                Cli(_) | Conformance(_) | Result(_) => {
                    dispatch_message(msg, state.config.tcp_listen_address, state.clone()).await;
                }
                Overlay(_) | CEGossip(_) => {
                    dispatch_message(msg, state.config.overlay().listen_address, state.clone())
                        .await;
                }
                Hello(_) => {
                    log::error!(
                        "loopback queue: hello message in loopback queue. this should never happen"
                    );
                    continue;
                }
            }
        }
    });
}
