use tokio::sync::mpsc;

use crate::{
    config::node_config::{ConstraintConfig, NodeConfig, TcpChannelConfig},
    network::{
        message_types::{CliTestMessage, PeerMessage},
        peer_manager_task::{PeerManagerMessage, peer_manager_task},
    },
    state::app_state::AppState,
    types::TracedEvent,
};
use std::net::SocketAddr;

pub fn addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{}", port).parse().unwrap()
}

/// Minimal NodeConfig that satisfies AppState::new without touching disk
fn make_base_config() -> NodeConfig {
    NodeConfig {
        node_id: 1,
        tcp_listen_address: "127.0.0.1:9000".parse().unwrap(),
        unix_socket: "/tmp/test.sock".to_string(),
        log_file: "test.log".to_string(),
        peers: vec![],
        event_queue_size: 100,
        tcp_channels: TcpChannelConfig {
            tcp_channel_mpsc_channel_size: 100,
            peer_manager_task_mpsc_channel_size: 256,
            message_queue_size: 256,
            reconnect_start_delay: 1,
            reconnect_max_delay: 30,
        },
        constraints: vec![],
    }
}

pub fn make_config_with_port(listen_port: u16) -> NodeConfig {
    let mut config = make_base_config();
    config.tcp_listen_address = addr(listen_port);
    config
}

pub fn make_config_with_constraints(constraints: Vec<ConstraintConfig>) -> NodeConfig {
    let mut config = make_base_config();
    config.constraints = constraints;
    config
}

pub fn make_state_with_event_q(listen_port: u16) -> (AppState, mpsc::Receiver<TracedEvent>) {
    let (event_tx, event_rx) = mpsc::channel(16);
    let state = AppState::new(make_config_with_port(listen_port), event_tx);
    (state, event_rx)
}

pub fn make_state(listen_port: u16) -> AppState {
    let (event_tx, _event_rx) = mpsc::channel(16);
    AppState::new(make_config_with_port(listen_port), event_tx)
}

pub fn spawn_manager(peer_addr: SocketAddr, state: AppState) -> mpsc::Sender<PeerManagerMessage> {
    let (tx, rx) = mpsc::channel(64);
    tokio::spawn(peer_manager_task(peer_addr, rx, state));
    tx
}

/// Register a peer manager in state and return a receiver so the test can
/// observe what the manager gets told.
pub fn register_manager(state: &AppState, peer: SocketAddr) -> mpsc::Receiver<PeerManagerMessage> {
    let (tx, rx) = mpsc::channel(64);
    state.peer_managers.lock().unwrap().insert(peer, tx);
    rx
}

pub fn cli_msg(port: u16) -> PeerMessage {
    PeerMessage::cli(CliTestMessage {
        request_id: 1,
        message: "hello".to_string(),
        sender_addr: addr(port),
    })
}
