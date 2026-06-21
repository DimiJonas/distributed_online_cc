use bytes::BytesMut;
use chrono::Utc;
use process_mining::core::event_data::case_centric::{
    XESEditableAttribute,
    xes::{XESImportOptions, import_xes_slice},
};
use tokio::sync::mpsc;

use crate::{
    config::node_config::{ConstraintConfig, NodeConfig, ResultBufferConfig, TcpChannelConfig},
    network::{
        message_types::{CliTestMessage, PeerMessage},
        peer_manager_task::{PeerManagerMessage, peer_manager_task},
    },
    state::app_state::AppState,
    types::{ConstraintType, Token, TracedEvent},
};
use std::{collections::HashMap, net::SocketAddr, time::Duration};

pub fn addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{}", port).parse().unwrap()
}

// TODO: create a builder for NodeConfig

/// Minimal NodeConfig that satisfies AppState::new without touching disk
fn make_base_config() -> NodeConfig {
    let config = NodeConfig {
        node_id: 1,
        tcp_listen_address: "127.0.0.1:9000".parse().unwrap(),
        unix_socket: "/tmp/test.sock".to_string(),
        log_file: "test.log".to_string(),
        message_ttl: Duration::from_secs(30),
        event_queue_size: 100,
        loopback_queue_size: 100,
        result_sink_addresses: None,
        result_sink: None,
        case_end_grace_period: Duration::from_secs(10),
        case_retention_period: Duration::from_secs(15),
        tcp_channels: TcpChannelConfig {
            tcp_channel_mpsc_channel_size: 100,
            peer_manager_mpsc_channel_size: 256,
            message_queue_size: 256,
            reconnect_start_delay: Duration::from_secs(1),
            reconnect_max_delay: Duration::from_secs(30),
            wait_for_response_timeout: Duration::from_secs(20),
        },
        constraints: vec![],
        result_buffers: HashMap::new(),
        default_result_buffer: ResultBufferConfig::Immediate,
    };
    crate::network::message_types::init(config.clone());
    config
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

pub fn make_config_with_constraints_and_port(
    constraints: Vec<ConstraintConfig>,
    listen_port: u16,
) -> NodeConfig {
    let mut config = make_base_config();
    config.constraints = constraints;
    config.tcp_listen_address = addr(listen_port);
    config
}

pub fn make_state_and_event_q(listen_port: u16) -> (AppState, mpsc::Receiver<TracedEvent>) {
    let (event_tx, event_rx) = mpsc::channel(16);
    let (loopback_tx, _loopback_rx) = mpsc::channel(16);
    let (sink_tx, _sink_rx) = mpsc::channel(16);
    let state = AppState::new(
        make_config_with_port(listen_port),
        event_tx,
        loopback_tx,
        sink_tx,
    );
    (state, event_rx)
}

pub fn make_state_with_event_q(event_tx: mpsc::Sender<TracedEvent>) -> AppState {
    let (loopback_tx, _loopback_rx) = mpsc::channel(16);
    let (sink_tx, _sink_rx) = mpsc::channel(16);
    AppState::new(make_base_config(), event_tx, loopback_tx, sink_tx)
}

pub fn make_state_with_config_and_event_q(
    config: NodeConfig,
    event_tx: mpsc::Sender<TracedEvent>,
) -> AppState {
    let (loopback_tx, _loopback_rx) = mpsc::channel(16);
    let (sink_tx, _sink_rx) = mpsc::channel(16);
    AppState::new(config, event_tx, loopback_tx, sink_tx)
}

pub fn make_state_from_config(config: NodeConfig) -> AppState {
    let (event_tx, _event_rx) = mpsc::channel(16);
    let (loopback_tx, _loopback_rx) = mpsc::channel(16);
    let (sink_tx, _sink_rx) = mpsc::channel(16);
    AppState::new(config, event_tx, loopback_tx, sink_tx)
}

pub fn make_state(listen_port: u16) -> AppState {
    let (event_tx, _event_rx) = mpsc::channel(16);
    let (loopback_tx, _loopback_rx) = mpsc::channel(16);
    let (sink_tx, _sink_rx) = mpsc::channel(16);
    AppState::new(
        make_config_with_port(listen_port),
        event_tx,
        loopback_tx,
        sink_tx,
    )
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
        message: "hello".to_string(),
        sender_addr: addr(port),
    })
}

pub fn make_traced_event(case_id: &str, event_name: &str) -> TracedEvent {
    let xes = format!(
        r#"<?xml version="1.0" encoding="UTF-8" ?>
<log>
  <trace>
    <string key="concept:name" value="{case_id}"/>
    <event>
      <string key="concept:name" value="{event_name}"/>
      <date key="time:timestamp" value="2024-01-01T00:00:00+00:00"/>
    </event>
  </trace>
</log>"#
    );
    let buf = BytesMut::from(xes.as_bytes());
    let opts = XESImportOptions {
        sort_events_with_timestamp_key: Some("time:timestamp".to_string()),
        verbose: false,
        ..XESImportOptions::default()
    };
    let log = import_xes_slice(&buf, false, opts).unwrap();
    let trace = log.traces.into_iter().next().unwrap();

    let case_id_val = trace
        .attributes
        .get_by_key("concept:name")
        .and_then(|a| a.value.try_as_string())
        .cloned()
        .unwrap_or_default();

    let event = trace.events.into_iter().next().unwrap();
    TracedEvent {
        case_id: case_id_val,
        event,
    }
}

pub fn minimal_xes(case_id: &str, event_name: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8" ?>
<log>
  <trace>
    <string key="concept:name" value="{case_id}"/>
    <event>
      <string key="concept:name" value="{event_name}"/>
      <date key="time:timestamp" value="2024-01-01T00:00:00+00:00"/>
    </event>
  </trace>
</log>"#
    )
}

pub fn make_token(case_id: &str, constraint_id: &str) -> Token {
    Token::new(
        case_id.to_string(),
        constraint_id.to_string(),
        ConstraintType::Response,
        Some(Utc::now()),
        1,
    )
}

pub fn make_timer_token(case_id: &str, constraint_id: &str) -> Token {
    Token::new(
        case_id.to_string(),
        constraint_id.to_string(),
        ConstraintType::Time,
        Some(Utc::now()),
        1,
    )
}

pub fn make_config_with_buffers(
    constraints: Vec<ConstraintConfig>,
    buffers: HashMap<String, ResultBufferConfig>,
    default_buffer: ResultBufferConfig,
) -> NodeConfig {
    let mut config = make_base_config();
    config.constraints = constraints;
    config.result_buffers = buffers;
    config.default_result_buffer = default_buffer;
    config
}

pub fn make_traced_event_at(case_id: &str, event_name: &str, seconds_offset: i64) -> TracedEvent {
    use chrono::TimeZone;
    let base = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
    let ts = base + chrono::Duration::seconds(seconds_offset);
    let ts_str = ts.to_rfc3339();
    let xes = format!(
        r#"<?xml version="1.0" encoding="UTF-8" ?>
<log>
  <trace>
    <string key="concept:name" value="{case_id}"/>
    <event>
      <string key="concept:name" value="{event_name}"/>
      <date key="time:timestamp" value="{ts_str}"/>
    </event>
  </trace>
</log>"#
    );
    let buf = BytesMut::from(xes.as_bytes());
    let opts = XESImportOptions {
        sort_events_with_timestamp_key: Some("time:timestamp".to_string()),
        verbose: false,
        ..XESImportOptions::default()
    };
    let log = import_xes_slice(&buf, false, opts).unwrap();
    let trace = log.traces.into_iter().next().unwrap();

    use XESEditableAttribute;
    let case_id_val = trace
        .attributes
        .get_by_key("concept:name")
        .and_then(|a| a.value.try_as_string())
        .cloned()
        .unwrap_or_default();

    let event = trace.events.into_iter().next().unwrap();
    TracedEvent {
        case_id: case_id_val,
        event,
    }
}
