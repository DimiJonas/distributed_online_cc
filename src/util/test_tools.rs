use bytes::BytesMut;
use chrono::Utc;
use process_mining::core::event_data::case_centric::{
    XESEditableAttribute,
    xes::{XESImportOptions, import_xes_slice},
};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{
    config::{
        config_builder::make_base_config,
        node_config::{ConstraintConfig, NodeConfig, ResultBufferConfig},
    },
    network::{
        message_types::{CCMessage, CliTestMessage, PeerMessage},
        peer_manager_task::{PeerManagerMessage, peer_manager_task},
    },
    state::app_state::AppState,
    types::{CaseId, ConstraintType, EventTime, PeerAddress, Token, TokenPayload, TracedEvent},
};
use std::{collections::HashMap, net::SocketAddr};

pub fn addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{}", port).parse().unwrap()
}

pub fn peer_addr(port: u16) -> PeerAddress {
    PeerAddress::new_conformance_address(format!("127.0.0.1:{}", port).parse().unwrap())
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
    let (local_end_signal_tx, _local_end_signal_rx) = mpsc::channel(16);
    let (gossip_msg_q_tx, _gossip_msg_q_rx) = mpsc::channel(16);
    let state = AppState::new(
        make_config_with_port(listen_port),
        event_tx,
        loopback_tx,
        sink_tx,
        local_end_signal_tx,
        gossip_msg_q_tx,
    );
    (state, event_rx)
}

pub fn make_state_with_event_q(event_tx: mpsc::Sender<TracedEvent>) -> AppState {
    let (loopback_tx, _loopback_rx) = mpsc::channel(16);
    let (sink_tx, _sink_rx) = mpsc::channel(16);
    let (local_end_signal_tx, _local_end_signal_rx) = mpsc::channel(16);
    let (gossip_msg_q_tx, _gossip_msg_q_rx) = mpsc::channel(16);
    AppState::new(
        make_base_config(),
        event_tx,
        loopback_tx,
        sink_tx,
        local_end_signal_tx,
        gossip_msg_q_tx,
    )
}

pub fn make_state_with_config_and_event_q(
    config: NodeConfig,
    event_tx: mpsc::Sender<TracedEvent>,
) -> AppState {
    let (loopback_tx, _loopback_rx) = mpsc::channel(16);
    let (sink_tx, _sink_rx) = mpsc::channel(16);
    let (local_end_signal_tx, _local_end_signal_rx) = mpsc::channel(16);
    let (gossip_msg_q_tx, _gossip_msg_q_rx) = mpsc::channel(16);
    AppState::new(
        config,
        event_tx,
        loopback_tx,
        sink_tx,
        local_end_signal_tx,
        gossip_msg_q_tx,
    )
}

pub fn make_state_with_config_and_event_q_and_local_end_q(
    config: NodeConfig,
    event_tx: mpsc::Sender<TracedEvent>,
    local_end_tx: mpsc::Sender<(CaseId, EventTime)>,
) -> AppState {
    let (loopback_tx, _loopback_rx) = mpsc::channel(16);
    let (sink_tx, _sink_rx) = mpsc::channel(16);
    let (gossip_msg_q_tx, _gossip_msg_q_rx) = mpsc::channel(16);
    AppState::new(
        config,
        event_tx,
        loopback_tx,
        sink_tx,
        local_end_tx,
        gossip_msg_q_tx,
    )
}

pub fn make_state_from_config(config: NodeConfig) -> AppState {
    let (event_tx, _event_rx) = mpsc::channel(16);
    let (loopback_tx, _loopback_rx) = mpsc::channel(16);
    let (sink_tx, _sink_rx) = mpsc::channel(16);
    let (local_end_signal_tx, _local_end_signal_rx) = mpsc::channel(16);
    let (gossip_msg_q_tx, _gossip_msg_q_rx) = mpsc::channel(16);
    AppState::new(
        config,
        event_tx,
        loopback_tx,
        sink_tx,
        local_end_signal_tx,
        gossip_msg_q_tx,
    )
}

pub fn make_state(listen_port: u16) -> AppState {
    let (event_tx, _event_rx) = mpsc::channel(16);
    let (loopback_tx, _loopback_rx) = mpsc::channel(16);
    let (sink_tx, _sink_rx) = mpsc::channel(16);
    let (local_end_signal_tx, _local_end_signal_rx) = mpsc::channel(16);
    let (gossip_msg_q_tx, _gossip_msg_q_rx) = mpsc::channel(16);
    AppState::new(
        make_config_with_port(listen_port),
        event_tx,
        loopback_tx,
        sink_tx,
        local_end_signal_tx,
        gossip_msg_q_tx,
    )
}

pub fn spawn_manager(peer_addr: PeerAddress, state: AppState) -> mpsc::Sender<PeerManagerMessage> {
    let (tx, rx) = mpsc::channel(64);
    tokio::spawn(peer_manager_task(peer_addr, rx, state));
    tx
}

/// Register a peer manager in state and return a receiver so the test can
/// observe what the manager gets told.
pub fn register_manager(state: &AppState, peer: PeerAddress) -> mpsc::Receiver<PeerManagerMessage> {
    let (tx, rx) = mpsc::channel(64);
    state.peer_managers.lock().unwrap().insert(peer.address, tx);
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

pub fn make_traced_event_with_fixed_timestamp(case_id: &str, event_name: &str) -> TracedEvent {
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

/// Build a TracedEvent that carries a numeric sensor reading as a `<float>`
/// attribute, so the sensor collector handler can pick it up via
/// `get_attribute_by_key`. Timestamp is base (2024-01-01) + `seconds_offset`.
pub fn make_sensor_event(
    case_id: &str,
    event_name: &str,
    attribute_key: &str,
    value: f64,
    seconds_offset: i64,
) -> TracedEvent {
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
      <float key="{attribute_key}" value="{value}"/>
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

/// Build a `Token` carrying a `Counter` payload, used to answer pull-token
/// requests from a distributed-appearance trigger with a chosen count.
pub fn make_counter_token(case_id: &str, constraint_id: &str, count: u64) -> Token {
    Token::new_with_payload(
        case_id.to_string(),
        constraint_id.to_string(),
        ConstraintType::DistributedAppearance,
        None,
        1,
        TokenPayload::Counter(count),
    )
}

/// Mock peer: wait for exactly one outbound conformance request on
/// `manager_rx` (the receiver returned by `register_manager`), then resolve the
/// matching pending `send_and_receive` future with `response_cc`.
///
/// This lets handlers that negotiate over the network (not-coexistence event
/// path, distributed-appearance trigger) run to completion on a single-threaded
/// runtime without any real TCP.
pub async fn answer_one_conformance_request(
    state: AppState,
    manager_rx: &mut mpsc::Receiver<PeerManagerMessage>,
    response_cc: CCMessage,
) {
    let outbound = manager_rx
        .recv()
        .await
        .expect("expected one outbound message from handler");
    let request_id = match outbound {
        PeerManagerMessage::Outbound(boxed) => boxed
            .request_id
            .expect("outbound conformance request must carry a request_id"),
        _ => Uuid::new_v4(),
    };
    let response = PeerMessage::conformance_res(request_id, response_cc);
    let sender = {
        let mut pending = state.pending.lock().unwrap();
        pending
            .remove(&request_id)
            .expect("pending request must exist for the outbound request_id")
    };
    sender
        .send(response)
        .expect("send_and_receive receiver must still be waiting");
}
