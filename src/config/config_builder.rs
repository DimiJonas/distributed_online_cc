// crate::config::builder  — always compiled, usable from gen-config

use std::{collections::HashMap, net::SocketAddr, time::Duration};

#[cfg(test)]
use tokio::sync::mpsc;

use crate::{
    config::node_config::{
        ConfigMode, ConstraintConfig, GossipConfig, NodeConfig, OverlayConfig, ResultBufferConfig,
        TcpChannelConfig,
    },
    network::message_types,
    types::PeerId,
};
#[cfg(test)]
use crate::{
    state::app_state::AppState,
    types::{CaseEnd, TracedEvent},
    util::test_tools::{
        make_state_from_config, make_state_with_config_and_event_q,
        make_state_with_config_and_event_q_and_local_end_q,
    },
};
pub struct NodeConfigBuilder {
    config: NodeConfig,
}

impl NodeConfigBuilder {
    pub fn distributed_base() -> Self {
        Self::from_base(make_base_config(ConfigMode::Distributed))
    }
    pub fn central_base() -> Self {
        Self::from_base(make_base_config(ConfigMode::Central))
    }
    pub fn new_base_config() -> Self {
        Self::distributed_base()
    }
    pub fn from_base(config: NodeConfig) -> Self {
        Self { config }
    }

    // ---- mode switching ----
    pub fn as_central(mut self) -> Self {
        self.config.mode = ConfigMode::Central;
        self.config.overlay = None;
        self.config.gossip = None;
        self
    }
    pub fn as_distributed(mut self) -> Self {
        self.config.mode = ConfigMode::Distributed;
        self.config.overlay.get_or_insert_with(default_overlay);
        self.config.gossip.get_or_insert_with(default_gossip);
        self
    }

    // ---- private accessors ----
    fn overlay_mut(&mut self) -> &mut OverlayConfig {
        self.config
            .overlay
            .as_mut()
            .expect("overlay setter on a config without [overlay] — start from distributed_base()")
    }
    fn gossip_mut(&mut self) -> &mut GossipConfig {
        self.config.gossip.as_mut().expect(
            "gossip setter on a config without [case_end_gossip] — start from distributed_base()",
        )
    }

    // ---- core ----
    pub fn node_id(mut self, id: PeerId) -> Self {
        self.config.node_id = id;
        self
    }
    pub fn tcp_listen(mut self, a: SocketAddr) -> Self {
        self.config.tcp_listen_address = a;
        self
    }
    pub fn unix_socket(mut self, s: impl Into<String>) -> Self {
        self.config.unix_socket = s.into();
        self
    }
    pub fn log_file(mut self, s: impl Into<String>) -> Self {
        self.config.log_file = s.into();
        self
    }
    pub fn port(mut self, port: u16) -> Self {
        self.config.tcp_listen_address = format!("127.0.0.1:{port}").parse().unwrap();
        self
    }

    // ---- overlay ----
    pub fn overlay(mut self, o: OverlayConfig) -> Self {
        self.config.overlay = Some(o);
        self
    }
    pub fn overlay_listen(mut self, a: SocketAddr) -> Self {
        self.overlay_mut().listen_address = a;
        self
    }
    pub fn initial_peers(mut self, peers: Vec<SocketAddr>) -> Self {
        self.overlay_mut().initial_peers = peers;
        self
    }
    pub fn shuffle_interval(mut self, d: Duration) -> Self {
        self.overlay_mut().shuffle_interval = d;
        self
    }
    pub fn shuffle_length(mut self, n: usize) -> Self {
        self.overlay_mut().shuffle_length = n;
        self
    }
    pub fn max_peer_cache_size(mut self, n: usize) -> Self {
        self.overlay_mut().max_peer_cache_size = n;
        self
    }
    pub fn idle_timeout(mut self, d: Option<Duration>) -> Self {
        self.overlay_mut().idle_timeout = d;
        self
    }

    // ---- gossip ----
    pub fn gossip(mut self, g: GossipConfig) -> Self {
        self.config.gossip = Some(g);
        self
    }
    pub fn anti_entropy_interval(mut self, d: Duration) -> Self {
        self.gossip_mut().anti_entropy_interval = d;
        self
    }
    pub fn anti_entropy_fanout(mut self, n: usize) -> Self {
        self.gossip_mut().anti_entropy_fanout = n;
        self
    }
    pub fn push_fanout(mut self, n: usize) -> Self {
        self.gossip_mut().push_fanout = n;
        self
    }
    pub fn gossip_inbound_mpsc_channel_size(mut self, n: usize) -> Self {
        self.gossip_mut().gossip_inbound_mpsc_channel_size = n;
        self
    }
    pub fn gossip_outbound_mpsc_channel_size(mut self, n: usize) -> Self {
        self.gossip_mut().gossip_outbound_mpsc_channel_size = n;
        self
    }

    // ---- constraint layer ----
    pub fn constraints(mut self, c: Vec<ConstraintConfig>) -> Self {
        self.config.constraints = c;
        self
    }
    pub fn constraint(mut self, c: ConstraintConfig) -> Self {
        self.config.constraints.push(c);
        self
    }
    pub fn case_end_grace_period(mut self, p: Duration) -> Self {
        self.config.case_end_grace_period = p;
        self
    }
    pub fn case_end_retention_period(mut self, p: Duration) -> Self {
        self.config.case_end_retention_period = p;
        self
    }
    pub fn default_buffer(mut self, b: ResultBufferConfig) -> Self {
        self.config.default_result_buffer = b;
        self
    }
    pub fn buffer(mut self, id: impl Into<String>, b: ResultBufferConfig) -> Self {
        self.config.result_buffers.insert(id.into(), b);
        self
    }

    pub fn build(self) -> NodeConfig {
        self.config.validate();
        self.config
    }
}

fn default_overlay() -> OverlayConfig {
    OverlayConfig {
        initial_peers: vec![],
        listen_address: "127.0.0.1:10000".parse().unwrap(),
        max_peer_cache_size: 8,
        shuffle_length: 4,
        shuffle_interval: Duration::from_mins(10),
        idle_timeout: None,
    }
}

fn default_gossip() -> GossipConfig {
    GossipConfig {
        anti_entropy_interval: Duration::from_mins(5),
        anti_entropy_fanout: 1,
        push_fanout: 4,
        gossip_inbound_mpsc_channel_size: 1024,
        gossip_outbound_mpsc_channel_size: 64,
    }
}

pub fn make_base_config(mode: ConfigMode) -> NodeConfig {
    let (overlay, gossip) = match mode {
        ConfigMode::Distributed => (Some(default_overlay()), Some(default_gossip())),
        ConfigMode::Central => (None, None),
    };
    let config = NodeConfig {
        mode,
        node_id: 1,
        tcp_listen_address: "127.0.0.1:9000".parse().unwrap(),
        unix_socket: "/tmp/test.sock".to_string(),
        log_file: "test.log".to_string(),
        event_queue_size: 100,
        loopback_queue_size: 100,
        result_sink_addresses: None,
        result_sink: None,
        case_end_grace_period: Duration::from_secs(10),
        case_end_retention_period: Duration::from_secs(15),
        tcp_channels: TcpChannelConfig {
            tcp_channel_mpsc_channel_size: 100,
            peer_manager_mpsc_channel_size: 256,
            message_queue_size: 256,
            internal_message_ttl: Duration::from_secs(30),
            reconnect_start_delay: Duration::from_secs(1),
            reconnect_max_delay: Duration::from_secs(30),
            wait_for_response_timeout: Duration::from_secs(20),
            max_reconnect_attempts: None,
            connect_timeout: Duration::from_secs(10),
        },
        constraints: vec![],
        result_buffers: HashMap::new(),
        default_result_buffer: ResultBufferConfig::Immediate,
        overlay,
        gossip,
    };
    message_types::init(config.clone()); // see caveat above
    config
}

#[cfg(test)]
impl NodeConfigBuilder {
    pub fn build_state(self) -> AppState {
        make_state_from_config(self.config)
    }
    pub fn build_state_with_event_q(
        self,
    ) -> (
        AppState,
        mpsc::Sender<TracedEvent>,
        mpsc::Receiver<TracedEvent>,
    ) {
        let (event_tx, event_rx) = mpsc::channel(64);
        let state = make_state_with_config_and_event_q(self.config, event_tx.clone());
        (state, event_tx, event_rx)
    }

    #[allow(clippy::type_complexity)]
    pub fn build_state_with_event_q_and_local_end_q(
        self,
    ) -> (
        AppState,
        mpsc::Sender<TracedEvent>,
        mpsc::Receiver<TracedEvent>,
        mpsc::Sender<Vec<CaseEnd>>,
        mpsc::Receiver<Vec<CaseEnd>>,
    ) {
        let (event_tx, event_rx) = mpsc::channel(64);
        let (local_end_tx, local_end_rx) = mpsc::channel(64);
        let state = make_state_with_config_and_event_q_and_local_end_q(
            self.config,
            event_tx.clone(),
            local_end_tx.clone(),
        );
        (state, event_tx, event_rx, local_end_tx, local_end_rx)
    }
}
