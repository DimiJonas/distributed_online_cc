use crate::state::result_send_buffer::ResultBufferPolicy;
use crate::types::{ConstraintId, ConstraintType, EventId, PeerId};
use serde::{Deserialize, Serialize, de};
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::fs;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    pub mode: ConfigMode,
    pub node_id: PeerId,
    pub tcp_listen_address: SocketAddr,
    #[serde(default = "default_unix_socket")]
    pub unix_socket: String,
    #[serde(default = "default_log_file")]
    pub log_file: String,
    #[serde(default = "default_event_q_size")]
    pub event_queue_size: usize,
    #[serde(default = "default_loopback_q_size")]
    pub loopback_queue_size: usize,
    #[serde(with = "humantime_serde", default = "default_case_end_grace_period")]
    pub case_end_grace_period: Duration,
    /// how long a case end keeps being gossiped
    /// also used to derive how long a closed case is kept in memory
    /// (should be sized against gossip convergence in decentralized system)
    #[serde(with = "humantime_serde", default = "default_case_end_ttl")]
    pub case_end_ttl: Duration,
    pub result_sink_addresses: Option<Vec<SocketAddr>>,
    #[serde(default)]
    pub result_sink: Option<ResultSinkConfig>,
    #[serde(default)]
    pub constraints: Vec<ConstraintConfig>,
    #[serde(default)]
    pub tcp_channels: TcpChannelConfig,
    #[serde(default)]
    pub overlay: Option<OverlayConfig>,
    #[serde(default)]
    pub gossip: Option<GossipConfig>,
    #[serde(default)]
    pub forwarder: Option<ForwarderConfig>,
    #[serde(default)]
    pub default_result_buffer: ResultBufferConfig,
    #[serde(default)]
    pub result_buffers: HashMap<ConstraintId, ResultBufferConfig>,
    #[serde(
        with = "humantime_serde",
        default = "default_case_janitor_cleanup_interval"
    )]
    pub case_janitor_cleanup_interval: Duration,
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ConfigMode {
    #[default]
    Decentralized,
    Central,
}

impl NodeConfig {
    pub fn load_config(path: &str) -> Self {
        let contents = fs::read_to_string(path).expect("could not read config file");
        let config: NodeConfig = toml::from_str(&contents).expect("could not parse config file");
        config.validate();
        crate::network::message_types::init(config.clone());
        config
    }

    pub fn validate(&self) {
        match self.mode {
            ConfigMode::Decentralized => {
                assert!(
                    self.overlay.is_some(),
                    "decentralized mode requires [overlay]"
                );
                assert!(
                    self.gossip.is_some(),
                    "decentralized mode requires [gossip]"
                );
            }
            ConfigMode::Central => {
                assert!(
                    self.overlay.is_none(),
                    "central mode must not configure [overlay]"
                );
                assert!(
                    self.gossip.is_none(),
                    "central mode must not configure [gossip]"
                );
            }
        }
    }

    pub fn overlay(&self) -> &OverlayConfig {
        self.overlay.as_ref().expect("validated at load")
    }

    pub fn gossip(&self) -> &GossipConfig {
        self.gossip.as_ref().expect("validated at load")
    }

    pub fn is_decentralized(&self) -> bool {
        self.mode == ConfigMode::Decentralized
    }

    pub fn is_own_address(&self, addr: SocketAddr) -> bool {
        addr == self.tcp_listen_address
            || self
                .overlay
                .as_ref()
                .is_some_and(|overlay| overlay.listen_address == addr)
    }

    pub fn build_constraint_list(&self) -> Vec<(ConstraintId, ConstraintType)> {
        self.constraints
            .iter()
            .map(|c| (c.constraint_id().to_string(), c.constraint_type()))
            .collect()
    }

    pub fn result_buffer_policy(&self, constraint_id: &str) -> ResultBufferPolicy {
        self.result_buffers
            .get(constraint_id)
            .unwrap_or(&self.default_result_buffer)
            .to_policy()
    }

    pub fn build_event_attribute_list(&self) -> HashSet<String> {
        let mut attributes =
            HashSet::from(["concept:name".to_string(), "time:timestamp".to_string()]);
        attributes.extend(self.constraints.iter().filter_map(|c| {
            if let ConstraintConfig::SensorCollector { attribute_key, .. } = c {
                Some(attribute_key.clone())
            } else {
                None
            }
        }));
        if let Some(forwarder_config) = &self.forwarder
            && let Some(attribute_list) = &forwarder_config.include_event_attributes
        {
            attributes.extend(attribute_list.iter().cloned())
        }
        attributes
    }

    pub fn build_event_list(&self) -> HashSet<EventId> {
        let mut events: HashSet<EventId> = self
            .constraints
            .iter()
            .map(|c| c.event_id().to_string())
            .collect();
        if let Some(forwarder_config) = &self.forwarder
            && let Some(event_list) = &forwarder_config.include_event_ids
        {
            events.extend(event_list.iter().cloned());
        }
        events
    }

    /// how long this node remembers having closed a case
    pub fn case_end_tombstone_ttl(&self) -> Duration {
        self.case_end_grace_period + 2 * self.case_end_ttl
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TcpChannelConfig {
    #[serde(default = "default_tcp_channel_mpsc_channel_size")]
    pub tcp_channel_mpsc_channel_size: usize,
    #[serde(default = "default_peer_manager_mpsc_channel_size")]
    pub peer_manager_mpsc_channel_size: usize,
    #[serde(default = "default_tcp_message_queue_size")]
    pub message_queue_size: usize,
    #[serde(with = "humantime_serde", default = "default_internal_message_ttl")]
    pub internal_message_ttl: Duration,
    #[serde(
        default = "default_tcp_reconnect_start_delay",
        with = "humantime_serde"
    )]
    pub reconnect_start_delay: Duration,
    #[serde(default = "default_tcp_reconnect_max_delay", with = "humantime_serde")]
    pub reconnect_max_delay: Duration,
    #[serde(
        default = "default_wait_for_response_timeout",
        with = "humantime_serde"
    )]
    pub wait_for_response_timeout: Duration,
    #[serde(default = "default_max_reconnect_attempts")]
    pub max_reconnect_attempts: Option<u32>,
    #[serde(default = "default_connect_timeout", with = "humantime_serde")]
    pub connect_timeout: Duration,
}

impl Default for TcpChannelConfig {
    fn default() -> Self {
        Self {
            tcp_channel_mpsc_channel_size: default_tcp_channel_mpsc_channel_size(),
            peer_manager_mpsc_channel_size: default_peer_manager_mpsc_channel_size(),
            message_queue_size: default_tcp_message_queue_size(),
            internal_message_ttl: default_internal_message_ttl(),
            reconnect_start_delay: default_tcp_reconnect_start_delay(),
            reconnect_max_delay: default_tcp_reconnect_max_delay(),
            wait_for_response_timeout: default_wait_for_response_timeout(),
            max_reconnect_attempts: default_max_reconnect_attempts(),
            connect_timeout: default_connect_timeout(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResultSinkConfig {
    pub db_path: String,
    #[serde(default = "default_result_sink_mpsc_channel_size")]
    pub mpsc_channel_size: usize,
    #[serde(default = "default_retry_interval", with = "humantime_serde")]
    pub retry_interval: Duration,
    #[serde(default = "default_retry_buffer_size")]
    pub retry_buffer_size: usize,
    /// how many results are written in one sqlite transaction
    #[serde(default = "default_result_sink_max_batch_size")]
    pub max_batch_size: usize,
    /// how long a partially filled batch waits before it is written anyway
    #[serde(
        default = "default_result_sink_flush_interval",
        with = "humantime_serde"
    )]
    pub flush_interval: Duration,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OverlayConfig {
    pub listen_address: SocketAddr,
    pub initial_peers: Vec<SocketAddr>,
    #[serde(default = "default_shuffle_interval", with = "humantime_serde")]
    pub shuffle_interval: Duration,
    #[serde(default = "default_shuffle_length")]
    pub shuffle_length: usize,
    #[serde(default = "default_max_peer_cache_size")]
    pub max_peer_cache_size: usize,
    #[serde(default = "default_idle_timeout", with = "humantime_serde")]
    pub idle_timeout: Option<Duration>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GossipConfig {
    #[serde(with = "humantime_serde", default = "default_anti_entropy_interval")]
    pub anti_entropy_interval: Duration,
    #[serde(default = "default_anti_entropy_fanout")]
    pub anti_entropy_fanout: usize,
    #[serde(default = "default_push_fanout")]
    pub push_fanout: usize,
    #[serde(default = "default_gossip_outbound_mpsc_channel_size")]
    pub gossip_outbound_mpsc_channel_size: usize,
    #[serde(default = "default_gossip_inbound_mpsc_channel_size")]
    pub gossip_inbound_mpsc_channel_size: usize,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            anti_entropy_interval: default_anti_entropy_interval(),
            anti_entropy_fanout: default_anti_entropy_fanout(),
            push_fanout: default_push_fanout(),
            gossip_inbound_mpsc_channel_size: default_gossip_inbound_mpsc_channel_size(),
            gossip_outbound_mpsc_channel_size: default_gossip_outbound_mpsc_channel_size(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ForwarderConfig {
    #[serde(default)]
    pub include_event_attributes: Option<Vec<String>>,
    #[serde(default)]
    pub include_event_ids: Option<Vec<EventId>>,
    pub central_node_address: SocketAddr,
}

#[derive(Debug, PartialEq, Clone)]
pub enum SensorConstraintMode {
    Lt,
    Gt,
    Lte,
    Gte,
    Eq,
}

impl SensorConstraintMode {
    pub fn evaluate_condition(&self, a: f64, b: f64) -> bool {
        match self {
            Self::Lt => a < b,
            Self::Gt => a > b,
            Self::Lte => a <= b,
            Self::Gte => a >= b,
            Self::Eq => a == b,
        }
    }
}

impl FromStr for SensorConstraintMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim() {
            "<=" => Ok(Self::Lte),
            "=<" => Ok(Self::Lte),
            ">=" => Ok(Self::Gte),
            "=>" => Ok(Self::Gte),
            "<" => Ok(Self::Lt),
            ">" => Ok(Self::Gt),
            "=" => Ok(Self::Eq),
            "==" => Ok(Self::Eq),
            other => Err(format!("Unknown operator: {other:?}")),
        }
    }
}

impl Serialize for SensorConstraintMode {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for SensorConstraintMode {
    fn deserialize<D: de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(de::Error::custom)
    }
}

impl Display for SensorConstraintMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Lt => write!(f, "<"),
            Self::Gt => write!(f, ">"),
            Self::Lte => write!(f, "<="),
            Self::Gte => write!(f, ">="),
            Self::Eq => write!(f, "=="),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum MinMaxMode {
    Min,
    Max,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum AppearanceCounterReset {
    Zero,
    Subtract,
    Keep,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
pub enum DistributedAppearanceCounterReset {
    Zero,
    Keep,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(tag = "policy", deny_unknown_fields)]
pub enum ResultBufferConfig {
    #[default]
    Immediate,
    GracePeriod {
        #[serde(with = "humantime_serde")]
        duration: Duration,
    },
    Revocable {
        #[serde(with = "humantime_serde")]
        duration: Duration,
    },
}

impl ResultBufferConfig {
    pub fn to_policy(&self) -> ResultBufferPolicy {
        match self {
            ResultBufferConfig::Immediate => ResultBufferPolicy::GracePeriod {
                duration: Duration::from_secs(0),
            },
            ResultBufferConfig::GracePeriod { duration } => ResultBufferPolicy::GracePeriod {
                duration: *duration,
            },
            ResultBufferConfig::Revocable { duration } => ResultBufferPolicy::Revocable {
                duration: *duration,
            },
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "constraint_role", deny_unknown_fields)]
pub enum ConstraintConfig {
    ResponseProducer {
        constraint_id: ConstraintId,
        event_id: EventId, // concept:name to watch for
    },
    ResponseConsumer {
        constraint_id: ConstraintId,
        event_id: EventId,
        // nodes holding the produced response token
        producer_peers: Vec<SocketAddr>,
    },
    EndActivity {
        constraint_id: ConstraintId,
        event_id: EventId,
    },
    PrecedenceProducer {
        constraint_id: ConstraintId,
        event_id: EventId,
        consumer_peers: Vec<SocketAddr>,
    },
    PrecedenceConsumer {
        constraint_id: ConstraintId,
        event_id: EventId,
    },
    OrderFirst {
        constraint_id: ConstraintId,
        event_id: EventId,
    },
    OrderSecond {
        constraint_id: ConstraintId,
        event_id: EventId,
        order_first_peers: Vec<SocketAddr>,
    },
    SensorCollector {
        constraint_id: ConstraintId,
        event_id: EventId,
        attribute_key: String,
        #[serde(deserialize_with = "deserialize_f64")]
        value: f64,
        mode: SensorConstraintMode,
        #[serde(with = "humantime_serde")]
        max_value_age: Duration,
    },
    SensorTrigger {
        constraint_id: ConstraintId,
        event_id: EventId,
        collector_peers: Vec<SocketAddr>,
    },
    TimeStart {
        constraint_id: ConstraintId,
        event_id: EventId,
        #[serde(with = "humantime_serde")]
        duration: Duration,
        mode: MinMaxMode,
    },
    TimeStop {
        constraint_id: ConstraintId,
        event_id: EventId,
        time_start_peers: Vec<SocketAddr>,
    },
    NotCoexistenceA {
        constraint_id: ConstraintId,
        event_id: EventId,
        b_peer: SocketAddr,
    },
    NotCoexistenceB {
        constraint_id: ConstraintId,
        event_id: EventId,
        a_peer: SocketAddr,
    },
    AppearanceCounter {
        constraint_id: ConstraintId,
        event_id: EventId,
        mode: MinMaxMode,
        frequency: u64,
        reset: AppearanceCounterReset,
    },
    AppearanceTrigger {
        constraint_id: ConstraintId,
        event_id: EventId,
        counter_peers: Vec<SocketAddr>,
    },
    DistributedAppearanceCounter {
        constraint_id: ConstraintId,
        event_id: EventId,
        reset: DistributedAppearanceCounterReset,
    },
    DistributedAppearanceTrigger {
        constraint_id: ConstraintId,
        event_id: EventId,
        mode: MinMaxMode,
        frequency: u64,
        counter_peers: Vec<SocketAddr>,
    },
}

impl ConstraintConfig {
    pub fn constraint_id(&self) -> &str {
        match self {
            ConstraintConfig::ResponseProducer { constraint_id, .. } => constraint_id,
            ConstraintConfig::ResponseConsumer { constraint_id, .. } => constraint_id,
            ConstraintConfig::EndActivity { constraint_id, .. } => constraint_id,
            ConstraintConfig::PrecedenceProducer { constraint_id, .. } => constraint_id,
            ConstraintConfig::PrecedenceConsumer { constraint_id, .. } => constraint_id,
            ConstraintConfig::OrderFirst { constraint_id, .. } => constraint_id,
            ConstraintConfig::OrderSecond { constraint_id, .. } => constraint_id,
            ConstraintConfig::SensorCollector { constraint_id, .. } => constraint_id,
            ConstraintConfig::SensorTrigger { constraint_id, .. } => constraint_id,
            ConstraintConfig::TimeStart { constraint_id, .. } => constraint_id,
            ConstraintConfig::TimeStop { constraint_id, .. } => constraint_id,
            ConstraintConfig::NotCoexistenceA { constraint_id, .. } => constraint_id,
            ConstraintConfig::NotCoexistenceB { constraint_id, .. } => constraint_id,
            ConstraintConfig::AppearanceCounter { constraint_id, .. } => constraint_id,
            ConstraintConfig::AppearanceTrigger { constraint_id, .. } => constraint_id,
            ConstraintConfig::DistributedAppearanceCounter { constraint_id, .. } => constraint_id,
            ConstraintConfig::DistributedAppearanceTrigger { constraint_id, .. } => constraint_id,
        }
    }

    pub fn constraint_type(&self) -> ConstraintType {
        match self {
            ConstraintConfig::ResponseProducer { .. } => ConstraintType::Response,
            ConstraintConfig::ResponseConsumer { .. } => ConstraintType::Response,
            ConstraintConfig::EndActivity { .. } => ConstraintType::EndActivity,
            ConstraintConfig::PrecedenceProducer { .. } => ConstraintType::Precedence,
            ConstraintConfig::PrecedenceConsumer { .. } => ConstraintType::Precedence,
            ConstraintConfig::OrderFirst { .. } => ConstraintType::Order,
            ConstraintConfig::OrderSecond { .. } => ConstraintType::Order,
            ConstraintConfig::SensorCollector { .. } => ConstraintType::Sensor,
            ConstraintConfig::SensorTrigger { .. } => ConstraintType::Sensor,
            ConstraintConfig::TimeStart { .. } => ConstraintType::Time,
            ConstraintConfig::TimeStop { .. } => ConstraintType::Time,
            ConstraintConfig::NotCoexistenceA { .. } => ConstraintType::NotCoexistence,
            ConstraintConfig::NotCoexistenceB { .. } => ConstraintType::NotCoexistence,
            ConstraintConfig::AppearanceCounter { .. } => ConstraintType::Appearance,
            ConstraintConfig::AppearanceTrigger { .. } => ConstraintType::Appearance,
            ConstraintConfig::DistributedAppearanceTrigger { .. } => {
                ConstraintType::DistributedAppearance
            }
            ConstraintConfig::DistributedAppearanceCounter { .. } => {
                ConstraintType::DistributedAppearance
            }
        }
    }

    pub fn event_id(&self) -> &str {
        match self {
            ConstraintConfig::ResponseProducer { event_id, .. } => event_id,
            ConstraintConfig::ResponseConsumer { event_id, .. } => event_id,
            ConstraintConfig::EndActivity { event_id, .. } => event_id,
            ConstraintConfig::PrecedenceProducer { event_id, .. } => event_id,
            ConstraintConfig::PrecedenceConsumer { event_id, .. } => event_id,
            ConstraintConfig::OrderFirst { event_id, .. } => event_id,
            ConstraintConfig::OrderSecond { event_id, .. } => event_id,
            ConstraintConfig::SensorCollector { event_id, .. } => event_id,
            ConstraintConfig::SensorTrigger { event_id, .. } => event_id,
            ConstraintConfig::TimeStart { event_id, .. } => event_id,
            ConstraintConfig::TimeStop { event_id, .. } => event_id,
            ConstraintConfig::NotCoexistenceA { event_id, .. } => event_id,
            ConstraintConfig::NotCoexistenceB { event_id, .. } => event_id,
            ConstraintConfig::AppearanceCounter { event_id, .. } => event_id,
            ConstraintConfig::AppearanceTrigger { event_id, .. } => event_id,
            ConstraintConfig::DistributedAppearanceCounter { event_id, .. } => event_id,
            ConstraintConfig::DistributedAppearanceTrigger { event_id, .. } => event_id,
        }
    }
}

fn default_result_sink_max_batch_size() -> usize {
    512
}

fn default_result_sink_flush_interval() -> Duration {
    Duration::from_millis(200)
}

fn default_unix_socket() -> String {
    "/tmp/event_listener.sock".to_string()
}

fn default_log_file() -> String {
    "app.log".to_string()
}

fn default_internal_message_ttl() -> Duration {
    Duration::from_secs(30)
}

fn default_event_q_size() -> usize {
    100
}

fn default_loopback_q_size() -> usize {
    100
}

fn default_tcp_channel_mpsc_channel_size() -> usize {
    100
}

fn default_tcp_message_queue_size() -> usize {
    256
}

fn default_max_reconnect_attempts() -> Option<u32> {
    None
}

fn default_tcp_reconnect_start_delay() -> Duration {
    Duration::from_secs(1)
}

fn default_tcp_reconnect_max_delay() -> Duration {
    Duration::from_secs(30)
}

fn default_peer_manager_mpsc_channel_size() -> usize {
    256
}

fn default_wait_for_response_timeout() -> Duration {
    Duration::from_secs(20)
}

fn default_connect_timeout() -> Duration {
    Duration::from_secs(10)
}

fn default_result_sink_mpsc_channel_size() -> usize {
    1024
}

fn default_retry_interval() -> Duration {
    Duration::from_secs(10)
}

fn default_retry_buffer_size() -> usize {
    4096
}

fn default_case_end_grace_period() -> Duration {
    Duration::from_secs(10)
}

fn default_case_end_ttl() -> Duration {
    Duration::from_secs(30)
}

fn default_shuffle_interval() -> Duration {
    Duration::from_mins(10)
}

fn default_shuffle_length() -> usize {
    4
}

fn default_max_peer_cache_size() -> usize {
    8
}

fn default_idle_timeout() -> Option<Duration> {
    Some(Duration::from_mins(2))
}

fn default_anti_entropy_interval() -> Duration {
    Duration::from_secs(5)
}

fn default_push_fanout() -> usize {
    4
}

fn default_gossip_outbound_mpsc_channel_size() -> usize {
    64
}

fn default_gossip_inbound_mpsc_channel_size() -> usize {
    1024
}

fn default_anti_entropy_fanout() -> usize {
    1
}

fn default_case_janitor_cleanup_interval() -> Duration {
    Duration::from_secs(1)
}

// deserialize a value that can be int or float
fn deserialize_f64<'de, D: de::Deserializer<'de>>(deserializer: D) -> Result<f64, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum IntOrFloat {
        Int(i64),
        Float(f64),
    }

    match IntOrFloat::deserialize(deserializer)? {
        IntOrFloat::Int(i) => Ok(i as f64),
        IntOrFloat::Float(f) => Ok(f),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constraint_id_matches_for_all_variants() {
        let rp = ConstraintConfig::ResponseProducer {
            constraint_id: "rp-id".to_string(),
            event_id: "e1".to_string(),
        };
        assert_eq!(rp.constraint_id(), "rp-id");

        let rc = ConstraintConfig::ResponseConsumer {
            constraint_id: "rc-id".to_string(),
            event_id: "e2".to_string(),
            producer_peers: vec![],
        };
        assert_eq!(rc.constraint_id(), "rc-id");

        let ea = ConstraintConfig::EndActivity {
            constraint_id: "ea-id".to_string(),
            event_id: "e3".to_string(),
        };
        assert_eq!(ea.constraint_id(), "ea-id");
    }

    #[test]
    fn constraint_type_response_for_producer_and_consumer() {
        let rp = ConstraintConfig::ResponseProducer {
            constraint_id: "x".to_string(),
            event_id: "e".to_string(),
        };
        assert!(matches!(rp.constraint_type(), ConstraintType::Response));

        let rc = ConstraintConfig::ResponseConsumer {
            constraint_id: "x".to_string(),
            event_id: "e".to_string(),
            producer_peers: vec![],
        };
        assert!(matches!(rc.constraint_type(), ConstraintType::Response));
    }

    #[test]
    fn constraint_type_end_activity_for_end_activity() {
        let ea = ConstraintConfig::EndActivity {
            constraint_id: "x".to_string(),
            event_id: "e".to_string(),
        };
        assert!(matches!(ea.constraint_type(), ConstraintType::EndActivity));
    }
}
