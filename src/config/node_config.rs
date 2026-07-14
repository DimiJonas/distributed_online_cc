use crate::state::result_send_buffer::ResultBufferPolicy;
use crate::types::{ConstraintId, ConstraintType, EventId, PeerId};
use serde::{Deserialize, Serialize, de};
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::fs;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NodeConfig {
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
    pub constraints: Vec<ConstraintConfig>,
    #[serde(default)]
    pub tcp_channels: TcpChannelConfig,
    #[serde(default)]
    pub result_buffers: HashMap<ConstraintId, ResultBufferConfig>,
    #[serde(default)]
    pub default_result_buffer: ResultBufferConfig,
    pub result_sink_addresses: Option<Vec<SocketAddr>>,
    pub result_sink: Option<ResultSinkConfig>,
    #[serde(with = "humantime_serde", default = "default_case_end_grace_period")]
    pub case_end_grace_period: Duration,
    pub overlay: OverlayConfig,
    #[serde(default)]
    pub case_end_gossip: CEGossipConfig,
}

impl NodeConfig {
    pub fn load_config(path: &str) -> Self {
        let contents = fs::read_to_string(path).expect("could not read config file");
        let config: NodeConfig = toml::from_str(&contents).expect("could not parse config file");
        crate::network::message_types::init(config.clone());
        config
    }

    pub fn build_constraint_list(&self) -> Vec<(ConstraintId, ConstraintType)> {
        self.constraints
            .iter()
            .map(|c| {
                (
                    c.constraint_id().to_string().to_lowercase(),
                    c.constraint_type(),
                )
            })
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
                Some(attribute_key.clone().to_lowercase())
            } else {
                None
            }
        }));
        attributes
    }

    pub fn build_event_list(&self) -> HashSet<EventId> {
        self.constraints
            .iter()
            .map(|c| c.event_id().to_string().to_lowercase())
            .collect()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
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
pub struct ResultSinkConfig {
    pub db_path: String,
    #[serde(default = "default_result_sink_mpsc_channel_size")]
    pub mpsc_channel_size: usize,
    #[serde(default = "default_retry_interval", with = "humantime_serde")]
    pub retry_interval: Duration,
    #[serde(default = "default_retry_buffer_size")]
    pub retry_buffer_size: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
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
pub struct CEGossipConfig {
    #[serde(with = "humantime_serde", default = "default_anti_entropy_interval")]
    pub anti_entropy_interval: Duration,
    #[serde(with = "humantime_serde", default = "default_case_end_propagation_ttl")]
    pub case_end_propagation_ttl: Duration,
    #[serde(default = "default_fanout")]
    pub fanout: usize,
    #[serde(default = "default_local_end_signal_mpsc_channel_size")]
    pub local_end_signal_mpsc_channel_size: usize,
    #[serde(default = "default_gossip_message_queue_size")]
    pub gossip_message_queue_size: usize,
}

impl Default for CEGossipConfig {
    fn default() -> Self {
        Self {
            anti_entropy_interval: default_anti_entropy_interval(),
            case_end_propagation_ttl: default_case_end_propagation_ttl(),
            fanout: default_fanout(),
            gossip_message_queue_size: default_gossip_message_queue_size(),
            local_end_signal_mpsc_channel_size: default_local_end_signal_mpsc_channel_size(),
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Clone)]
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum AppearanceCounterReset {
    Zero,
    Subtract,
    Keep,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum DistributedAppearanceCounterReset {
    Zero,
    Keep,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(tag = "policy")]
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
#[serde(tag = "constraint_role")]
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

fn default_case_end_propagation_ttl() -> Duration {
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

fn default_fanout() -> usize {
    4
}

fn default_local_end_signal_mpsc_channel_size() -> usize {
    64
}

fn default_gossip_message_queue_size() -> usize {
    1024
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
