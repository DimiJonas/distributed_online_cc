use crate::types::{
    ConstraintId, ConstraintType, EventId, PeerId, SensorConstraintMode, TimeConstraintMode,
};
use serde::{Deserialize, Serialize, de};
use std::fs;
use std::net::SocketAddr;
use std::time::Duration;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NodeConfig {
    pub node_id: PeerId,
    pub tcp_listen_address: SocketAddr,
    #[serde(default = "default_unix_socket")]
    pub unix_socket: String,
    #[serde(default = "default_log_file")]
    pub log_file: String,
    #[serde(with = "duration_secs", default = "default_message_ttl")]
    pub message_ttl: Duration,
    #[serde(default = "default_event_q_size")]
    pub event_queue_size: usize,
    pub constraints: Vec<ConstraintConfig>,
    #[serde(default)]
    pub tcp_channels: TcpChannelConfig,
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
            .map(|c| (c.constraint_id().to_string(), c.constraint_type()))
            .collect()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TcpChannelConfig {
    #[serde(default = "default_tcp_channel_mpsc_channel_size")]
    pub tcp_channel_mpsc_channel_size: usize,
    #[serde(default = "default_peer_manager_task_mpsc_channel_size")]
    pub peer_manager_task_mpsc_channel_size: usize,
    #[serde(default = "default_tcp_message_queue_size")]
    pub message_queue_size: usize,
    #[serde(default = "default_tcp_reconnect_start_delay")]
    pub reconnect_start_delay: u64,
    #[serde(default = "default_tcp_reconnect_max_delay")]
    pub reconnect_max_delay: u64,
}

impl Default for TcpChannelConfig {
    fn default() -> Self {
        Self {
            tcp_channel_mpsc_channel_size: default_tcp_channel_mpsc_channel_size(),
            peer_manager_task_mpsc_channel_size: default_peer_manager_task_mpsc_channel_size(),
            message_queue_size: default_tcp_message_queue_size(),
            reconnect_start_delay: default_tcp_reconnect_start_delay(),
            reconnect_max_delay: default_tcp_reconnect_max_delay(),
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
    },
    PrecedenceConsumer {
        constraint_id: ConstraintId,
        event_id: EventId,
        producer_peers: Vec<SocketAddr>,
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
    Sensor {
        constraint_id: ConstraintId,
        event_id: EventId,
        attribute_key: String,
        #[serde(deserialize_with = "deserialize_f64")]
        value: f64,
        mode: SensorConstraintMode,
    },
    Time {
        constraint_id: ConstraintId,
        event_id: EventId,
        #[serde(deserialize_with = "deserialize_humantime_duration")]
        duration: Duration,
        mode: TimeConstraintMode,
    },
    // TODO: add more
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
            ConstraintConfig::Sensor { constraint_id, .. } => constraint_id,
            ConstraintConfig::Time { constraint_id, .. } => constraint_id,
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
            ConstraintConfig::Sensor { .. } => ConstraintType::Sensor,
            ConstraintConfig::Time { .. } => ConstraintType::Time,
        }
    }
}

fn default_unix_socket() -> String {
    "/tmp/event_listener.sock".to_string()
}

fn default_log_file() -> String {
    "app.log".to_string()
}

fn default_message_ttl() -> Duration {
    Duration::from_secs(30)
}

fn default_event_q_size() -> usize {
    100
}

fn default_tcp_channel_mpsc_channel_size() -> usize {
    100
}

fn default_tcp_message_queue_size() -> usize {
    256
}

fn default_tcp_reconnect_start_delay() -> u64 {
    1
}

fn default_tcp_reconnect_max_delay() -> u64 {
    30
}

fn default_peer_manager_task_mpsc_channel_size() -> usize {
    256
}

// module used to (de-)serialize a duration value in seconds with serde
mod duration_secs {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u64(d.as_secs())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        let secs = u64::deserialize(d)?;
        Ok(Duration::from_secs(secs))
    }
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

fn deserialize_humantime_duration<'de, D: de::Deserializer<'de>>(
    deserializer: D,
) -> Result<Duration, D::Error> {
    let s = String::deserialize(deserializer)?;
    humantime::parse_duration(&s).map_err(de::Error::custom)
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
