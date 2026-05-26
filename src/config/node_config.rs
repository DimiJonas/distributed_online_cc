use crate::types::{ConstraintId, ConstraintType, PeerId};
use serde::{Deserialize, Serialize};
use std::fs;
use std::net::SocketAddr;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NodeConfig {
    pub node_id: PeerId,
    pub tcp_listen_address: SocketAddr,
    #[serde(default = "default_unix_socket")]
    pub unix_socket: String,
    #[serde(default = "default_log_file")]
    pub log_file: String,
    #[serde(default)]
    pub peers: Vec<String>,
    #[serde(default = "default_event_q_size")]
    pub event_queue_size: usize,
    pub constraints: Vec<ConstraintConfig>,
    #[serde(default)]
    pub tcp_channels: TcpChannelConfig,
}

impl NodeConfig {
    pub fn load_config(path: &str) -> Self {
        let contents = fs::read_to_string(path).expect("could not read config file");
        toml::from_str(&contents).expect("could not parse config file")
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
        event_id: String, // concept:name to watch for
    },
    ResponseConsumer {
        constraint_id: ConstraintId,
        event_id: String,
        // nodes holding the produced response token
        producer_peers: Vec<SocketAddr>,
    },
    EndActivity {
        constraint_id: ConstraintId,
        event_id: String,
    },
    PrecedenceProducer {
        constraint_id: ConstraintId,
        event_id: String,
    },
    PrecedenceConsumer {
        constraint_id: ConstraintId,
        event_id: String,
        producer_peers: Vec<SocketAddr>,
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
        }
    }

    pub fn constraint_type(&self) -> ConstraintType {
        match self {
            ConstraintConfig::ResponseProducer { .. } => ConstraintType::Response,
            ConstraintConfig::ResponseConsumer { .. } => ConstraintType::Response,
            ConstraintConfig::EndActivity { .. } => ConstraintType::EndActivity,
            ConstraintConfig::PrecedenceProducer { .. } => ConstraintType::Precedence,
            ConstraintConfig::PrecedenceConsumer { .. } => ConstraintType::Precedence,
        }
    }
}

fn default_unix_socket() -> String {
    "/tmp/event_listener.sock".to_string()
}

fn default_log_file() -> String {
    "app.log".to_string()
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
