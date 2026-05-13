/*
    config could look like this in TOML for a response constraint:

        node_id = 1

        [[constraints]]
        constraint_role = "ResponseProducer"
        constraint_id = "response_order_shipped"
        event_id = "order_placed"

        [[constraints]]
        constraint_role = "ResponseConsumer"
        constraint_id = "response_order_shipped"
        event_id = "order_shipped"
        node_a_peers = [2, 3]

*/

use crate::types::{ConstraintId, ConstraintType, PeerId};
use serde::{Deserialize, Serialize};
use std::fs;
use std::net::SocketAddr;

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
    // TODO: add more
}

impl ConstraintConfig {
    pub fn constraint_id(&self) -> &str {
        match self {
            ConstraintConfig::ResponseProducer { constraint_id, .. } => constraint_id,
            ConstraintConfig::ResponseConsumer { constraint_id, .. } => constraint_id,
            ConstraintConfig::EndActivity { constraint_id, .. } => constraint_id,
        }
    }

    pub fn constraint_type(&self) -> ConstraintType {
        match self {
            ConstraintConfig::ResponseProducer { .. } => ConstraintType::Response,
            ConstraintConfig::ResponseConsumer { .. } => ConstraintType::Response,
            ConstraintConfig::EndActivity { .. } => ConstraintType::EndActivity,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NodeConfig {
    pub node_id: PeerId,
    pub listen_address: SocketAddr,
    pub unix_socket: String,
    #[serde(default)]
    pub peers: Vec<String>,
    pub constraints: Vec<ConstraintConfig>,
}

impl NodeConfig {
    pub fn load_config(path: &str) -> Self {
        let contents = fs::read_to_string(path).expect("could not read config file");
        toml::from_str(&contents).expect("could not parse config file")
    }
}
