use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type PeerId = u8;
pub type RequestId = u64;

#[derive(Serialize, Deserialize, Debug)]
pub enum PeerMessage {
    Request { id: RequestId, payload: String },
    Response { id: RequestId, payload: String },
}

#[derive(Debug)]
pub enum CliCommand {
    Send { peer_id: PeerId, msg: String },
    Connect { peer_id: PeerId },
}

#[derive(Debug, Clone, Serialize)]
pub enum XesAttributeValue {
    String(String),
    Date(DateTime<Utc>),
    Int(i64),
    Float(f64),
    Boolean(bool),
}

#[derive(Debug, Clone, Serialize)]
pub struct XesEvent {
    // from parent <trace>'s concept:name
    pub trace_id: String,
    pub attributes: HashMap<String, XesAttributeValue>,
}

impl XesEvent {
    pub fn name(&self) -> Option<&str> {
        match self.attributes.get("concept:name") {
            Some(XesAttributeValue::String(name)) => Some(name.as_str()),
            _ => None,
        }
    }

    pub fn timestamp(&self) -> Option<DateTime<Utc>> {
        match self.attributes.get("time:timestamp") {
            Some(XesAttributeValue::Date(timestamp)) => Some(*timestamp),
            _ => None,
        }
    }
}
