use chrono::{DateTime, FixedOffset, Utc};
use process_mining::core::event_data::case_centric::{Event, XESEditableAttribute};
use serde::{Deserialize, Serialize};

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

// augmenting the Event struct from rust4pm with a trace id
#[derive(Debug, Clone, Serialize)]
pub struct TracedEvent {
    pub trace_id: String,
    pub event: Event,
}

impl TracedEvent {
    pub fn timestamp(&self) -> Option<&DateTime<FixedOffset>> {
        self.event
            .attributes
            .get_by_key("time:timestamp")
            .and_then(|a| a.value.try_as_date())
    }

    pub fn timestamo_utc(&self) -> Option<DateTime<Utc>> {
        self.timestamp().map(|dt| dt.with_timezone(&Utc))
    }

    pub fn name(&self) -> Option<&str> {
        self.event
            .attributes
            .get_by_key("concept:name")
            .and_then(|a| a.value.try_as_string())
            .map(|s| s.as_str())
    }
}
