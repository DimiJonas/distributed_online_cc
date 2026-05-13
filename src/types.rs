use std::net::SocketAddr;

use chrono::{DateTime, FixedOffset, Utc};
use process_mining::core::event_data::case_centric::{Event, XESEditableAttribute};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type PeerId = u8;
pub type RequestId = u64;
pub type CaseId = String;
pub type ConstraintId = String;
pub type TokenId = Uuid;
pub type MessageId = Uuid;

#[derive(Debug)]
pub enum CliCommand {
    Send {
        peer_address: SocketAddr,
        msg: String,
    },
    Connect {
        peer_address: SocketAddr,
    },
}

// augmenting the Event struct from rust4pm with a trace id
#[derive(Debug, Clone, Serialize)]
pub struct TracedEvent {
    pub case_id: CaseId,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConstraintType {
    Response,
    EndActivity,
}

impl ConstraintType {
    // each constraint type can start with a different result
    pub fn initial_result(&self) -> ConstraintResultType {
        match self {
            ConstraintType::Response => ConstraintResultType::Satisfied,
            ConstraintType::EndActivity => ConstraintResultType::Satisfied,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConstraintResultType {
    Violated,
    TemporarilyViolated,
    Satisfied,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstraintResult {
    pub constraint_type: ConstraintType,
    pub constraint_result: ConstraintResultType,
    pub info: String,
    pub history: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Token {
    pub id: TokenId,
    pub case_id: CaseId,
    pub constraint_id: ConstraintId, // maybe redundant
    pub constraint_type: ConstraintType,
    pub created_at: DateTime<Utc>,
    pub created_by: PeerId,
    pub payload: Option<serde_json::Value>,
}

impl Token {
    pub fn new(
        case_id: CaseId,
        constraint_id: ConstraintId,
        constraint_type: ConstraintType,
        created_by: PeerId,
        payload: Option<serde_json::Value>,
    ) -> Self {
        Self {
            id: TokenId::new_v4(),
            case_id,
            constraint_id,
            constraint_type,
            created_at: Utc::now(),
            created_by,
            payload,
        }
    }
}
