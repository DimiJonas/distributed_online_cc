use std::{fmt::Display, net::SocketAddr};

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

impl Display for ConstraintResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", serde_json::to_string_pretty(self).unwrap())
    }
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

impl Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "token with id={} for case/constraint {}/{}",
            self.id, self.case_id, self.constraint_id
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constraint_type_response_initial_result_is_satisfied() {
        assert!(matches!(
            ConstraintType::Response.initial_result(),
            ConstraintResultType::Satisfied
        ));
    }

    #[test]
    fn constraint_type_end_activity_initial_result_is_satisfied() {
        assert!(matches!(
            ConstraintType::EndActivity.initial_result(),
            ConstraintResultType::Satisfied
        ));
    }

    #[test]
    fn token_new_sets_fields_correctly() {
        let token = Token::new(
            "my-case".to_string(),
            "my-constraint".to_string(),
            ConstraintType::Response,
            7,
            None,
        );
        assert_eq!(token.case_id, "my-case");
        assert_eq!(token.constraint_id, "my-constraint");
        assert_eq!(token.created_by, 7);
        assert!(token.payload.is_none());
    }

    #[test]
    fn token_new_generates_unique_ids() {
        let t1 = Token::new(
            "c".to_string(),
            "x".to_string(),
            ConstraintType::Response,
            1,
            None,
        );
        let t2 = Token::new(
            "c".to_string(),
            "x".to_string(),
            ConstraintType::Response,
            1,
            None,
        );
        assert_ne!(t1.id, t2.id);
    }

    #[test]
    fn token_with_payload() {
        let payload = serde_json::json!({ "key": "value" });
        let token = Token::new(
            "c".to_string(),
            "x".to_string(),
            ConstraintType::Response,
            1,
            Some(payload.clone()),
        );
        assert_eq!(token.payload.unwrap(), payload);
    }
}
