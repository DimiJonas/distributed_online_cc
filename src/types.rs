use std::{collections::HashMap, fmt::Display, net::SocketAddr};

use chrono::{DateTime, Utc};
use process_mining::core::event_data::case_centric::AttributeValue;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type PeerId = u64;
pub type RequestId = Uuid;
pub type CaseId = String;
pub type ConstraintId = String;
pub type EventId = String;
pub type TokenId = Uuid;
pub type EventInstanceId = Uuid;
pub type MessageId = Uuid;
pub type EventTime = DateTime<Utc>;
pub type SensorAttributeKey = String;
pub type AttributeKey = String;
pub type SensorAttributeThreshold = f64;
pub type Age = u64;
pub type PeerBatch = HashMap<SocketAddr, Age>;
pub type HandlerIndex = usize;

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Copy, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NetworkType {
    Overlay,
    Conformance,
}

#[derive(Debug, Clone, Copy, PartialEq, Hash, Eq, Serialize)]
pub struct PeerAddress {
    pub address: SocketAddr,
    pub network: NetworkType,
}

impl PeerAddress {
    pub fn new(address: SocketAddr, network: NetworkType) -> Self {
        Self { address, network }
    }

    pub fn new_overlay_address(address: SocketAddr) -> Self {
        Self::new(address, NetworkType::Overlay)
    }

    pub fn new_conformance_address(address: SocketAddr) -> Self {
        Self::new(address, NetworkType::Conformance)
    }
}

impl Display for PeerAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({:?} network)", self.address, self.network)
    }
}

// augmenting the Event struct from rust4pm with a trace id
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TracedEvent {
    pub case_id: CaseId,
    pub event_id: EventId,
    pub timestamp: EventTime,
    pub attributes: HashMap<AttributeKey, AttributeValue>,
    // for measurement purposes only
    pub instance_id: EventInstanceId,
}

impl TracedEvent {
    pub fn new(
        case_id: CaseId,
        event_id: EventId,
        timestamp: EventTime,
        attributes: HashMap<AttributeKey, AttributeValue>,
    ) -> Self {
        Self {
            case_id,
            event_id,
            timestamp,
            instance_id: EventInstanceId::new_v4(),
            attributes,
        }
    }

    pub fn get_attribute_by_key(&self, key: &AttributeKey) -> Option<&AttributeValue> {
        self.attributes.get(key)
    }
}

impl Display for TracedEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Event: name={} case={} time={}, instance_id={}",
            self.event_id, self.case_id, self.timestamp, self.instance_id,
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaseEnd {
    pub case_id: CaseId,
    pub timestamp: EventTime,
}

impl Display for CaseEnd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Case End: case={} time={}", self.case_id, self.timestamp)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Copy)]
pub enum ConstraintType {
    Response,
    EndActivity,
    Precedence,
    Order,
    Sensor,
    Time,
    NotCoexistence,
    Appearance,
    DistributedAppearance,
}

impl Display for ConstraintType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Hash, Eq, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub enum ConstraintRole {
    EndActivity,
    ResponseConsumer,
    ResponseProducer,
    PrecedenceProducer,
    PrecedenceConsumer,
    OrderFirst,
    OrderSecond,
    TimeStart,
    TimeStop,
    SensorCollector,
    SensorTrigger,
    NotCoexistenceA,
    NotCoexistenceB,
    AppearanceCounter,
    AppearanceTrigger,
    DistributedAppearanceCounter,
    DistributedAppearanceTrigger,
}

impl Display for ConstraintRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Copy)]
pub enum ConstraintResultType {
    Violated,
    TemporarilyViolated,
    Satisfied,
}

impl Display for ConstraintResultType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConstraintResultType::Satisfied => write!(f, "satisfied"),
            ConstraintResultType::TemporarilyViolated => write!(f, "temporarily violated"),
            ConstraintResultType::Violated => write!(f, "violated"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstraintResult {
    pub constraint_type: ConstraintType,
    pub constraint_result: ConstraintResultType,
    pub info: String,
}

impl Display for ConstraintResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ConstraintResult: type={:?} result={:?} info={}",
            self.constraint_type, self.constraint_result, self.info
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub enum TokenPayload {
    #[default]
    None,
    Timestamps(Vec<EventTime>),
    ExecutionPermission(bool),
    Counter(u64),
}

impl TokenPayload {
    pub fn get_count(&self) -> u64 {
        match self {
            TokenPayload::Counter(count) => *count,
            _ => 0,
        }
    }

    pub fn increment_counter(&mut self) {
        match self {
            TokenPayload::Counter(count) => *count += 1,
            TokenPayload::None => {
                *self = TokenPayload::Counter(1);
            }
            _ => {}
        }
    }

    pub fn reset_counter(&mut self) {
        match self {
            TokenPayload::Counter(count) => *count = 0,
            TokenPayload::None => {
                *self = TokenPayload::Counter(0);
            }
            _ => {}
        }
    }

    pub fn subtract_counter(&mut self, amount: u64) {
        match self {
            TokenPayload::Counter(count) => *count = count.saturating_sub(amount),
            TokenPayload::None => {
                *self = TokenPayload::Counter(0);
            }
            _ => {}
        }
    }

    pub fn push_timestamp(&mut self, t: EventTime) {
        match self {
            TokenPayload::Timestamps(v) => v.push(t),
            TokenPayload::None => {
                *self = TokenPayload::Timestamps(vec![t]);
            }
            _ => {}
        }
    }

    pub fn timestamps(&self) -> &[EventTime] {
        match self {
            TokenPayload::Timestamps(v) => v.as_slice(),
            _ => &[],
        }
    }

    pub fn grant_execution_permission(&mut self) {
        match self {
            TokenPayload::ExecutionPermission(b) => *b = true,
            TokenPayload::None => {
                *self = TokenPayload::ExecutionPermission(true);
            }
            _ => {}
        }
    }

    pub fn deny_execution_permission(&mut self) {
        match self {
            TokenPayload::ExecutionPermission(b) => *b = false,
            TokenPayload::None => {
                *self = TokenPayload::ExecutionPermission(false);
            }
            _ => {}
        }
    }

    pub fn is_execution_allowed(&self) -> bool {
        match self {
            TokenPayload::ExecutionPermission(b) => *b,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Token {
    pub id: TokenId,
    pub case_id: CaseId,
    pub constraint_id: ConstraintId,
    pub constraint_type: ConstraintType,
    pub event_time: Option<EventTime>,
    pub created_at: EventTime,
    pub created_by: PeerId,
    pub payload: TokenPayload,
}

impl Token {
    pub fn new(
        case_id: CaseId,
        constraint_id: ConstraintId,
        constraint_type: ConstraintType,
        event_time: Option<EventTime>,
        created_by: PeerId,
    ) -> Self {
        Self {
            id: TokenId::new_v4(),
            case_id,
            constraint_id,
            constraint_type,
            event_time,
            created_at: Utc::now(),
            created_by,
            payload: TokenPayload::None,
        }
    }

    pub fn new_with_payload(
        case_id: CaseId,
        constraint_id: ConstraintId,
        constraint_type: ConstraintType,
        event_time: Option<EventTime>,
        created_by: PeerId,
        payload: TokenPayload,
    ) -> Self {
        Self {
            payload,
            ..Self::new(
                case_id,
                constraint_id,
                constraint_type,
                event_time,
                created_by,
            )
        }
    }
}

impl Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Token with id={} for case={}/constraint={}",
            self.id, self.case_id, self.constraint_id
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_new_sets_fields_correctly() {
        let token = Token::new(
            "my-case".to_string(),
            "my-constraint".to_string(),
            ConstraintType::Response,
            Some(Utc::now()),
            7,
        );
        assert_eq!(token.case_id, "my-case");
        assert_eq!(token.constraint_id, "my-constraint");
        assert_eq!(token.created_by, 7);
        assert!(matches!(token.payload, TokenPayload::None));
    }

    #[test]
    fn token_new_generates_unique_ids() {
        let t1 = Token::new(
            "c".to_string(),
            "x".to_string(),
            ConstraintType::Response,
            Some(Utc::now()),
            1,
        );
        let t2 = Token::new(
            "c".to_string(),
            "x".to_string(),
            ConstraintType::Response,
            Some(Utc::now()),
            1,
        );
        assert_ne!(t1.id, t2.id);
    }

    #[test]
    fn token_payload_push_timestamp_from_none() {
        let mut p = TokenPayload::None;
        p.push_timestamp(Utc::now());
        assert!(matches!(p, TokenPayload::Timestamps(_)));
        assert_eq!(p.timestamps().len(), 1);
    }

    #[test]
    fn token_payload_push_timestamp_appends() {
        let mut p = TokenPayload::Timestamps(vec![]);
        let t1 = Utc::now();
        let t2 = Utc::now();
        p.push_timestamp(t1);
        p.push_timestamp(t2);
        assert_eq!(p.timestamps().len(), 2);
    }

    #[test]
    fn token_with_timestamps_payload() {
        let now = Utc::now();
        let token = Token::new_with_payload(
            "c".to_string(),
            "x".to_string(),
            ConstraintType::Order,
            Some(now),
            1,
            TokenPayload::Timestamps(vec![now]),
        );
        assert_eq!(token.payload.timestamps().len(), 1);
    }
}
