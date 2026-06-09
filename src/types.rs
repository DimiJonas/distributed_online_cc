use std::{fmt::Display, net::SocketAddr, str::FromStr};

use chrono::{DateTime, FixedOffset, Utc};
use process_mining::core::event_data::case_centric::{AttributeValue, Event, XESEditableAttribute};
use serde::{Deserialize, Serialize, de};
use uuid::Uuid;

pub type PeerId = u8;
pub type RequestId = u64;
pub type CaseId = String;
pub type ConstraintId = String;
pub type EventId = String;
pub type TokenId = Uuid;
pub type EventInstanceId = Uuid;
pub type MessageId = Uuid;
pub type EventTime = DateTime<Utc>;

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

    pub fn timestamp_utc(&self) -> Option<DateTime<Utc>> {
        self.timestamp().map(|dt| dt.with_timezone(&Utc))
    }

    pub fn timestamp_utc_as_str(&self) -> String {
        if let Some(timestamp) = self.timestamp_utc() {
            timestamp.clone().to_string()
        } else {
            "None".to_string()
        }
    }

    pub fn get_attribute_by_key(&self, key: &str) -> Option<AttributeValue> {
        self.event
            .attributes
            .get_by_key(key)
            .map(|a| a.value.clone())
    }

    pub fn name(&self) -> Option<&str> {
        self.event
            .attributes
            .get_by_key("concept:name")
            .and_then(|a| a.value.try_as_string())
            .map(|s| s.as_str())
    }
}

impl Display for TracedEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Event: name={} case={} time={}",
            self.name().unwrap_or_default(),
            self.case_id,
            self.timestamp_utc_as_str(),
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ConstraintType {
    Response,
    EndActivity,
    Precedence,
    Order,
    Sensor,
    TimerStartMax,
    TimerStartMin,
    TimerStop,
}

impl ConstraintType {
    // each constraint type can start with a different result
    pub fn initial_result(&self) -> ConstraintResultType {
        match self {
            ConstraintType::Response => ConstraintResultType::Satisfied,
            ConstraintType::EndActivity => ConstraintResultType::Satisfied,
            ConstraintType::Precedence => ConstraintResultType::Satisfied,
            ConstraintType::Order => ConstraintResultType::Satisfied,
            ConstraintType::Sensor => ConstraintResultType::Satisfied,
            ConstraintType::TimerStartMax => ConstraintResultType::Satisfied,
            ConstraintType::TimerStartMin => ConstraintResultType::TemporarilyViolated,
            ConstraintType::TimerStop => ConstraintResultType::Satisfied,
        }
    }
}

impl Display for ConstraintType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Response => write!(f, "Response"),
            Self::EndActivity => write!(f, "EndActivity"),
            Self::Precedence => write!(f, "Precedence"),
            Self::Order => write!(f, "Order"),
            Self::Sensor => write!(f, "Sensor"),
            Self::TimerStartMax => write!(f, "Time"),
            Self::TimerStartMin => write!(f, "Time"),
            Self::TimerStop => write!(f, "Time"),
        }
    }
}

#[derive(Debug, Hash, Eq, PartialEq, Clone, Copy)]
pub enum ConstraintHandlerId {
    EndActivity,
    ResponseConsumer,
    ResponseProducer,
    PrecedenceProducer,
    PrecedenceConsumer,
    OrderFirst,
    OrderSecond,
    TimeStart,
    TimeStop,
    Sensor,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConstraintResultType {
    Violated,
    TemporarilyViolated,
    Satisfied,
}

impl Display for ConstraintResultType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Violated => write!(f, "Violated"),
            Self::TemporarilyViolated => write!(f, "TemporarilyViolated"),
            Self::Satisfied => write!(f, "Satisfied"),
        }
    }
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
        write!(
            f,
            "ConstraintResult: type={} result={} info={}",
            self.constraint_type, self.constraint_result, self.info
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub enum TokenPayload {
    #[default]
    None,
    Timestamps(Vec<EventTime>),
    // TimerTokens,
}

impl TokenPayload {
    pub fn push_timestamp(&mut self, t: EventTime) {
        match self {
            TokenPayload::Timestamps(v) => v.push(t),
            TokenPayload::None => {
                *self = TokenPayload::Timestamps(vec![t]);
            }
        }
    }

    pub fn timestamps(&self) -> &[EventTime] {
        match self {
            TokenPayload::Timestamps(v) => v.as_slice(),
            TokenPayload::None => &[],
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

#[derive(Debug, PartialEq, Serialize, Clone)]
pub enum SensorConstraintMode {
    Lt,
    Gt,
    Lte,
    Gte,
    Eq,
}

impl SensorConstraintMode {
    pub fn evaluate(&self, a: f64, b: f64) -> bool {
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
#[serde(rename_all = "lowercase")] // makes serialization case insensitive
pub enum TimeConstraintMode {
    Min,
    Max,
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
