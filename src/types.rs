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
}

impl ConstraintType {
    // each constraint type can start with a different result
    pub fn initial_result(&self) -> ConstraintResultType {
        match self {
            ConstraintType::Response => ConstraintResultType::Satisfied,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerMessage {
    pub message_id: MessageId,
    pub payload: PeerMessagePayload,
}

impl PeerMessage {
    pub fn new(payload: PeerMessagePayload) -> Self {
        Self {
            message_id: MessageId::new_v4(),
            payload,
        }
    }

    // convenience constructors
    pub fn hello(msg: HelloMessage) -> Self {
        Self::new(PeerMessagePayload::Hello(msg))
    }
    pub fn cli(msg: CliTestMessage) -> Self {
        Self::new(PeerMessagePayload::Cli(msg))
    }
    pub fn conformance(msg: CCMessage) -> Self {
        Self::new(PeerMessagePayload::Conformance(msg))
    }

    pub fn overlay_gossip(msg: OverlayMessage) -> Self {
        Self::new(PeerMessagePayload::Overlay(msg))
    }

    pub fn termination(msg: TerminationDetectionMessage) -> Self {
        Self::new(PeerMessagePayload::TerminationDetection(msg))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PeerMessagePayload {
    Hello(HelloMessage),
    Cli(CliTestMessage),
    Conformance(CCMessage),
    Overlay(OverlayMessage),
    TerminationDetection(TerminationDetectionMessage),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloMessage {
    pub listening_address: SocketAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CliTestMessage {
    pub request_id: u64,
    pub message: String,
    pub sender_addr: SocketAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CCMessageType {
    // push token into token-in area of another node
    PushToken,
    // request token on another node (maybe by constraint+case)
    PullTokenRequest,
    // response to a PullTokenRequest with token in payload
    PullTokenResponse,
    // trigger token deletion on another node (by token_id)
    DeleteToken,
    // trigger constraint check for a case on another node
    TriggerCheck,
    // response to TriggerCheck with the check result (maybe not needed)
    CheckResult,
    // notify case termination
    CaseTerminated,
}

// message envelope for conformance checking communication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CCMessage {
    pub message_type: CCMessageType,
    pub case_id: CaseId,
    pub constraint_id: ConstraintId,
    // no timestamp unless needed for a specific constraint
    pub timestamp: Option<DateTime<Utc>>,
    // present for: PushToken, PullTokenResponse
    pub token: Option<Token>,
    // present for: DeleteToken (identifies which token to remove)
    pub token_id: Option<TokenId>,
    // present for: CheckResult (true = constraint satisfied)
    pub result: Option<bool>,
}

impl CCMessage {
    fn base(msg_type: CCMessageType, case_id: CaseId, constraint_id: ConstraintId) -> Self {
        Self {
            message_type: msg_type,
            case_id,
            constraint_id,
            timestamp: None,
            token: None,
            token_id: None,
            result: None,
        }
    }

    pub fn push_token(token: Token) -> Self {
        let mut msg = Self::base(
            CCMessageType::PushToken,
            token.case_id.clone(),
            token.constraint_id.clone(),
        );
        msg.token = Some(token);
        msg
    }

    pub fn delete_token(case_id: CaseId, constraint_id: ConstraintId, token_id: TokenId) -> Self {
        let mut msg = Self::base(CCMessageType::DeleteToken, case_id, constraint_id);
        msg.token_id = Some(token_id);
        msg
    }

    pub fn trigger_check(case_id: CaseId, constraint_id: ConstraintId) -> Self {
        Self::base(CCMessageType::TriggerCheck, case_id, constraint_id)
    }

    pub fn check_result(case_id: CaseId, constraint_id: ConstraintId, satisfied: bool) -> Self {
        let mut msg = Self::base(CCMessageType::CheckResult, case_id, constraint_id);
        msg.result = Some(satisfied);
        msg
    }

    pub fn case_terminated(case_id: CaseId) -> Self {
        Self::base(
            CCMessageType::CaseTerminated,
            case_id,
            String::new(), // no specific constraint addresses
        )
    }

    // TODO: add more constructors e.g. for pulltokenrequest etc.
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OverlayMessage {
    // TODO
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TerminationDetectionMessage {
    // TODO
}
