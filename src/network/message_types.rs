use std::{fmt::Display, net::SocketAddr, sync::OnceLock, time::Duration};

use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use crate::{
    config::node_config::NodeConfig,
    types::{CaseId, ConstraintId, EventTime, MessageId, Token, TokenId},
};

// !gets set once at startup
// !consider switching to a message factory that lives in the appstate if more values have to be configured
// controls how long a message lives in the queue before being dropped
pub static MESSAGE_TTL: OnceLock<Duration> = OnceLock::new();
// static DEFAULT_MESSAGE_TTL: Duration = Duration::from_secs(30);

pub fn init(config: NodeConfig) {
    match MESSAGE_TTL.set(config.message_ttl) {
        Ok(()) => {}
        Err(_) => {
            log::warn!("message TTL already initialized");
        }
    }
}

#[derive(Debug, Clone)]
pub struct PeerMessage {
    pub message_id: MessageId,
    pub payload: PeerMessagePayload,
    // None = never expires (used for handshake)
    pub deadline: Option<Instant>,
}

impl PeerMessage {
    pub fn new(payload: PeerMessagePayload) -> Self {
        Self {
            message_id: MessageId::new_v4(),
            payload,
            deadline: Some(
                Instant::now() + *MESSAGE_TTL.get().expect("message TTL not initialized"),
            ),
        }
    }

    // for handshake
    pub fn new_no_expiry(payload: PeerMessagePayload) -> Self {
        Self {
            message_id: MessageId::new_v4(),
            payload,
            deadline: None,
        }
    }

    pub fn new_with_ttl(payload: PeerMessagePayload, ttl: Duration) -> Self {
        Self {
            message_id: MessageId::new_v4(),
            payload,
            deadline: Some(Instant::now() + ttl),
        }
    }

    pub fn is_expired(&self) -> bool {
        match self.deadline {
            Some(deadline) => Instant::now() > deadline,
            None => false,
        }
    }

    // convenience constructors
    pub fn hello(msg: HelloMessage) -> Self {
        Self::new_no_expiry(PeerMessagePayload::Hello(msg))
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

impl Display for PeerMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let payload_type = match self.payload {
            PeerMessagePayload::Hello(_) => "hello",
            PeerMessagePayload::Cli(_) => "cli",
            PeerMessagePayload::Conformance(_) => "conformance",
            PeerMessagePayload::Overlay(_) => "overlay",
            PeerMessagePayload::TerminationDetection(_) => "termination detection",
        };
        write!(f, "id={} type={}", self.message_id, payload_type)
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

impl Display for HelloMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", serde_json::to_string_pretty(self).unwrap())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CliTestMessage {
    pub request_id: u64,
    pub message: String,
    pub sender_addr: SocketAddr,
}

impl Display for CliTestMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", serde_json::to_string_pretty(self).unwrap())
    }
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
    pub timestamp: Option<EventTime>,
    // present for: PushToken, PullTokenResponse
    // TODO: check if this can be removed - pushtoken just uses the timestamp for the order constraint
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
        msg.timestamp = token.event_time;
        msg.token = Some(token);
        msg
    }

    pub fn delete_token(
        case_id: CaseId,
        constraint_id: ConstraintId,
        event_timestamp: EventTime,
        token_id: TokenId,
    ) -> Self {
        let mut msg = Self::base(CCMessageType::DeleteToken, case_id, constraint_id);
        msg.token_id = Some(token_id);
        msg.timestamp = Some(event_timestamp);
        msg
    }

    pub fn trigger_check(
        case_id: CaseId,
        constraint_id: ConstraintId,
        event_timestamp: EventTime,
    ) -> Self {
        let mut msg = Self::base(CCMessageType::TriggerCheck, case_id, constraint_id);
        msg.timestamp = Some(event_timestamp);
        msg
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

impl Display for CCMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CCMessage {}/{}", self.case_id, self.constraint_id)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OverlayMessage {
    // TODO
}

impl Display for OverlayMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OverlayMessage")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TerminationDetectionMessage {
    // TODO
}

impl Display for TerminationDetectionMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TerminationDetectionMessage")
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::types::{ConstraintType, Token, TokenId};

    #[test]
    fn push_token_has_correct_type_and_token() {
        let token = Token::new(
            "case-1".to_string(),
            "c-1".to_string(),
            ConstraintType::Response,
            Some(Utc::now()),
            1,
        );
        let msg = CCMessage::push_token(token.clone());
        assert_eq!(msg.message_type, CCMessageType::PushToken);
        assert!(msg.token.is_some());
        assert_eq!(msg.case_id, "case-1");
    }

    #[test]
    fn delete_token_has_correct_type_and_token_id() {
        let msg = CCMessage::delete_token(
            "case-2".to_string(),
            "c-2".to_string(),
            Utc::now(),
            TokenId::nil(),
        );
        assert_eq!(msg.message_type, CCMessageType::DeleteToken);
        assert!(msg.token_id.is_some());
        assert!(msg.token.is_none());
    }

    #[test]
    fn trigger_check_has_no_token_or_result() {
        let msg = CCMessage::trigger_check("case-3".to_string(), "c-3".to_string(), Utc::now());
        assert_eq!(msg.message_type, CCMessageType::TriggerCheck);
        assert!(msg.token.is_none());
        assert!(msg.result.is_none());
    }

    #[test]
    fn check_result_carries_boolean_result() {
        let msg = CCMessage::check_result("case-4".to_string(), "c-4".to_string(), true);
        assert_eq!(msg.message_type, CCMessageType::CheckResult);
        assert_eq!(msg.result, Some(true));
    }

    #[test]
    fn case_terminated_has_empty_constraint_id() {
        let msg = CCMessage::case_terminated("case-5".to_string());
        assert_eq!(msg.message_type, CCMessageType::CaseTerminated);
        assert!(msg.constraint_id.is_empty());
    }

    #[test]
    fn peer_message_convenience_constructors_set_correct_payload_variant() {
        use crate::network::message_types::{CliTestMessage, HelloMessage, PeerMessagePayload};

        let hello = PeerMessage::hello(HelloMessage {
            listening_address: "127.0.0.1:9000".parse().unwrap(),
        });
        assert!(matches!(hello.payload, PeerMessagePayload::Hello(_)));

        let cli = PeerMessage::cli(CliTestMessage {
            request_id: 1,
            message: "test".to_string(),
            sender_addr: "127.0.0.1:9000".parse().unwrap(),
        });
        assert!(matches!(cli.payload, PeerMessagePayload::Cli(_)));
    }
}
