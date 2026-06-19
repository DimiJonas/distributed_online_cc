use std::{fmt::Display, net::SocketAddr, sync::OnceLock, time::Duration};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use crate::{
    config::node_config::NodeConfig,
    network::message_types::MessageKind::{FireAndForget, Request, Response},
    types::{
        CaseId, ConstraintHandlerId, ConstraintId, ConstraintResultType, ConstraintType, EventTime,
        MessageId, RequestId, Token,
    },
};

/// gets set once at startup
/// consider switching to a message factory that lives in the appstate if more values have to be configured
///
/// controls how long a message lives in the queue before being dropped
pub static MESSAGE_TTL: OnceLock<Duration> = OnceLock::new();

pub fn init(config: NodeConfig) {
    match MESSAGE_TTL.set(config.message_ttl) {
        Ok(()) => {}
        Err(_) => {
            log::warn!("message TTL already initialized");
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Copy, PartialEq)]
pub enum MessageKind {
    Request,
    Response,
    FireAndForget,
}

#[derive(Debug, Clone)]
pub struct PeerMessage {
    pub message_id: MessageId,
    pub request_id: Option<RequestId>,
    pub message_kind: MessageKind,
    pub payload: PeerMessagePayload,
    // just an internal field to avoid messages being stuck permanently in the message queue
    // gets stripped when the message is sent to another peer
    // None = never expires (used for handshake)
    pub deadline: Option<Instant>,
}

impl PeerMessage {
    pub fn new(payload: PeerMessagePayload) -> Self {
        Self {
            message_id: MessageId::new_v4(),
            request_id: None,
            message_kind: FireAndForget,
            payload,
            deadline: Some(
                Instant::now() + *MESSAGE_TTL.get().expect("message TTL not initialized"),
            ),
        }
    }

    pub fn new_request(payload: PeerMessagePayload) -> Self {
        Self {
            message_id: MessageId::new_v4(),
            request_id: Some(RequestId::new_v4()),
            message_kind: Request,
            payload,
            deadline: Some(
                Instant::now() + *MESSAGE_TTL.get().expect("message TTL not initialized"),
            ),
        }
    }

    pub fn new_response(request_id: Option<RequestId>, payload: PeerMessagePayload) -> Self {
        Self {
            message_id: MessageId::new_v4(),
            request_id,
            message_kind: Response,
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
            request_id: None,
            message_kind: FireAndForget,
            payload,
            deadline: None,
        }
    }

    pub fn new_with_ttl(payload: PeerMessagePayload, ttl: Duration) -> Self {
        Self {
            message_id: MessageId::new_v4(),
            request_id: None,
            message_kind: FireAndForget,
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

    pub fn cli_req(msg: CliTestMessage) -> Self {
        Self::new_request(PeerMessagePayload::Cli(msg))
    }

    pub fn cli_res(request_id: RequestId, msg: CliTestMessage) -> Self {
        Self::new_response(Some(request_id), PeerMessagePayload::Cli(msg))
    }

    pub fn conformance(msg: CCMessage) -> Self {
        Self::new(PeerMessagePayload::Conformance(msg))
    }

    pub fn conformance_req(msg: CCMessage) -> Self {
        Self::new_request(PeerMessagePayload::Conformance(msg))
    }

    pub fn conformance_res(request_id: RequestId, msg: CCMessage) -> Self {
        Self::new_response(Some(request_id), PeerMessagePayload::Conformance(msg))
    }

    pub fn overlay_gossip(msg: OverlayMessage) -> Self {
        Self::new(PeerMessagePayload::Overlay(msg))
    }

    pub fn termination(msg: TerminationDetectionMessage) -> Self {
        Self::new(PeerMessagePayload::TerminationDetection(msg))
    }

    pub fn result(msg: CCResultMessage) -> Self {
        Self::new(PeerMessagePayload::Result(msg))
    }
}

impl Display for PeerMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "message_id={} type={}", self.message_id, self.payload)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PeerMessagePayload {
    Hello(HelloMessage),
    Cli(CliTestMessage),
    Conformance(CCMessage),
    Overlay(OverlayMessage),
    TerminationDetection(TerminationDetectionMessage),
    Result(CCResultMessage),
}

impl Display for PeerMessagePayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PeerMessagePayload::Hello(_) => write!(f, "hello"),
            PeerMessagePayload::Cli(_) => write!(f, "cli"),
            PeerMessagePayload::Conformance(_) => write!(f, "conformance"),
            PeerMessagePayload::Overlay(_) => write!(f, "overlay"),
            PeerMessagePayload::TerminationDetection(_) => write!(f, "termination detection"),
            PeerMessagePayload::Result(_) => write!(f, "result"),
        }
    }
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
    pub token: Option<Token>,
    // present for: PullTokenRequest and requests in general
    pub request_id: Option<RequestId>,
    // present for: PullTokenRequest
    pub sender_address: Option<SocketAddr>,
    // present for: PushToken for NotCoexistence constraint pushing claim tokens
    // used to differentiate if the token came from notcoexistence a or b handler which is needed when both are configured on the same node
    pub creating_handler: Option<ConstraintHandlerId>,
}

impl CCMessage {
    fn base(msg_type: CCMessageType, case_id: CaseId, constraint_id: ConstraintId) -> Self {
        Self {
            message_type: msg_type,
            case_id,
            constraint_id,
            timestamp: None,
            token: None,
            request_id: None,
            sender_address: None,
            creating_handler: None,
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

    pub fn push_claim_token(token: Token, handler_id: ConstraintHandlerId) -> Self {
        let mut msg = Self::base(
            CCMessageType::PushToken,
            token.case_id.clone(),
            token.constraint_id.clone(),
        );
        msg.timestamp = token.event_time;
        msg.token = Some(token);
        msg.creating_handler = Some(handler_id);
        msg
    }

    pub fn pull_token_request(case_id: CaseId, constraint_id: ConstraintId) -> Self {
        Self::base(CCMessageType::PullTokenRequest, case_id, constraint_id)
    }

    pub fn pull_token_response(token: Token) -> Self {
        let mut msg = Self::base(
            CCMessageType::PullTokenResponse,
            token.case_id.clone(),
            token.constraint_id.clone(),
        );
        msg.token = Some(token);
        msg
    }

    pub fn delete_token(
        case_id: CaseId,
        constraint_id: ConstraintId,
        event_timestamp: EventTime,
    ) -> Self {
        let mut msg = Self::base(CCMessageType::DeleteToken, case_id, constraint_id);
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
        write!(
            f,
            "CCMessage({:?}) for {}/{}",
            self.message_type, self.case_id, self.constraint_id
        )
    }
}

// only used to send results to the result sink
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CCResultMessage {
    pub created_at: DateTime<Utc>,
    pub info: String,
    pub case_id: CaseId,
    pub constraint_id: ConstraintId,
    pub constraint_type: ConstraintType,
    pub result: ConstraintResultType,
}

impl CCResultMessage {
    pub fn display_result(&self) -> String {
        format!(
            "Result of constraint {} ({:?} constraint) for {} at {} is {:?}:{}",
            self.constraint_id,
            self.constraint_type,
            self.case_id,
            self.created_at,
            self.result,
            self.info
        )
    }
}

impl Display for CCResultMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CCResultMessage({:?}) for {}/{}",
            self.result, self.case_id, self.constraint_id
        )
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
    use crate::types::{ConstraintType, Token};

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
        let msg = CCMessage::delete_token("case-2".to_string(), "c-2".to_string(), Utc::now());
        assert_eq!(msg.message_type, CCMessageType::DeleteToken);
        assert!(msg.token.is_none());
    }

    #[test]
    fn trigger_check_has_no_token_or_result() {
        let msg = CCMessage::trigger_check("case-3".to_string(), "c-3".to_string(), Utc::now());
        assert_eq!(msg.message_type, CCMessageType::TriggerCheck);
        assert!(msg.token.is_none());
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
            message: "test".to_string(),
            sender_addr: "127.0.0.1:9000".parse().unwrap(),
        });
        assert!(matches!(cli.payload, PeerMessagePayload::Cli(_)));
    }
}
