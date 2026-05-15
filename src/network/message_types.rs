use std::net::SocketAddr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::{CaseId, ConstraintId, MessageId, Token, TokenId};

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ConstraintType, Token, TokenId};

    #[test]
    fn push_token_has_correct_type_and_token() {
        let token = Token::new(
            "case-1".to_string(),
            "c-1".to_string(),
            ConstraintType::Response,
            1,
            None,
        );
        let msg = CCMessage::push_token(token.clone());
        assert_eq!(msg.message_type, CCMessageType::PushToken);
        assert!(msg.token.is_some());
        assert_eq!(msg.case_id, "case-1");
    }

    #[test]
    fn delete_token_has_correct_type_and_token_id() {
        let msg = CCMessage::delete_token("case-2".to_string(), "c-2".to_string(), TokenId::nil());
        assert_eq!(msg.message_type, CCMessageType::DeleteToken);
        assert!(msg.token_id.is_some());
        assert!(msg.token.is_none());
    }

    #[test]
    fn trigger_check_has_no_token_or_result() {
        let msg = CCMessage::trigger_check("case-3".to_string(), "c-3".to_string());
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
