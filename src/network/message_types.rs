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
