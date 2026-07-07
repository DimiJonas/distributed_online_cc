use thiserror::Error;
use tokio::sync::mpsc::error::SendError;

use crate::{
    network::{
        message_types::{MessageKind, PeerMessage},
        peer_manager_task::PeerManagerMessage,
    },
    types::{PeerAddress, RequestId},
};

#[derive(Debug, Error)]
pub enum SendReceiveError {
    #[error("timed out waiting for response to request {request_id}")]
    Timeout { request_id: RequestId },

    #[error("connection or channel closed before response arrived for request {request_id}")]
    ChannelClosed { request_id: RequestId },

    #[error("request id is missing in the constructed message {message}")]
    MissingRequestId { message: Box<PeerMessage> },

    #[error("message kind is {:?} but should be {:?}", actual_kind, expected_kind)]
    WrongMessageKind {
        expected_kind: MessageKind,
        actual_kind: MessageKind,
    },

    #[error("failed to send message to {peer_address}: {source}")]
    SendFailed {
        peer_address: PeerAddress,
        #[source]
        source: SendError<PeerManagerMessage>, // or whatever your send error is
    },
}
