use std::fmt::Display;

use crate::{
    network::message_types::{MESSAGE_TTL, PeerMessage, PeerMessagePayload},
    types::MessageId,
};
use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

static HEADER_LENGTH: usize = 4; // 4 bytes for the length prefix

#[derive(Debug)]
pub enum FramingError {
    // buffer too short to contain a complete frame
    Incomplete,
    // json serialization failed
    Serialize(serde_json::Error),
    // json deserialization failed
    Deserialize(serde_json::Error),
}

impl Display for FramingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FramingError::Incomplete => write!(f, "incomplete frame in buffer"),
            FramingError::Serialize(e) => write!(f, "failed to serialize message: {}", e),
            FramingError::Deserialize(e) => write!(f, "failed to deserialize message: {}", e),
        }
    }
}

impl std::error::Error for FramingError {}

// serializable message type (PeerMessage has an Instant and that is not serializable)
#[derive(Serialize, Deserialize)]
struct WirePeerMessage {
    message_id: MessageId,
    payload: PeerMessagePayload,
}

// convert from PeerMessage to WirePeerMessage (drop deadline - its local metadata anyway)
impl From<&PeerMessage> for WirePeerMessage {
    fn from(msg: &PeerMessage) -> Self {
        Self {
            message_id: msg.message_id,
            payload: msg.payload.clone(),
        }
    }
}

// convertfrom WirePeerMessage to PeerMessage (add deadline)
impl From<WirePeerMessage> for PeerMessage {
    fn from(wire: WirePeerMessage) -> Self {
        // messages received from the wire get a fresh TTL from now
        PeerMessage {
            message_id: wire.message_id,
            payload: wire.payload,
            deadline: Some(
                Instant::now() + *MESSAGE_TTL.get().expect("message TTL not initialized"),
            ),
        }
    }
}

/// Encode a PeerMessage for sending.
pub fn encode(msg: &PeerMessage) -> Result<BytesMut, FramingError> {
    let wire_msg = WirePeerMessage::from(msg);
    let msg_bytes = serde_json::to_vec(&wire_msg).map_err(FramingError::Serialize)?;

    // prefix the json data with its length for raw framing
    let mut framed_msg = BytesMut::with_capacity(HEADER_LENGTH + msg_bytes.len());
    framed_msg.put_u32(msg_bytes.len() as u32);
    framed_msg.extend_from_slice(&msg_bytes);
    Ok(framed_msg)
}

/// Decode one complete frame from `buf`.
/// The caller must ensure `check_frame(buf)` is true before calling,
/// otherwise `FramingError::Incomplete` is returned.
pub fn decode(buf: &mut BytesMut) -> Result<PeerMessage, FramingError> {
    if !check_frame(buf) {
        return Err(FramingError::Incomplete);
    }

    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    buf.advance(HEADER_LENGTH);
    let msg_bytes = buf.split_to(len);

    let wire_msg: WirePeerMessage =
        serde_json::from_slice(&msg_bytes).map_err(FramingError::Deserialize)?;
    Ok(PeerMessage::from(wire_msg))
}

/// Returns true when `buf` contains a complete frame.
pub fn check_frame(buf: &BytesMut) -> bool {
    if buf.len() < HEADER_LENGTH {
        return false;
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    buf.len() >= HEADER_LENGTH + len
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::message_types::{HelloMessage, PeerMessage};

    fn sample_message() -> PeerMessage {
        PeerMessage::hello(HelloMessage {
            listening_address: "127.0.0.1:8000".parse().unwrap(),
        })
    }

    #[test]
    fn encode_decode_roundtrip() {
        let msg = sample_message();
        let mut buf = encode(&msg).unwrap();
        let decoded = decode(&mut buf).unwrap();

        if let (
            crate::network::message_types::PeerMessagePayload::Hello(orig),
            crate::network::message_types::PeerMessagePayload::Hello(dec),
        ) = (&msg.payload, &decoded.payload)
        {
            assert_eq!(orig.listening_address, dec.listening_address);
        } else {
            panic!("payload variant mismatch after decode");
        }
    }

    #[test]
    fn check_frame_true_when_complete() {
        let buf = encode(&sample_message()).unwrap();
        assert!(check_frame(&buf));
    }

    #[test]
    fn check_frame_false_for_empty_buffer() {
        assert!(!check_frame(&BytesMut::new()));
    }

    #[test]
    fn check_frame_false_when_only_partial_header() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[0u8, 0u8]);
        assert!(!check_frame(&buf));
    }

    #[test]
    fn check_frame_false_when_body_incomplete() {
        let full = encode(&sample_message()).unwrap();
        let partial = BytesMut::from(&full[..5]);
        assert!(!check_frame(&partial));
    }

    #[test]
    fn encode_has_correct_length_prefix() {
        let buf = encode(&sample_message()).unwrap();
        let declared_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        assert_eq!(buf.len(), 4 + declared_len);
    }

    #[test]
    fn decode_returns_incomplete_on_empty_buffer() {
        let mut buf = BytesMut::new();
        assert!(matches!(decode(&mut buf), Err(FramingError::Incomplete)));
    }

    #[test]
    fn decode_returns_incomplete_on_partial_frame() {
        let full = encode(&sample_message()).unwrap();
        let mut partial = BytesMut::from(&full[..5]);
        assert!(matches!(
            decode(&mut partial),
            Err(FramingError::Incomplete)
        ));
    }

    #[test]
    fn decode_returns_deserialize_error_on_garbage_body() {
        // Build a frame with valid length prefix but garbage JSON body
        let garbage = b"not valid json!!";
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&(garbage.len() as u32).to_be_bytes());
        buf.extend_from_slice(garbage);
        assert!(matches!(
            decode(&mut buf),
            Err(FramingError::Deserialize(_))
        ));
    }
}
