use crate::network::message_types::PeerMessage;
use bytes::{Buf, BufMut, BytesMut};

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

impl std::fmt::Display for FramingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FramingError::Incomplete => write!(f, "incomplete frame in buffer"),
            FramingError::Serialize(e) => write!(f, "failed to serialize message: {}", e),
            FramingError::Deserialize(e) => write!(f, "failed to deserialize message: {}", e),
        }
    }
}

impl std::error::Error for FramingError {}

/// Encode a PeerMessage for sending.
pub fn encode(msg: &PeerMessage) -> Result<BytesMut, FramingError> {
    let msg_bytes = serde_json::to_vec(msg).map_err(FramingError::Serialize)?;

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

    serde_json::from_slice(&msg_bytes).map_err(FramingError::Deserialize)
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
