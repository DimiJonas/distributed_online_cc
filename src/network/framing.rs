use crate::types::PeerMessage;
use bytes::{Buf, BufMut, BytesMut};

static HEADER_LENGTH: usize = 4; // 4 bytes for the length prefix

pub fn encode(msg: &PeerMessage) -> BytesMut {
    let msg_bytes = serde_json::to_vec(&msg).unwrap();

    // prefix the json data with its length for raw framing
    let mut framed_msg = BytesMut::with_capacity(HEADER_LENGTH + msg_bytes.len());
    framed_msg.put_u32(msg_bytes.len() as u32);
    framed_msg.extend_from_slice(&msg_bytes);
    framed_msg
}

pub fn decode(buf: &mut BytesMut) -> PeerMessage {
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;

    buf.advance(HEADER_LENGTH);
    let msg_bytes = buf.split_to(len);

    serde_json::from_slice(&msg_bytes).unwrap()
}

pub fn check_frame(buf: &BytesMut) -> bool {
    if buf.len() < HEADER_LENGTH {
        // not even enough for length prefix
        return false;
    }

    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;

    // all bytes received?
    buf.len() >= HEADER_LENGTH + len
}
