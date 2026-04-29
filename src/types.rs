use serde::{Deserialize, Serialize};

pub type PeerId = u8;
pub type RequestId = u64;

#[derive(Serialize, Deserialize, Debug)]
pub enum PeerMessage {
    Request { id: RequestId, payload: String },
    Response { id: RequestId, payload: String },
}

#[derive(Debug)]
pub enum CliCommand {
    Send { peer_id: PeerId, msg: String },
    Connect { peer_id: PeerId },
}
