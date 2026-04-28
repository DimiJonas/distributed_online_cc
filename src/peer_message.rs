#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub enum PeerMessage {
    Text(String),
}
