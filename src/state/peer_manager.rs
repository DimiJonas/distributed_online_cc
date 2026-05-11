use crate::types::PeerMessage;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::sync::mpsc;

pub struct PeerHandle {
    sender: mpsc::Sender<PeerMessage>,
}

impl PeerHandle {
    pub fn new(sender: mpsc::Sender<PeerMessage>) -> Self {
        Self { sender }
    }
}

pub struct PeerManager {
    peers: HashMap<SocketAddr, PeerHandle>,
}

impl PeerManager {
    pub fn new() -> Self {
        Self {
            peers: HashMap::new(),
        }
    }

    pub fn insert(&mut self, peer_id: SocketAddr, handle: PeerHandle) {
        if self.peers.contains_key(&peer_id) {
            println!("peer with id {} already exists, overwriting", peer_id);
        }
        self.peers.insert(peer_id, handle);
    }

    pub fn remove(&mut self, peer_id: &SocketAddr) {
        self.peers.remove(peer_id);
    }

    pub fn get_sender(&self, peer_id: &SocketAddr) -> Option<mpsc::Sender<PeerMessage>> {
        self.peers.get(peer_id).map(|h| h.sender.clone())
    }
}

impl Default for PeerManager {
    fn default() -> Self {
        Self::new()
    }
}
