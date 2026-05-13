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

    pub fn insert(&mut self, peer_address: SocketAddr, handle: PeerHandle) {
        if self.peers.contains_key(&peer_address) {
            println!("peer with id {} already exists, overwriting", peer_address);
        }
        self.peers.insert(peer_address, handle);
    }

    pub fn remove(&mut self, peer_address: &SocketAddr) {
        self.peers.remove(peer_address);
    }

    pub fn get_sender(&self, peer_address: &SocketAddr) -> Option<mpsc::Sender<PeerMessage>> {
        self.peers.get(peer_address).map(|h| h.sender.clone())
    }

    pub fn contains(&self, peer_address: &SocketAddr) -> bool {
        self.peers.contains_key(peer_address)
    }
}

impl Default for PeerManager {
    fn default() -> Self {
        Self::new()
    }
}
