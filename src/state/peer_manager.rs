use crate::network::message_types::PeerMessage;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_handle() -> PeerHandle {
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        PeerHandle::new(tx)
    }

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{}", port).parse().unwrap()
    }

    #[test]
    fn new_manager_is_empty() {
        let pm = PeerManager::new();
        assert!(!pm.contains(&addr(8000)));
    }

    #[test]
    fn insert_and_contains() {
        let mut pm = PeerManager::new();
        pm.insert(addr(8001), dummy_handle());
        assert!(pm.contains(&addr(8001)));
    }

    #[test]
    fn get_sender_returns_some_after_insert() {
        let mut pm = PeerManager::new();
        pm.insert(addr(8002), dummy_handle());
        assert!(pm.get_sender(&addr(8002)).is_some());
    }

    #[test]
    fn get_sender_returns_none_for_unknown_peer() {
        let pm = PeerManager::new();
        assert!(pm.get_sender(&addr(9999)).is_none());
    }

    #[test]
    fn remove_clears_entry() {
        let mut pm = PeerManager::new();
        pm.insert(addr(8003), dummy_handle());
        pm.remove(&addr(8003));
        assert!(!pm.contains(&addr(8003)));
        assert!(pm.get_sender(&addr(8003)).is_none());
    }

    #[test]
    fn remove_nonexistent_does_not_panic() {
        let mut pm = PeerManager::new();
        pm.remove(&addr(9999)); // should not panic
    }
}
