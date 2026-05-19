use crate::network::peer_manager_task::PeerManagerMessage;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::sync::mpsc;

//maps address -> sender into the peer_manager_task for that peer
pub struct PeerManagerMap {
    peer_managers: HashMap<SocketAddr, mpsc::Sender<PeerManagerMessage>>,
}

impl PeerManagerMap {
    pub fn new() -> Self {
        Self {
            peer_managers: HashMap::new(),
        }
    }

    pub fn insert(&mut self, peer_address: SocketAddr, sender: mpsc::Sender<PeerManagerMessage>) {
        if self.peer_managers.contains_key(&peer_address) {
            log::warn!(
                "peer manager for {} already exists, ignoring insert",
                peer_address
            );
            return;
        }
        self.peer_managers.insert(peer_address, sender);
    }

    pub fn get(&self, peer_address: &SocketAddr) -> Option<mpsc::Sender<PeerManagerMessage>> {
        self.peer_managers.get(peer_address).cloned()
    }

    pub fn has_manager(&self, peer_address: &SocketAddr) -> bool {
        self.peer_managers.contains_key(peer_address)
    }
}

impl Default for PeerManagerMap {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{}", port).parse().unwrap()
    }
}
