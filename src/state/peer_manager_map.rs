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

    pub fn remove(
        &mut self,
        peer_address: &SocketAddr,
    ) -> Option<mpsc::Sender<PeerManagerMessage>> {
        self.peer_managers.remove(peer_address)
    }
}

impl Default for PeerManagerMap {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::util::test_tools::addr;

    use super::*;

    fn make_sender() -> mpsc::Sender<PeerManagerMessage> {
        let (tx, _rx) = mpsc::channel(1);
        tx
    }

    #[test]
    fn test_new_is_empty() {
        let map = PeerManagerMap::new();
        assert!(!map.has_manager(&addr(8080)));
    }

    #[test]
    fn test_default_is_empty() {
        let map = PeerManagerMap::default();
        assert!(!map.has_manager(&addr(8080)));
    }

    #[test]
    fn test_insert_and_has_manager() {
        let mut map = PeerManagerMap::new();
        let a = addr(8080);
        map.insert(a, make_sender());
        assert!(map.has_manager(&a));
    }

    #[test]
    fn test_get_returns_sender_after_insert() {
        let mut map = PeerManagerMap::new();
        let a = addr(8080);
        map.insert(a, make_sender());
        assert!(map.get(&a).is_some());
    }

    #[test]
    fn test_get_returns_none_for_unknown_address() {
        let map = PeerManagerMap::new();
        assert!(map.get(&addr(9999)).is_none());
    }

    #[test]
    fn test_duplicate_insert_is_ignored() {
        let mut map = PeerManagerMap::new();
        let a = addr(8080);
        let (tx1, _rx1) = mpsc::channel::<PeerManagerMessage>(1);
        let (tx2, _rx2) = mpsc::channel::<PeerManagerMessage>(1);

        map.insert(a, tx1.clone());
        map.insert(a, tx2.clone()); // should be ignored

        // The stored sender should still be the first one (same pointer)
        let stored = map.get(&a).unwrap();
        assert!(stored.same_channel(&tx1));
        assert!(!stored.same_channel(&tx2));
    }

    #[test]
    fn test_insert_multiple_distinct_addresses() {
        let mut map = PeerManagerMap::new();
        let a1 = addr(8001);
        let a2 = addr(8002);
        let a3 = addr(8003);

        map.insert(a1, make_sender());
        map.insert(a2, make_sender());
        map.insert(a3, make_sender());

        assert!(map.has_manager(&a1));
        assert!(map.has_manager(&a2));
        assert!(map.has_manager(&a3));
    }

    #[test]
    fn test_has_manager_false_for_different_port() {
        let mut map = PeerManagerMap::new();
        map.insert(addr(8080), make_sender());
        assert!(!map.has_manager(&addr(8081)));
    }

    #[test]
    fn test_get_clones_sender_independently() {
        let mut map = PeerManagerMap::new();
        let a = addr(8080);
        map.insert(a, make_sender());

        let s1 = map.get(&a).unwrap();
        let s2 = map.get(&a).unwrap();
        // Both clones should refer to the same underlying channel
        assert!(s1.same_channel(&s2));
    }
}
