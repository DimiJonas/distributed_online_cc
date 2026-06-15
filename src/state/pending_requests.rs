use crate::network::message_types::PeerMessage;
use crate::types::RequestId;
use std::collections::HashMap;
use tokio::sync::oneshot;

pub struct PendingRequestManager {
    requests: HashMap<RequestId, oneshot::Sender<PeerMessage>>,
}

impl PendingRequestManager {
    pub fn new() -> Self {
        Self {
            requests: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.requests.len()
    }

    pub fn is_empty(&self) -> bool {
        self.requests.is_empty()
    }

    pub fn insert(&mut self, request_id: RequestId, sender: oneshot::Sender<PeerMessage>) {
        self.requests.insert(request_id, sender);
    }

    pub fn remove(&mut self, request_id: &RequestId) -> Option<oneshot::Sender<PeerMessage>> {
        self.requests.remove(request_id)
    }
}

impl Default for PendingRequestManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    fn make_sender() -> oneshot::Sender<PeerMessage> {
        let (tx, _rx) = oneshot::channel();
        tx
    }

    #[test]
    fn new_is_empty() {
        let mgr = PendingRequestManager::new();
        assert!(mgr.is_empty());
        assert_eq!(mgr.len(), 0);
    }

    #[test]
    fn insert_increases_len() {
        let mut mgr = PendingRequestManager::new();
        mgr.insert(Uuid::new_v4(), make_sender());
        assert_eq!(mgr.len(), 1);
        assert!(!mgr.is_empty());
    }

    #[test]
    fn remove_returns_sender_and_decreases_len() {
        let id = Uuid::new_v4();
        let mut mgr = PendingRequestManager::new();
        mgr.insert(id, make_sender());
        let result = mgr.remove(&id);
        assert!(result.is_some());
        assert_eq!(mgr.len(), 0);
    }

    #[test]
    fn remove_unknown_returns_none() {
        let mut mgr = PendingRequestManager::new();
        assert!(mgr.remove(&Uuid::new_v4()).is_none());
    }

    #[test]
    fn double_insert_same_id_overwrites() {
        let id = Uuid::new_v4();
        let mut mgr = PendingRequestManager::new();
        mgr.insert(id, make_sender());
        mgr.insert(id, make_sender());
        assert_eq!(mgr.len(), 1); // still one entry
    }
}
