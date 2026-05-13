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
