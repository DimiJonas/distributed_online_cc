use crate::state::peer_manager::PeerManager;
use crate::state::pending_reuests::PendingRequestManager;
use crate::state::request_id_container::RequestIdContainer;
use crate::types::RequestId;
use crate::types::XesEvent;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct AppState {
    pub peers: Arc<Mutex<PeerManager>>,
    pub pending: Arc<Mutex<PendingRequestManager>>,
    pub request_ids: Arc<Mutex<RequestIdContainer>>,
    pub event_q: mpsc::Sender<XesEvent>,
}

impl AppState {
    pub fn new(initial_request_id: RequestId, event_q: mpsc::Sender<XesEvent>) -> Self {
        Self {
            peers: Arc::new(Mutex::new(PeerManager::new())),
            pending: Arc::new(Mutex::new(PendingRequestManager::new())),
            request_ids: Arc::new(Mutex::new(RequestIdContainer::new(initial_request_id))),
            event_q,
        }
    }
}
