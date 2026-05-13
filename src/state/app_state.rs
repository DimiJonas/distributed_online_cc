use crate::config::types::NodeConfig;
use crate::protocol::constraint_logic::handler_registry::HandlerRegistry;
use crate::state::cc_result_store::CCResultStore;
use crate::state::peer_manager::PeerManager;
use crate::state::pending_requests::PendingRequestManager;
use crate::state::request_id_container::RequestIdContainer;
use crate::state::token_store::TokenStore;
use crate::types::TracedEvent;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<NodeConfig>,
    pub peers: Arc<Mutex<PeerManager>>,
    pub pending: Arc<Mutex<PendingRequestManager>>,
    pub request_ids: Arc<Mutex<RequestIdContainer>>,
    pub event_q: mpsc::Sender<TracedEvent>,
    pub token_store: Arc<Mutex<TokenStore>>,
    pub cc_results: Arc<Mutex<CCResultStore>>,
    pub handlers: Arc<HandlerRegistry>,
}

impl AppState {
    pub fn new(config_path: String, event_q: mpsc::Sender<TracedEvent>) -> Self {
        let config = Arc::new(NodeConfig::load_config(&config_path));
        let handlers = Arc::new(HandlerRegistry::build(&config));
        let initial_request_id = config.node_id as u64 * 1000;
        Self {
            config,
            peers: Arc::new(Mutex::new(PeerManager::new())),
            pending: Arc::new(Mutex::new(PendingRequestManager::new())),
            request_ids: Arc::new(Mutex::new(RequestIdContainer::new(initial_request_id))),
            event_q,
            token_store: Arc::new(Mutex::new(TokenStore::new())),
            cc_results: Arc::new(Mutex::new(CCResultStore::new())),
            handlers,
        }
    }
}
