use crate::config::{handler_registry::HandlerRegistry, node_config::NodeConfig};
use crate::network::message_types::PeerMessage;
use crate::state::cc_result_store::CCResultStore;
use crate::state::meta_token_store::MetaTokenStore;
use crate::state::peer_manager_map::PeerManagerMap;
use crate::state::pending_requests::PendingRequestManager;
use crate::state::result_send_buffer::ConstraintResultSendBuffer;
use crate::state::timer_token_store::TimerTokenStore;
use crate::state::token_in_store::TokenInStore;
use crate::state::token_out_store::TokenOutStore;
use crate::state::watchdog_cancel_store::WatchdogCancelStore;
use crate::types::TracedEvent;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<NodeConfig>,
    pub pending: Arc<Mutex<PendingRequestManager>>,
    pub event_q: mpsc::Sender<TracedEvent>,
    pub token_in: Arc<Mutex<TokenInStore>>,
    pub token_out: Arc<Mutex<TokenOutStore>>,
    pub cc_results: Arc<Mutex<CCResultStore>>,
    pub handlers: Arc<HandlerRegistry>,
    pub peer_managers: Arc<Mutex<PeerManagerMap>>,
    pub watchdog_cancel_tokens: Arc<Mutex<WatchdogCancelStore>>,
    pub result_send_buffer: Arc<Mutex<ConstraintResultSendBuffer>>,
    pub timer_token_store: Arc<Mutex<TimerTokenStore>>,
    pub meta_token_store: Arc<Mutex<MetaTokenStore>>,
    pub loopback_q: mpsc::Sender<PeerMessage>,
}

impl AppState {
    pub fn new(
        config: NodeConfig,
        event_q: mpsc::Sender<TracedEvent>,
        loopback_q: mpsc::Sender<PeerMessage>,
    ) -> Self {
        let handlers = Arc::new(HandlerRegistry::build(&config));
        Self {
            config: Arc::new(config),
            pending: Arc::new(Mutex::new(PendingRequestManager::new())),
            event_q,
            token_in: Arc::new(Mutex::new(TokenInStore::new())),
            token_out: Arc::new(Mutex::new(TokenOutStore::new())),
            cc_results: Arc::new(Mutex::new(CCResultStore::new())),
            handlers,
            peer_managers: Arc::new(Mutex::new(PeerManagerMap::new())),
            watchdog_cancel_tokens: Arc::new(Mutex::new(WatchdogCancelStore::new())),
            result_send_buffer: Arc::new(Mutex::new(ConstraintResultSendBuffer::new())),
            timer_token_store: Arc::new(Mutex::new(TimerTokenStore::new())),
            meta_token_store: Arc::new(Mutex::new(MetaTokenStore::new())),
            loopback_q,
        }
    }
}
