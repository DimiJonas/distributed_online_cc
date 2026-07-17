use crate::config::{handler_registry::HandlerRegistry, node_config::NodeConfig};
use crate::network::message_types::{CCResultMessage, CEGossipMessage, PeerMessage};
use crate::state::case_end_gossip_cache::CEGossipCache;
use crate::state::case_state_map::CaseStateMap;
use crate::state::cc_result_store::CCResultStore;
use crate::state::meta_token_store::MetaTokenStore;
use crate::state::peer_cache::PeerCache;
use crate::state::peer_manager_map::PeerManagerMap;
use crate::state::pending_requests::PendingRequestManager;
use crate::state::result_send_buffer::ConstraintResultSendBuffer;
use crate::state::sensor_event_store::SensorEventStore;
use crate::state::timer_token_store::TimerTokenStore;
use crate::state::token_in_store::TokenInStore;
use crate::state::token_out_store::TokenOutStore;
use crate::state::watchdog_cancel_store::WatchdogCancelStore;
use crate::types::{CaseEnd, TracedEvent};
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
    pub sensor_events: Arc<Mutex<SensorEventStore>>,
    pub result_sink_channel: mpsc::Sender<CCResultMessage>,
    pub case_state: Arc<Mutex<CaseStateMap>>,
    pub dist: Option<Arc<DistributedState>>,
}

pub struct DistributedState {
    pub peer_cache: Arc<Mutex<PeerCache>>,
    pub ce_gossip_cache: Arc<Mutex<CEGossipCache>>,
    pub outbound_gossip_q: mpsc::Sender<Vec<CaseEnd>>,
    pub inbound_gossip_q: mpsc::Sender<CEGossipMessage>,
}

impl AppState {
    fn new_base(
        config: NodeConfig,
        event_q: mpsc::Sender<TracedEvent>,
        loopback_q: mpsc::Sender<PeerMessage>,
        result_sink_channel: mpsc::Sender<CCResultMessage>,
        dist: Option<Arc<DistributedState>>,
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
            sensor_events: Arc::new(Mutex::new(SensorEventStore::new())),
            result_sink_channel,
            case_state: Arc::new(Mutex::new(CaseStateMap::new())),
            dist,
        }
    }

    pub fn new_distributed(
        config: NodeConfig,
        event_q: mpsc::Sender<TracedEvent>,
        loopback_q: mpsc::Sender<PeerMessage>,
        result_sink_channel: mpsc::Sender<CCResultMessage>,
        outbound_gossip_q: mpsc::Sender<Vec<CaseEnd>>,
        inbound_gossip_q: mpsc::Sender<CEGossipMessage>,
    ) -> Self {
        assert!(
            config.is_distributed(),
            "new_distributed with a central config"
        );
        let case_end_ttl = config.case_end_retention_period;
        let dist = Arc::new(DistributedState {
            peer_cache: Arc::new(Mutex::new(PeerCache::new())),
            ce_gossip_cache: Arc::new(Mutex::new(CEGossipCache::new(case_end_ttl))),
            outbound_gossip_q,
            inbound_gossip_q,
        });
        Self::new_base(config, event_q, loopback_q, result_sink_channel, Some(dist))
    }

    pub fn new_central(
        config: NodeConfig,
        event_q: mpsc::Sender<TracedEvent>,
        loopback_q: mpsc::Sender<PeerMessage>,
        result_sink_channel: mpsc::Sender<CCResultMessage>,
    ) -> Self {
        assert!(
            !config.is_distributed(),
            "new_central with a distributed config"
        );
        Self::new_base(config, event_q, loopback_q, result_sink_channel, None)
    }

    pub fn dist(&self) -> &DistributedState {
        self.dist
            .as_ref()
            .expect("distributed-only state accessed in central mode")
    }
}
