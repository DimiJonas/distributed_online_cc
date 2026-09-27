use crate::config::{handler_registry::HandlerRegistry, node_config::NodeConfig};
use crate::network::message_types::{CCResultMessage, CEGossipMessage, PeerMessage};
use crate::state::case_end_gossip_cache::CEGossipCache;
use crate::state::case_state_map::CaseStateMap;
use crate::state::cc_result_store::CCResultStore;
use crate::state::evaluation_store::EvaluationStore;
use crate::state::meta_token_store::MetaTokenStore;
use crate::state::peer_cache::PeerCache;
use crate::state::peer_manager_map::PeerManagerMap;
use crate::state::pending_requests::PendingRequestManager;
use crate::state::result_revision_store::ResultRevisionStore;
use crate::state::result_send_buffer::ConstraintResultSendBuffer;
use crate::state::sensor_event_store::SensorEventStore;
use crate::state::stream_clock::StreamClock;
use crate::state::timer_token_store::TimerTokenStore;
use crate::state::token_in_store::TokenInStore;
use crate::state::token_out_store::TokenOutStore;
use crate::state::watchdog_cancel_store::WatchdogCancelStore;
use crate::types::{CaseEnd, TracedEvent};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

/// lock order to avoid deadlocks:
///   case_state -> sensor_events -> evaluations -> token_in -> token_out -> timer_token_store -> meta_token_store -> watchdog_cancel_tokens -> cc_results
///     -> result_revisions -> result_send_buffer -> pending -> ce_gossip_cache
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
    pub result_revisions: Arc<Mutex<ResultRevisionStore>>,
    pub timer_token_store: Arc<Mutex<TimerTokenStore>>,
    pub meta_token_store: Arc<Mutex<MetaTokenStore>>,
    pub evaluations: Arc<Mutex<EvaluationStore>>,
    pub loopback_q: mpsc::Sender<PeerMessage>,
    pub sensor_events: Arc<Mutex<SensorEventStore>>,
    pub result_sink_channel: mpsc::Sender<CCResultMessage>,
    pub case_state: Arc<Mutex<CaseStateMap>>,
    pub stream_clock: Arc<StreamClock>,
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
            result_revisions: Arc::new(Mutex::new(ResultRevisionStore::new())),
            timer_token_store: Arc::new(Mutex::new(TimerTokenStore::new())),
            meta_token_store: Arc::new(Mutex::new(MetaTokenStore::new())),
            evaluations: Arc::new(Mutex::new(EvaluationStore::new())),
            loopback_q,
            sensor_events: Arc::new(Mutex::new(SensorEventStore::new())),
            result_sink_channel,
            case_state: Arc::new(Mutex::new(CaseStateMap::new())),
            stream_clock: Arc::new(StreamClock::new()),
            dist,
        }
    }

    pub fn new_decentralized(
        config: NodeConfig,
        event_q: mpsc::Sender<TracedEvent>,
        loopback_q: mpsc::Sender<PeerMessage>,
        result_sink_channel: mpsc::Sender<CCResultMessage>,
        outbound_gossip_q: mpsc::Sender<Vec<CaseEnd>>,
        inbound_gossip_q: mpsc::Sender<CEGossipMessage>,
    ) -> Self {
        assert!(
            config.participates_in_overlay(),
            "new_distributed without [overlay]/[gossip]"
        );
        let case_end_ttl = config.case_end_ttl;
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
            !config.participates_in_overlay(),
            "new_central with an overlay/gossip config"
        );
        Self::new_base(config, event_q, loopback_q, result_sink_channel, None)
    }

    pub fn dist(&self) -> &DistributedState {
        self.dist
            .as_ref()
            .expect("distributed-only state accessed in central mode")
    }
}
