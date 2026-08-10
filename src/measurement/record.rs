use std::net::SocketAddr;

use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::types::{CaseId, ConstraintId, EventInstanceId, PeerAddress};

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Sent,
    Received,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChurnEvent {
    Created,
    Removed,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResultTrigger {
    Event,
    CaseEnd,
    Timer,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CaseEndSource {
    EndEvent,
    PushGossip,
    AntiEntropy,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct QueueDepths {
    pub event_q: [usize; 2],
    pub loopback_q: [usize; 2],
    pub result_sink_q: [usize; 2],
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gossip_out_q: Option<[usize; 2]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gossip_in_q: Option<[usize; 2]>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Record {
    /// one message being sent
    /// the attacker probably sees the subset {ts_*, node_id, dir, peer, net, bytes}.
    Message {
        direction: Direction,
        #[serde(flatten)]
        peer: PeerAddress,
        /// payload variant: hello | cli | conformance | overlay | ce_gossip | result | event
        payload_type: String,
        /// type for conformance messages
        cc_msg_type: Option<String>,
        /// type for gossip messages
        gossip_msg_type: Option<String>,
        /// frame size including 4 byte length prefix
        bytes: usize,
        message_id: String,
        /// time spent in serde for this message in microseconds
        serde_us: u64,
        case_id: Option<CaseId>,
        constraint_id: Option<ConstraintId>,
        /// present on Event messages to follow one event from forwarder to central
        instance_id: Option<EventInstanceId>,
    },

    /// when a message was discarded (one of the overload signals)
    MessageDrop {
        reason: &'static str,
        kind: String,
        #[serde(flatten)]
        peer: Option<PeerAddress>,
        message_id: String,
    },

    EventInput {
        case_id: CaseId,
        event_id: String,
        instance_id: EventInstanceId,
        /// time spent parsing XES for the whole socket payload in microseconds
        parse_us: u64,
        /// true on forwarder nodes
        forwarded: bool,
    },

    /// event q processor dequeues an event and starts processing
    EventDequeue {
        case_id: CaseId,
        event_id: String,
        instance_id: EventInstanceId,
        /// how many handlers will be dispatched for this event
        handlers: usize,
        q_length: usize,
    },

    /// one constraint handler finished processing one event
    EventHandled {
        case_id: CaseId,
        constraint_id: ConstraintId,
        instance_id: EventInstanceId,
        /// in microseconds
        duration_us: u64,
    },

    /// a result was produced (not yet written to db)
    ResultEmitted {
        case_id: CaseId,
        constraint_id: ConstraintId,
        constraint_type: String,
        result: String,
        /// immediate / grace period / revocable
        policy: String,
        /// set when the emitting handler knows which event triggered the check (None if case_end or timer triggered)
        instance_id: Option<EventInstanceId>,
        // TODO: when aggregating measurements: substract timer duration from the detection delay if needed.
        trigger: ResultTrigger,
        decided_at: DateTime<Utc>,
    },

    /// the result sink persisted a row
    ResultPersisted {
        case_id: CaseId,
        constraint_id: ConstraintId,
        /// created_at as carried in the cc result message
        created_at: String,
    },

    /// sampled link queue depth
    TCPChannelQueueDepth {
        #[serde(flatten)]
        peer: PeerAddress,
        depth: usize,
        capacity: usize,
    },

    /// emitted once at startup for every run
    Meta {
        mode: String,
        constraints: usize,
        listen: SocketAddr,
        #[serde(skip_serializing_if = "Option::is_none")]
        overlay_listen: Option<SocketAddr>,
    },

    /// emitted when a peer manager is created or removed
    PeerChurn {
        event: ChurnEvent,
        #[serde(flatten)]
        peer: PeerAddress,
    },

    /// record the long lived queues that will signal saturation when they get filled up
    Queues {
        tick: u64,
        #[serde(flatten)]
        depths: QueueDepths,
    },

    /// a node learned that a case has ended
    CaseEndSeen {
        case_id: CaseId,
        /// end_event | push_gossip | anti_entropy
        source: CaseEndSource,
        source_peer: Option<SocketAddr>,
    },
}
