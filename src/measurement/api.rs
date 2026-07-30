use std::net::SocketAddr;
use std::time::Instant;

use tokio::sync::mpsc;

use crate::measurement::record::{Direction, QueueDepths, Record};
use crate::measurement::record_writer::{enabled, message_trace_enabled, record};
use crate::network::message_types::{PeerMessage, PeerMessagePayload};
use crate::state::result_send_buffer::ResultBufferPolicy;
use crate::types::{
    CaseId, ConstraintId, ConstraintResultType, ConstraintType, EventInstanceId, PeerAddress,
};

/// record one message
pub fn message(
    direction: Direction,
    peer: PeerAddress,
    message: &PeerMessage,
    bytes: usize,
    serde_us: u64,
) {
    if !message_trace_enabled() {
        return;
    }

    let payload_type = format!("{}", &message.payload);
    let (cc_msg_type, case_id, constraint_id) = match &message.payload {
        PeerMessagePayload::Conformance(cc) => (
            Some(format!("{}", &cc.message_type)),
            Some(cc.case_id.clone()),
            Some(cc.constraint_id.clone()),
        ),
        PeerMessagePayload::Result(r) => {
            (None, Some(r.case_id.clone()), Some(r.constraint_id.clone()))
        }
        PeerMessagePayload::Event(e) => (None, Some(e.case_id.clone()), None),
        _ => (None, None, None),
    };
    let gossip_msg_type = match &message.payload {
        PeerMessagePayload::CEGossip(g) => Some(g.payload.name()),
        _ => None,
    };
    let instance_id = match &message.payload {
        PeerMessagePayload::Event(e) => Some(e.instance_id),
        _ => None,
    };

    record(Record::Message {
        direction,
        peer,
        payload_type,
        cc_msg_type,
        gossip_msg_type,
        bytes,
        message_id: message.message_id.to_string(),
        serde_us,
        case_id,
        constraint_id,
        instance_id,
    });
}

/// record a message that was dropped
pub fn dropped_message(reason: &'static str, message: &PeerMessage, peer: Option<PeerAddress>) {
    record(Record::MessageDrop {
        reason,
        kind: format!("{}", &message.payload),
        peer,
        message_id: message.message_id.to_string(),
    });
}

pub fn event_input(
    case_id: &CaseId,
    event_id: &str,
    instance_id: EventInstanceId,
    parse_us: u64,
    forwarded: bool,
) {
    record(Record::EventInput {
        case_id: case_id.clone(),
        event_id: event_id.to_string(),
        instance_id,
        parse_us,
        forwarded,
    });
}

pub fn event_dequeue(
    case_id: &CaseId,
    event_id: &str,
    instance_id: EventInstanceId,
    handlers: usize,
    q_length: usize,
) {
    record(Record::EventDequeue {
        case_id: case_id.clone(),
        event_id: event_id.to_string(),
        instance_id,
        handlers,
        q_length,
    });
}

pub fn event_handled(
    case_id: &CaseId,
    constraint_id: &ConstraintId,
    instance_id: EventInstanceId,
    duration_us: u64,
) {
    record(Record::EventHandled {
        case_id: case_id.clone(),
        constraint_id: constraint_id.clone(),
        instance_id,
        duration_us,
    });
}

pub fn result_emitted(
    case_id: &CaseId,
    constraint_id: &ConstraintId,
    constraint_type: ConstraintType,
    result_type: ConstraintResultType,
    policy: &ResultBufferPolicy,
    instance_id: Option<EventInstanceId>,
    // trigger: String,
) {
    record(Record::ResultEmitted {
        case_id: case_id.clone(),
        constraint_id: constraint_id.clone(),
        constraint_type: constraint_type.to_string(),
        result: format!("{}", result_type),
        policy: format!("{}", policy),
        instance_id,
        // trigger,
    });
}

pub fn result_persisted(case_id: &CaseId, constraint_id: &ConstraintId, created_at: &str) {
    record(Record::ResultPersisted {
        case_id: case_id.clone(),
        constraint_id: constraint_id.clone(),
        created_at: created_at.to_string(),
    });
}

pub fn tcp_channel_queue_depth(peer: PeerAddress, depth: usize, capacity: usize) {
    record(Record::TCPChannelQueueDepth {
        peer,
        depth,
        capacity,
    });
}

pub fn meta(
    mode: &str,
    constraints: usize,
    listen: SocketAddr,
    overlay_listen: Option<SocketAddr>,
) {
    record(Record::Meta {
        mode: mode.to_string(),
        constraints,
        listen,
        overlay_listen,
    });
}

pub fn peer_churn(churn_event: &'static str, peer_address: PeerAddress) {
    record(Record::PeerChurn {
        event: churn_event,
        peer: peer_address,
    });
}

pub fn queues(tick: u64, depths: QueueDepths) {
    record(Record::Queues { tick, depths });
}

/// helper to create a queue depth for a bounded tokio channel
#[inline]
pub fn depth_of<T>(sender: &mpsc::Sender<T>) -> [usize; 2] {
    [
        sender.max_capacity() - sender.capacity(),
        sender.max_capacity(),
    ]
}

/// helper for timing a synchronous block
/// e.g.
/// ```ignore
/// let (value, us) = metrics::timed(|| encode(&msg));
/// ```
#[inline]
pub fn timed<T, F: FnOnce() -> T>(f: F) -> (T, u64) {
    if !enabled() {
        return (f(), 0);
    }
    let start = Instant::now();
    let value = f();
    (value, start.elapsed().as_micros() as u64)
}
