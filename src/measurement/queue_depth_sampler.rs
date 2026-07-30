//! periodically samples bounded channel capacity

use std::time::{Duration, Instant};

use crate::measurement::api::{depth_of, queues};
use crate::measurement::record::QueueDepths;
use crate::measurement::record_writer::enabled;
use crate::state::app_state::AppState;

pub fn spawn_queue_depth_sampler(state: AppState, interval: Duration) {
    if !enabled() {
        return;
    }
    std::thread::Builder::new()
        .name("metrics-queue-sampler".into())
        .spawn(move || {
            let mut tick: u64 = 0;
            let mut next = Instant::now();
            loop {
                next += interval;
                match next.checked_duration_since(Instant::now()) {
                    Some(duration) => {
                        std::thread::sleep(duration);
                    }
                    None => {
                        // fell behind (sample took longer than interval - maybe because of system saturation)
                        // skip the missed deadlines and realign to the current time
                        next = Instant::now() + interval;
                    }
                }
                do_sample(&state, tick);
                tick += 1;
            }
        })
        .expect("failed to spawn queue depth sampler");
}

// samples every bounded channel
pub fn do_sample(state: &AppState, tick: u64) {
    let (gossip_out_depth, gossip_in_depth) = if state.config.is_distributed() {
        (
            Some(depth_of(&state.dist().outbound_gossip_q)),
            Some(depth_of(&state.dist().inbound_gossip_q)),
        )
    } else {
        (None, None)
    };
    let depths = QueueDepths {
        event_q: depth_of(&state.event_q),
        loopback_q: depth_of(&state.loopback_q),
        result_sink_q: depth_of(&state.result_sink_channel),
        gossip_out_q: gossip_out_depth,
        gossip_in_q: gossip_in_depth,
    };
    queues(tick, depths);
}
