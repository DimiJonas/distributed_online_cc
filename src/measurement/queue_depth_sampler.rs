//! periodically samples queue depth
//!
//! two intervals:
//!   * 'resolution' - how often the depths are read
//!   * 'report_interval' - how often a record is written
//!
//! each record contains the depth at report time and the maximum depth seen since the previous record

use std::time::{Duration, Instant};

use crate::measurement::api::{depth_of, queues};
use crate::measurement::record::QueueDepths;
use crate::measurement::record_writer::enabled;
use crate::state::app_state::AppState;

/// one queue: [depth_now, max_since_last_report, capacity]
type Depth = [usize; 3];

#[derive(Default, Clone, Copy)]
struct Accumulator {
    // queue depth: amount of elements inside and max capacity
    last: [usize; 2],
    // max elements inside measured since last report interval
    max: usize,
}

impl Accumulator {
    fn observe(&mut self, sample: [usize; 2]) {
        self.last = sample;
        self.max = self.max.max(sample[0]);
    }

    /// emit and reset the max
    fn take(&mut self) -> Depth {
        let out = [self.last[0], self.max, self.last[1]];
        self.max = 0;
        out
    }
}

#[derive(Default)]
struct Accumulators {
    event_q: Accumulator,
    loopback_q: Accumulator,
    result_sink_q: Accumulator,
    gossip_out_q: Accumulator,
    gossip_in_q: Accumulator,
}

pub fn spawn_queue_depth_sampler(state: AppState) {
    if !enabled() {
        return;
    }

    let report_interval = state.config.metrics.queue_report_interval;
    // never read more often than the report interval and never slower than 1 ms
    let resolution = state
        .config
        .metrics
        .queue_sample_resolution
        .clamp(Duration::from_millis(1), report_interval);

    std::thread::Builder::new()
        .name("metrics-queue-sampler".into())
        .spawn(move || {
            let mut tick: u64 = 0;
            let mut accumulators = Accumulators::default();
            let mut next_sample = Instant::now();
            let mut next_report = Instant::now() + report_interval;

            loop {
                next_sample += resolution;
                match next_sample.checked_duration_since(Instant::now()) {
                    Some(duration) => std::thread::sleep(duration),
                    None => {
                        // sampling fell behind so skip the missed deadlines and realign to the current time
                        next_sample = Instant::now() + resolution;
                    }
                }

                observe(&state, &mut accumulators);

                if Instant::now() >= next_report {
                    do_report(&state, &mut accumulators, tick);
                    tick += 1;
                    next_report += report_interval;
                    if next_report <= Instant::now() {
                        next_report = Instant::now() + report_interval;
                    }
                }
            }
        })
        .expect("failed to spawn queue depth sampler");
}

// read every channel once
fn observe(state: &AppState, accumulators: &mut Accumulators) {
    accumulators.event_q.observe(depth_of(&state.event_q));
    accumulators.loopback_q.observe(depth_of(&state.loopback_q));
    accumulators
        .result_sink_q
        .observe(depth_of(&state.result_sink_channel));
    if state.dist.is_some() {
        accumulators
            .gossip_out_q
            .observe(depth_of(&state.dist().outbound_gossip_q));
        accumulators
            .gossip_in_q
            .observe(depth_of(&state.dist().inbound_gossip_q));
    }
}

fn do_report(state: &AppState, accumulators: &mut Accumulators, tick: u64) {
    let (gossip_out_depth, gossip_in_depth) = if state.dist.is_some() {
        (
            Some(accumulators.gossip_out_q.take()),
            Some(accumulators.gossip_in_q.take()),
        )
    } else {
        (None, None)
    };
    let depths = QueueDepths {
        event_q: accumulators.event_q.take(),
        loopback_q: accumulators.loopback_q.take(),
        result_sink_q: accumulators.result_sink_q.take(),
        gossip_out_q: gossip_out_depth,
        gossip_in_q: gossip_in_depth,
    };
    queues(tick, depths);
}

/// for tests: a single immediate sample with no history
pub fn do_sample(state: &AppState, tick: u64) {
    let mut accumulators = Accumulators::default();
    observe(state, &mut accumulators);
    do_report(state, &mut accumulators, tick);
}
