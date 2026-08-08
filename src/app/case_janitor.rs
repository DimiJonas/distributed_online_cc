use chrono::{Duration as ChronoDuration, Utc};
use std::time::Duration;
use tokio::time::interval;

use crate::state::app_state::AppState;

/// expire ended cases using the stream clock
///
/// ages out the two case end windows:
/// - case_end_retention_period = how long the node advertises the case end through gossip
/// - case_end_tombstone_ttl = how long the node remembers a closed case (derived from case_end_retention_period, so it is definitely longer)
pub fn spawn_case_janitor(state: AppState, cleanup_interval: Duration) {
    let tombstone_ttl = match ChronoDuration::from_std(state.config.case_end_tombstone_ttl()) {
        Ok(ttl) => ttl,
        Err(e) => {
            log::error!(
                "case janitor: derived case end tombstone ttl out of range ({}); janitor not started",
                e
            );
            return;
        }
    };

    tokio::spawn(async move {
        let mut ticker = interval(cleanup_interval);
        loop {
            ticker.tick().await;
            let now = Utc::now();

            if state.config.is_decentralized() {
                let mut ce_cache = state.dist().ce_gossip_cache.lock().unwrap();
                ce_cache.prune(now);
            }

            let cutoff = now - tombstone_ttl;
            let forgotten = {
                let mut case_state = state.case_state.lock().unwrap();
                case_state.prune_ended(cutoff)
            };

            if forgotten > 0 {
                log::debug!("case janitor: forgot {} ended case(s)", forgotten);
            }
        }
    });
}
