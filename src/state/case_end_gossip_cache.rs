use std::collections::HashMap;
use std::collections::hash_map::{DefaultHasher, Entry};
use std::hash::{Hash, Hasher};
use std::time::Duration as StdDuration;

use chrono::Duration;

use crate::types::{CaseEnd, CaseId, EventTime};

#[derive(Debug)]
pub enum CaseEndOutcome {
    New,
    Updated,
    Unchanged,
    // arrived for a case this node has already closed
    Straggler,
}

// it is important to differentiate new entries from updated entries
// because for new entries c case closing task has to be spawned
#[derive(Debug)]
pub struct MergeResult {
    pub new: Vec<CaseEnd>,
    pub updated: Vec<CaseEnd>,
    pub straggler_count: usize,
}

// one cached case end
#[derive(Debug, Clone)]
struct CacheEntry {
    event_time: EventTime,
    observed_at: EventTime,
}

/// keeps track of case end gossip information
pub struct CEGossipCache {
    // case id -> entry
    cache: HashMap<CaseId, CacheEntry>,
    ttl: Duration,
}

impl CEGossipCache {
    pub fn new(ttl: StdDuration) -> Self {
        let ttl = Duration::from_std(ttl).expect("case_end_retention_period out of range");
        Self {
            cache: HashMap::new(),
            ttl,
        }
    }

    pub fn contains(&self, case_id: &CaseId) -> bool {
        self.cache.contains_key(case_id)
    }

    /// record a case end
    /// 'already_ended' should be read from CaseStateMap
    pub fn observe(
        &mut self,
        case_end: &CaseEnd,
        observed_at: EventTime,
        already_ended: bool,
    ) -> CaseEndOutcome {
        match self.cache.entry(case_end.case_id.clone()) {
            Entry::Vacant(vacant) => {
                if already_ended {
                    // dont reinsert
                    return CaseEndOutcome::Straggler;
                }
                vacant.insert(CacheEntry {
                    event_time: case_end.timestamp,
                    observed_at,
                });
                CaseEndOutcome::New
            }
            Entry::Occupied(mut occupied) => {
                let entry = occupied.get_mut();
                if case_end.timestamp < entry.event_time {
                    // update event timestamp to earliest, but keep original observed_at
                    entry.event_time = case_end.timestamp;
                    CaseEndOutcome::Updated
                } else {
                    CaseEndOutcome::Unchanged
                }
            }
        }
    }

    fn is_live(&self, observed_at: EventTime, now: EventTime) -> bool {
        observed_at + self.ttl > now
    }

    /// merge a batch from a peer
    ///
    /// 'is_ended' is looked up per case against the ended case record
    pub fn merge(
        &mut self,
        incoming: impl IntoIterator<Item = CaseEnd>,
        now: EventTime,
        is_ended: impl Fn(&CaseId) -> bool,
    ) -> MergeResult {
        let mut result = MergeResult {
            new: Vec::new(),
            updated: Vec::new(),
            straggler_count: 0,
        };
        for case_end in incoming {
            let already_ended = is_ended(&case_end.case_id);
            match self.observe(&case_end, now, already_ended) {
                CaseEndOutcome::New => result.new.push(case_end),
                CaseEndOutcome::Updated => result.updated.push(case_end),
                CaseEndOutcome::Unchanged => {}
                CaseEndOutcome::Straggler => {
                    log::trace!(
                        "ce gossip cache: ignoring straggler case end {} (already closed on this node)",
                        case_end
                    );
                    result.straggler_count += 1;
                }
            }
        }
        result
    }

    // pushable state: live entries only
    pub fn live_entries(&self, now: EventTime) -> Vec<CaseEnd> {
        self.cache
            .iter()
            .filter(|(_, e)| self.is_live(e.observed_at, now))
            .map(|(c, e)| CaseEnd {
                case_id: c.clone(),
                timestamp: e.event_time,
            })
            .collect()
    }

    // order independent hash digest over the live set
    // DefaultHasher::new has fixed keys, so equal sets => equal digest across nodes
    // note: the digest hashes case ids and event timestamps only, never observed_at (differs per node)
    pub fn digest(&self, now: EventTime) -> u64 {
        let mut entries = self.live_entries(now);
        entries.sort_by(|a, b| a.case_id.cmp(&b.case_id)); // canonical order
        let mut h = DefaultHasher::new();
        for case_end in entries {
            case_end.case_id.hash(&mut h);
            case_end.timestamp.timestamp_millis().hash(&mut h);
        }
        h.finish()
    }

    // prune expired records
    pub fn prune(&mut self, now: EventTime) {
        let ttl = self.ttl;
        self.cache.retain(|_, e| e.observed_at + ttl > now);
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    const TTL_SECS: i64 = 60;

    fn cache() -> CEGossipCache {
        CEGossipCache::new(StdDuration::from_secs(TTL_SECS as u64))
    }

    fn ce(case: &str, secs: i64) -> CaseEnd {
        CaseEnd {
            case_id: case.to_string(),
            timestamp: Utc.with_ymd_and_hms(2014, 1, 1, 0, 0, 0).unwrap() + Duration::seconds(secs),
        }
    }

    fn never_ended(_: &CaseId) -> bool {
        false
    }

    /// a replayed case end carries a 2014 timestamp; that must not affect liveness,
    /// which is about how long *this node* keeps advertising it
    #[test]
    fn historical_timestamps_do_not_affect_liveness() {
        let mut c = cache();
        let now = Utc::now();

        assert!(matches!(
            c.observe(&ce("case-0", 0), now, false),
            CaseEndOutcome::New
        ));
        assert_eq!(c.live_entries(now).len(), 1);
        assert_ne!(c.digest(now), cache().digest(now));
    }

    #[test]
    fn expires_relative_to_observation() {
        let mut c = cache();
        let now = Utc::now();
        c.observe(&ce("case-0", 0), now, false);

        assert_eq!(
            c.live_entries(now + Duration::seconds(TTL_SECS - 1)).len(),
            1
        );

        let after = now + Duration::seconds(TTL_SECS + 1);
        assert!(c.live_entries(after).is_empty());
        c.prune(after);
        assert!(c.is_empty());
    }

    /// regression test (tombstone resurrection): an already-closed case must not come back as news
    #[test]
    fn already_closed_case_is_not_resurrected() {
        let mut c = cache();
        let now = Utc::now();
        c.observe(&ce("case-0", 0), now, false);

        let after = now + Duration::seconds(TTL_SECS + 1);
        c.prune(after);
        assert!(c.is_empty(), "precondition: local copy dropped");

        let outcome = c.observe(&ce("case-0", 0), after, true);
        assert!(
            matches!(outcome, CaseEndOutcome::Straggler),
            "a case this node already closed must never come back as New"
        );
        assert!(
            c.is_empty(),
            "and must not be reinserted, or it gets re-advertised"
        );

        let merged = c.merge(vec![ce("case-0", 0)], after, |_| true);
        assert!(merged.new.is_empty());
        assert_eq!(merged.straggler_count, 1);
    }

    /// the counterpart: a case this node never closed is news, however late
    ///
    /// this is what makes a short suppression window safe. a node that missed the
    /// original round still closes the case when it finally hears about it.
    #[test]
    fn unknown_case_is_news_however_late() {
        let mut c = cache();
        let much_later = Utc::now() + Duration::hours(6);

        let merged = c.merge(vec![ce("case-0", 0)], much_later, never_ended);
        assert_eq!(
            merged.new.len(),
            1,
            "never closed here, so it is still news"
        );
    }

    // merging the same case end twice yields New then Unchanged, so only one case closing task is spawned per case
    #[test]
    fn duplicate_merge_is_unchanged() {
        let mut c = cache();
        let now = Utc::now();

        let first = c.merge(vec![ce("case-0", 0)], now, never_ended);
        assert_eq!(first.new.len(), 1);
        assert!(first.updated.is_empty());

        // second time the case is in the cache, so it is Unchanged regardless of
        // what the ended-case record says
        let second = c.merge(vec![ce("case-0", 0)], now + Duration::seconds(1), |_| true);
        assert!(second.new.is_empty());
        assert!(second.updated.is_empty());
        assert_eq!(second.straggler_count, 0);
    }

    // an earlier event timestamp for a known case updates the payload but keeps the original observation time
    #[test]
    fn earlier_timestamp_updates() {
        let mut c = cache();
        let now = Utc::now();

        c.observe(&ce("case-0", 10), now, false);
        let outcome = c.observe(&ce("case-0", 5), now + Duration::seconds(1), true);
        assert!(matches!(outcome, CaseEndOutcome::Updated));

        let live = c.live_entries(now + Duration::seconds(2));
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].timestamp, ce("case-0", 5).timestamp);
    }

    // digests of two nodes that observed the same case end at different wall clock times must be equal
    #[test]
    fn digest_ignores_observation_time() {
        let mut a = cache();
        let mut b = cache();
        let now = Utc::now();

        a.observe(&ce("case-0", 0), now, false);
        b.observe(&ce("case-0", 0), now + Duration::seconds(30), false);

        let check = now + Duration::seconds(45);
        assert_eq!(a.digest(check), b.digest(check));
    }
}
