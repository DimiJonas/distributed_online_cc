use chrono::{DateTime, Duration, Utc};
use std::collections::HashMap;
use std::collections::hash_map::{DefaultHasher, Entry};
use std::hash::{Hash, Hasher};

use crate::types::{CaseEnd, CaseId, EventTime};

#[derive(Debug)]
pub enum CaseEndOutcome {
    New,
    Updated,
    Unchanged,
}

// it is important to differentiate new entries from updated entries
// because for new entries c case closing task has to be spawned
#[derive(Debug)]
pub struct MergeResult {
    pub new: Vec<CaseEnd>,
    pub updated: Vec<CaseEnd>,
}

// one cached case end
//
// its important to differentiate between the time propagated in the timestamp
// and the time when the event was first received when a 'not online' event stream is replayed for simulations
#[derive(Debug, Clone)]
struct CacheEntry {
    event_time: EventTime,
    observed_at: DateTime<Utc>,
}

pub struct CEGossipCache {
    // case id -> entry
    cache: HashMap<CaseId, CacheEntry>,
    ttl: Duration,
}

impl CEGossipCache {
    pub fn new(ttl: std::time::Duration) -> Self {
        let ttl = Duration::from_std(ttl).expect("case_end_propagation_ttl out of range");
        Self {
            cache: HashMap::new(),
            ttl,
        }
    }

    // check if a received case end is new or already known
    // if the timestamps differ (possibly because another end activity was configured?) then lower timestamp wins
    pub fn observe(&mut self, case_end: &CaseEnd, now: DateTime<Utc>) -> CaseEndOutcome {
        match self.cache.entry(case_end.case_id.clone()) {
            Entry::Vacant(vacant) => {
                vacant.insert(CacheEntry {
                    event_time: case_end.timestamp,
                    observed_at: now,
                });
                CaseEndOutcome::New
            }
            Entry::Occupied(mut occupied) => {
                let entry = occupied.get_mut();
                if case_end.timestamp < entry.event_time {
                    // update business timestamp to earliest, keep original observed_at
                    // (the observation time of the case end itself did not change)
                    entry.event_time = case_end.timestamp;
                    CaseEndOutcome::Updated
                } else {
                    CaseEndOutcome::Unchanged
                }
            }
        }
    }

    fn is_live(&self, observed_at: EventTime, now: DateTime<Utc>) -> bool {
        observed_at + self.ttl > now
    }

    // merge a batch from a peer
    // returns the entries that were new
    pub fn merge(
        &mut self,
        incoming: impl IntoIterator<Item = CaseEnd>,
        now: DateTime<Utc>,
    ) -> MergeResult {
        let mut result: MergeResult = MergeResult {
            new: Vec::new(),
            updated: Vec::new(),
        };
        for case_end in incoming {
            match self.observe(&case_end, now) {
                CaseEndOutcome::New => result.new.push(case_end),
                CaseEndOutcome::Updated => result.updated.push(case_end),
                CaseEndOutcome::Unchanged => {}
            }
        }
        result
    }

    // pushable state: live entries only
    pub fn live_entries(&self, now: DateTime<Utc>) -> Vec<CaseEnd> {
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
    pub fn digest(&self, now: DateTime<Utc>) -> u64 {
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
    pub fn prune(&mut self, now: DateTime<Utc>) {
        let ttl = self.ttl;
        self.cache.retain(|_, e| e.observed_at + ttl > now);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn ce(case: &str, secs: i64) -> CaseEnd {
        CaseEnd {
            case_id: case.to_string(),
            timestamp: Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap() + Duration::seconds(secs),
        }
    }

    // regression test: a case end whose event timestamp is far in the past (e.g. replayed event log) must still be accepted, gossiped and digested
    // TTL is relative to when the node observed the case end, not the events own timestamp
    #[test]
    fn old_business_timestamp_is_still_live() {
        let mut cache = CEGossipCache::new(std::time::Duration::from_secs(180));
        let now = Utc::now(); // years after 2020

        let outcome = cache.observe(&ce("case-0", 0), now);
        assert!(matches!(outcome, CaseEndOutcome::New));

        // entry is live and gossipable right after observation
        assert_eq!(cache.live_entries(now).len(), 1);
        let empty_digest = CEGossipCache::new(std::time::Duration::from_secs(180)).digest(now);
        assert_ne!(cache.digest(now), empty_digest);

        // ...and expires relative to observation time
        let later = now + Duration::seconds(181);
        assert!(cache.live_entries(later).is_empty());
        cache.prune(later);
        assert!(cache.live_entries(later).is_empty());
    }

    // merging the same case end twice yields New then Unchanged, so only one  case closing task is spawned per case
    #[test]
    fn duplicate_merge_is_unchanged() {
        let mut cache = CEGossipCache::new(std::time::Duration::from_secs(180));
        let now = Utc::now();

        let first = cache.merge(vec![ce("case-0", 0)], now);
        assert_eq!(first.new.len(), 1);
        assert!(first.updated.is_empty());

        let second = cache.merge(vec![ce("case-0", 0)], now + Duration::seconds(1));
        assert!(second.new.is_empty());
        assert!(second.updated.is_empty());
    }

    // an earlier event timestamp for a known case updates the payload but keeps the original observation time
    #[test]
    fn earlier_timestamp_updates() {
        let mut cache = CEGossipCache::new(std::time::Duration::from_secs(180));
        let now = Utc::now();

        cache.observe(&ce("case-0", 10), now);
        let outcome = cache.observe(&ce("case-0", 5), now + Duration::seconds(1));
        assert!(matches!(outcome, CaseEndOutcome::Updated));

        let live = cache.live_entries(now + Duration::seconds(2));
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].timestamp, ce("case-0", 5).timestamp);
    }

    // digests of two nodes that observed the same case end at different wall clock times must be equal (observed_at must not leak into the digest)
    #[test]
    fn digest_ignores_observation_time() {
        let mut a = CEGossipCache::new(std::time::Duration::from_secs(180));
        let mut b = CEGossipCache::new(std::time::Duration::from_secs(180));
        let now = Utc::now();

        a.observe(&ce("case-0", 0), now);
        b.observe(&ce("case-0", 0), now + Duration::seconds(30));

        let check_time = now + Duration::seconds(60);
        assert_eq!(a.digest(check_time), b.digest(check_time));
    }
}
