use chrono::{DateTime, Duration, Utc};
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::types::{CaseId, EventTime};

#[derive(Debug)]
pub enum CaseEndOutcome {
    New,
    Updated,
    Unchanged,
}

#[derive(Debug)]
pub struct MergeResult {
    pub new: Vec<(CaseId, EventTime)>,
    pub updated: Vec<(CaseId, EventTime)>,
}

pub struct CEGossipCache {
    // case id -> event time
    cache: HashMap<CaseId, EventTime>,
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
    pub fn observe(
        &mut self,
        case_id: &CaseId,
        ts: EventTime,
        now: DateTime<Utc>,
    ) -> CaseEndOutcome {
        if !self.is_live(ts, now) {
            return CaseEndOutcome::Unchanged;
        }
        match self.cache.get(case_id) {
            None => {
                self.cache.insert(case_id.clone(), ts);
                CaseEndOutcome::New
            }
            Some(&stored) if ts < stored => {
                // update timestamp to earliest
                self.cache.insert(case_id.clone(), ts);
                CaseEndOutcome::Updated
            }
            Some(_) => CaseEndOutcome::Unchanged,
        }
    }

    fn is_live(&self, event_time: EventTime, now: DateTime<Utc>) -> bool {
        event_time + self.ttl > now
    }

    // insert a case end
    // returns true if it is new
    // rejects already known cases and already expired cases (resurrection guard)
    pub fn insert_new(
        &mut self,
        case_id: &CaseId,
        event_time: EventTime,
        now: DateTime<Utc>,
    ) -> bool {
        if !self.is_live(event_time, now) {
            return false; // past ts+P everywhere: never re-import
        }
        if self.cache.contains_key(case_id) {
            return false; // duplicate: idempotent no-op
        }
        self.cache.insert(case_id.clone(), event_time);
        true
    }

    // merge a batch from a peer
    // returns the entries that were new
    pub fn merge(
        &mut self,
        incoming: impl IntoIterator<Item = (CaseId, EventTime)>,
        now: DateTime<Utc>,
    ) -> MergeResult {
        let mut result: MergeResult = MergeResult {
            new: Vec::new(),
            updated: Vec::new(),
        };
        for (case_id, event_time) in incoming {
            match self.observe(&case_id, event_time, now) {
                CaseEndOutcome::New => result.new.push((case_id, event_time)),
                CaseEndOutcome::Updated => result.updated.push((case_id, event_time)),
                CaseEndOutcome::Unchanged => {}
            }
        }
        result
    }

    // pushable state: live entries only
    pub fn live_entries(&self, now: DateTime<Utc>) -> Vec<(CaseId, EventTime)> {
        self.cache
            .iter()
            .filter(|(_, et)| self.is_live(**et, now))
            .map(|(c, &et)| (c.clone(), et))
            .collect()
    }

    // order independent hash digest over the live set
    // DefaultHasher::new has fixed keys, so equal sets => equal digest across nodes
    pub fn digest(&self, now: DateTime<Utc>) -> u64 {
        let mut entries = self.live_entries(now);
        entries.sort_by(|a, b| a.0.cmp(&b.0)); // canonical order
        let mut h = DefaultHasher::new();
        for (cid, et) in entries {
            cid.hash(&mut h);
            et.timestamp_millis().hash(&mut h);
        }
        h.finish()
    }

    // prune expired records
    pub fn prune(&mut self, now: DateTime<Utc>) {
        let ttl = self.ttl;
        self.cache.retain(|_, &mut et| et + ttl > now);
    }
}
