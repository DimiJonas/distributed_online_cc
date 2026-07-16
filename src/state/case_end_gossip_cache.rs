use chrono::{DateTime, Duration, Utc};
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
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
    pub fn observe(&mut self, case_end: &CaseEnd, now: DateTime<Utc>) -> CaseEndOutcome {
        if !self.is_live(case_end.timestamp, now) {
            return CaseEndOutcome::Unchanged;
        }
        match self.cache.get(&case_end.case_id) {
            None => {
                self.cache
                    .insert(case_end.case_id.clone(), case_end.timestamp.clone());
                CaseEndOutcome::New
            }
            Some(&stored) if case_end.timestamp < stored => {
                // update timestamp to earliest
                self.cache
                    .insert(case_end.case_id.clone(), case_end.timestamp.clone());
                CaseEndOutcome::Updated
            }
            Some(_) => CaseEndOutcome::Unchanged,
        }
    }

    fn is_live(&self, event_time: EventTime, now: DateTime<Utc>) -> bool {
        event_time + self.ttl > now
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
            .filter(|(_, et)| self.is_live(**et, now))
            .map(|(c, &et)| CaseEnd {
                case_id: c.clone(),
                timestamp: et,
            })
            .collect()
    }

    // order independent hash digest over the live set
    // DefaultHasher::new has fixed keys, so equal sets => equal digest across nodes
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
        self.cache.retain(|_, &mut et| et + ttl > now);
    }
}
