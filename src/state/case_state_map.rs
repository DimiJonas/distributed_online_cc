use std::collections::HashMap;

use crate::types::{CaseId, EventTime};

/// case lifecycle: open -> ended -> closed
#[derive(Debug, Hash, Eq, PartialEq, Clone, Copy)]
pub enum CaseState {
    Open,
    Ended,
    Closed,
}

#[derive(Debug, Clone, Copy)]
struct EndedCase {
    /// timestamp of the case end event
    event_time: EventTime,
    /// time at which this case end was first observed on this node
    observed_at: EventTime,
}

pub struct CaseStateMap {
    case_state: HashMap<CaseId, CaseState>,
    /// used to guard against re-gossiping already ended cases
    ended_cases: HashMap<CaseId, EndedCase>,
}

impl CaseStateMap {
    pub fn new() -> Self {
        Self {
            case_state: HashMap::new(),
            ended_cases: HashMap::new(),
        }
    }

    pub fn has_case(&self, case_id: &CaseId) -> bool {
        self.case_state.contains_key(case_id)
    }

    pub fn is_ended(&self, case_id: &CaseId) -> bool {
        self.case_state.get(case_id) == Some(&CaseState::Ended)
    }

    pub fn is_closed(&self, case_id: &CaseId) -> bool {
        self.case_state.get(case_id) == Some(&CaseState::Closed)
    }

    pub fn open_case(&mut self, case_id: CaseId) {
        self.case_state.insert(case_id, CaseState::Open);
    }

    pub fn end_case(&mut self, case_id: CaseId) {
        self.case_state.insert(case_id, CaseState::Ended);
    }

    pub fn close_case(&mut self, case_id: CaseId) {
        self.case_state.insert(case_id, CaseState::Closed);
    }

    pub fn remove(&mut self, case_id: &CaseId) -> Option<CaseState> {
        self.case_state.remove(case_id)
    }

    /// record that this case has ended and return whether that is news
    pub fn mark_ended(
        &mut self,
        case_id: &CaseId,
        end_time: EventTime,
        observed_at: EventTime,
    ) -> bool {
        match self.ended_cases.get_mut(case_id) {
            Some(known) => {
                if end_time < known.event_time {
                    known.event_time = end_time;
                }
                false
            }
            None => {
                self.ended_cases.insert(
                    case_id.clone(),
                    EndedCase {
                        event_time: end_time,
                        observed_at,
                    },
                );
                true
            }
        }
    }

    pub fn case_end_time(&self, case_id: &CaseId) -> Option<EventTime> {
        self.ended_cases.get(case_id).map(|ended| ended.event_time)
    }

    pub fn has_ended_record(&self, case_id: &CaseId) -> bool {
        self.ended_cases.contains_key(case_id)
    }

    /// forget ended cases
    pub fn prune_ended(&mut self, cutoff: EventTime) -> usize {
        let stale: Vec<CaseId> = self
            .ended_cases
            .iter()
            .filter(|(_, ended)| ended.observed_at <= cutoff)
            .map(|(case_id, _)| case_id.clone())
            .collect();
        for case_id in &stale {
            self.ended_cases.remove(case_id);
            self.case_state.remove(case_id);
        }
        stale.len()
    }

    pub fn ended_count(&self) -> usize {
        self.ended_cases.len()
    }

    pub fn case_count(&self) -> usize {
        self.case_state.len()
    }
}

impl Default for CaseStateMap {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, TimeZone, Utc};

    /// an event time on the (replayed, historical) stream timeline
    fn ev(secs: i64) -> EventTime {
        Utc.with_ymd_and_hms(2014, 1, 1, 0, 0, 0).unwrap() + Duration::seconds(secs)
    }

    #[test]
    fn first_mark_ended_wins() {
        let mut m = CaseStateMap::new();
        let case = "c1".to_string();
        let now = Utc::now();
        assert!(m.mark_ended(&case, ev(10), now));
        assert!(
            !m.mark_ended(&case, ev(10), now),
            "second signal is not news"
        );
        assert_eq!(m.case_end_time(&case), Some(ev(10)));
    }

    #[test]
    fn earlier_end_time_lowers_but_is_not_news() {
        let mut m = CaseStateMap::new();
        let case = "c1".to_string();
        let now = Utc::now();
        m.mark_ended(&case, ev(10), now);
        assert!(!m.mark_ended(&case, ev(5), now));
        assert_eq!(m.case_end_time(&case), Some(ev(5)));
    }

    /// the record must outlive `close_case` - that was the bug
    #[test]
    fn ended_record_survives_close_and_remove() {
        let mut m = CaseStateMap::new();
        let case = "c1".to_string();
        let now = Utc::now();
        m.open_case(case.clone());
        m.mark_ended(&case, ev(10), now);
        m.close_case(case.clone());
        m.remove(&case);

        assert!(!m.has_case(&case));
        assert!(m.has_ended_record(&case), "the ended record must persist");
        assert!(
            !m.mark_ended(&case, ev(10), now),
            "so a re-gossiped case end is not news"
        );
    }

    /// pruning is on local observation time, not on the case end's event time
    ///
    /// a replayed 2014 timestamp must not make the record instantly stale - that
    /// would drop the dedup guard immediately and reopen the resurrection window.
    #[test]
    fn prune_uses_observation_time_not_event_time() {
        let mut m = CaseStateMap::new();
        let case = "c1".to_string();
        let observed = Utc::now();
        m.open_case(case.clone());
        m.mark_ended(&case, ev(10), observed);

        // the event time is over a decade old; the record is still fresh
        m.prune_ended(observed - Duration::seconds(1));
        assert!(m.has_ended_record(&case));

        m.prune_ended(observed);
        assert!(!m.has_ended_record(&case));
        assert!(!m.has_case(&case), "case_state goes with it");
    }
}
