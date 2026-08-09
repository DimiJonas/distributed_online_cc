use std::collections::{HashMap, HashSet};

use crate::types::{CaseId, ConstraintId, EventTime, Token, TokenId};

/// this struct contains all the timer tokens for setting timers/spawning watchdog tasks and cancelling them if needed
#[derive(Debug, Default)]
pub struct TimerTokenStore {
    /// Timer tokens need a different key because they need to be unique for multiple events happening
    /// even though constraint id, case id and event id are the same
    timer_tokens: HashMap<TokenId, Token>,
    /// Index: (CaseId, ConstraintId) -> all active TokenIds for that pair
    timer_index: HashMap<(CaseId, ConstraintId), Vec<TokenId>>,
    /// tracks whether at least one timer start event was seen for a (CaseId, ConstraintId) pair
    /// makes it possible to distinguish "start never happened" from "all timers already fired" when a timer stop message arrives
    timer_start_seen: HashSet<(CaseId, ConstraintId)>,
    /// tokens whose watchdog already fired (timed out)
    fired_timer_tokens: HashMap<(CaseId, ConstraintId), Vec<Token>>,
    /// stop event times observed for a (CaseId, ConstraintId) pair for late corrections
    pending_stops: HashMap<(CaseId, ConstraintId), Vec<EventTime>>,
}

/// data needed to evaluate when a time stop message arrives
#[derive(Debug, Default)]
pub struct TimerStopEvaluation {
    pub live_tokens: Vec<Token>,
    pub fired_tokens: Vec<Token>,
    pub start_ever_seen: bool,
}

impl TimerStopEvaluation {
    pub fn has_tokens(&self) -> bool {
        !self.live_tokens.is_empty() || !self.fired_tokens.is_empty()
    }
}

impl TimerTokenStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert_timer_token(&mut self, token: Token) {
        let index_key = (token.case_id.clone(), token.constraint_id.clone());
        self.timer_start_seen.insert(index_key.clone());
        // update index
        self.timer_index
            .entry(index_key)
            .or_default()
            .push(token.id);
        // update token map
        self.timer_tokens.insert(token.id, token);
    }

    // removes live (still running watchdog) tokens
    fn remove_live_timer_token(&mut self, token_id: &TokenId) -> Option<Token> {
        // update token map
        let token = self.timer_tokens.remove(token_id)?;
        // update index
        let index_key = (token.case_id.clone(), token.constraint_id.clone());
        if let Some(ids) = self.timer_index.get_mut(&index_key) {
            ids.retain(|id| id != token_id);
            // remove entry from index if the entry does not contain any more token ids
            if ids.is_empty() {
                self.timer_index.remove(&index_key);
            }
        }
        Some(token)
    }

    /// moves the token from the live maps into fired map
    /// its important to keep fired tokens to preserve the start time for any time stop events coming in during the grace period and such
    ///
    /// called by a watchdog when it times out
    pub fn fire_timer_token(&mut self, token_id: &TokenId) -> Option<Token> {
        let token = self.remove_live_timer_token(token_id)?;
        let key = (token.case_id.clone(), token.constraint_id.clone());
        self.fired_timer_tokens
            .entry(key)
            .or_default()
            .push(token.clone());
        Some(token)
    }

    /// called when the time stop message is received
    /// takes the live and the fired tokens for a (case, constraint), plus whether a start was ever
    /// seen
    /// clears the tokens from the store
    /// timer_start_seen flag is left intact for diagnostics - its supposed to be cleared by clear_timer_state() at case end
    pub fn take_timer_tokens_for_evaluation(
        &mut self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
    ) -> TimerStopEvaluation {
        let key = (case_id.clone(), constraint_id.clone());
        let start_ever_seen = self.timer_start_seen.contains(&key);

        let live_tokens = match self.timer_index.remove(&key) {
            Some(ids) => ids
                .iter()
                .filter_map(|id| self.timer_tokens.remove(id))
                .collect(),
            None => vec![],
        };

        let fired_tokens = self.fired_timer_tokens.remove(&key).unwrap_or_default();

        TimerStopEvaluation {
            live_tokens,
            fired_tokens,
            start_ever_seen,
        }
    }

    /// remember that a time stop event was observed for a (case, constraint) (for late corrections)
    pub fn record_pending_stop(
        &mut self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
        stop_time: EventTime,
    ) {
        let key = (case_id.clone(), constraint_id.clone());
        let stops = self.pending_stops.entry(key).or_default();
        if let Err(pos) = stops.binary_search(&stop_time) {
            stops.insert(pos, stop_time);
        }
    }

    /// the earliest observed stop time that is not before 'start_time'
    pub fn earliest_pending_stop_after(
        &self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
        start_time: EventTime,
    ) -> Option<EventTime> {
        self.pending_stops
            .get(&(case_id.clone(), constraint_id.clone()))?
            .iter()
            .find(|stop| **stop >= start_time)
            .copied()
    }

    /// record that a start event was seen without inserting a live timer token
    /// this is used when the start event is settled immediately with an already observed stop event
    pub fn mark_start_seen(&mut self, case_id: &CaseId, constraint_id: &ConstraintId) {
        self.timer_start_seen
            .insert((case_id.clone(), constraint_id.clone()));
    }

    /// cleanup of all timer state for a (case, constraint)
    /// called for case end cleanup so state doesnt accumulate across cases
    pub fn clear_timer_state(&mut self, case_id: &CaseId, constraint_id: &ConstraintId) {
        let key = (case_id.clone(), constraint_id.clone());
        // remove from index
        if let Some(ids) = self.timer_index.remove(&key) {
            // remove from timer token map
            for id in ids {
                self.timer_tokens.remove(&id);
            }
        }
        // remove from fired token map
        self.fired_timer_tokens.remove(&key);
        // drop observed stop events
        self.pending_stops.remove(&key);
        // clean flag
        self.timer_start_seen.remove(&key);
    }

    pub fn get_timer_token(&self, token_id: &TokenId) -> Option<&Token> {
        self.timer_tokens.get(token_id)
    }

    pub fn timer_token_count(&self, case_id: &CaseId, constraint_id: &ConstraintId) -> usize {
        self.timer_index
            .get(&(case_id.clone(), constraint_id.clone()))
            .map_or(0, |v| v.len())
    }

    pub fn fired_timer_token_count(&self, case_id: &CaseId, constraint_id: &ConstraintId) -> usize {
        self.fired_timer_tokens
            .get(&(case_id.clone(), constraint_id.clone()))
            .map_or(0, |v| v.len())
    }
}

#[cfg(test)]
pub mod tests {
    use crate::util::test_tools::make_timer_token;

    use super::*;

    #[test]
    fn never_started_when_no_insert() {
        let mut store = TimerTokenStore::new();
        let eval = store.take_timer_tokens_for_evaluation(&"c".to_string(), &"tc".to_string());
        assert!(!eval.start_ever_seen);
        assert!(!eval.has_tokens());
    }

    #[test]
    fn live_tokens_found_after_insert() {
        let mut store = TimerTokenStore::new();
        store.insert_timer_token(make_timer_token("c", "tc"));
        store.insert_timer_token(make_timer_token("c", "tc"));
        let eval = store.take_timer_tokens_for_evaluation(&"c".to_string(), &"tc".to_string());
        assert!(eval.start_ever_seen);
        assert_eq!(eval.live_tokens.len(), 2);
        assert_eq!(eval.fired_tokens.len(), 0);
    }

    #[test]
    fn fired_token_preserved_and_returned_on_evaluation() {
        let mut store = TimerTokenStore::new();
        let t = make_timer_token("c", "tc");
        let id = t.id;
        store.insert_timer_token(t);

        // watchdog fires
        let fired = store.fire_timer_token(&id);
        assert!(fired.is_some());
        // no longer live
        assert_eq!(
            store.timer_token_count(&"c".to_string(), &"tc".to_string()),
            0
        );
        // but preserved as fired
        assert_eq!(
            store.fired_timer_token_count(&"c".to_string(), &"tc".to_string()),
            1
        );

        // late stop arrives
        let eval = store.take_timer_tokens_for_evaluation(&"c".to_string(), &"tc".to_string());
        assert!(eval.start_ever_seen);
        assert_eq!(eval.live_tokens.len(), 0);
        assert_eq!(eval.fired_tokens.len(), 1);
        assert_eq!(eval.fired_tokens[0].id, id);
    }

    #[test]
    fn mix_of_live_and_fired_tokens() {
        let mut store = TimerTokenStore::new();
        let t_fired = make_timer_token("c", "tc");
        let fired_id = t_fired.id;
        store.insert_timer_token(t_fired);
        store.insert_timer_token(make_timer_token("c", "tc")); // stays live
        store.fire_timer_token(&fired_id);

        let eval = store.take_timer_tokens_for_evaluation(&"c".to_string(), &"tc".to_string());
        assert_eq!(eval.live_tokens.len(), 1);
        assert_eq!(eval.fired_tokens.len(), 1);
    }

    fn at(secs: i64) -> EventTime {
        chrono::TimeZone::with_ymd_and_hms(&chrono::Utc, 2014, 1, 1, 0, 0, 0).unwrap()
            + chrono::Duration::seconds(secs)
    }

    #[test]
    fn no_pending_stop_when_none_recorded() {
        let store = TimerTokenStore::new();
        assert!(
            store
                .earliest_pending_stop_after(&"c".to_string(), &"tc".to_string(), at(10))
                .is_none()
        );
    }

    #[test]
    fn pending_stop_before_the_start_is_not_returned() {
        let mut store = TimerTokenStore::new();
        store.record_pending_stop(&"c".to_string(), &"tc".to_string(), at(5));
        assert!(
            store
                .earliest_pending_stop_after(&"c".to_string(), &"tc".to_string(), at(10))
                .is_none()
        );
    }

    #[test]
    fn earliest_pending_stop_at_or_after_the_start_is_returned() {
        let mut store = TimerTokenStore::new();
        // recorded out of order on purpose
        store.record_pending_stop(&"c".to_string(), &"tc".to_string(), at(50));
        store.record_pending_stop(&"c".to_string(), &"tc".to_string(), at(5));
        store.record_pending_stop(&"c".to_string(), &"tc".to_string(), at(20));

        assert_eq!(
            store.earliest_pending_stop_after(&"c".to_string(), &"tc".to_string(), at(10)),
            Some(at(20))
        );
        // a stop exactly at the start time still stops the timer (elapsed zero)
        assert_eq!(
            store.earliest_pending_stop_after(&"c".to_string(), &"tc".to_string(), at(20)),
            Some(at(20))
        );
    }

    #[test]
    fn pending_stops_are_not_consumed_by_a_lookup() {
        let mut store = TimerTokenStore::new();
        store.record_pending_stop(&"c".to_string(), &"tc".to_string(), at(30));
        let key = (&"c".to_string(), &"tc".to_string());
        assert_eq!(
            store.earliest_pending_stop_after(key.0, key.1, at(10)),
            Some(at(30))
        );
        assert_eq!(
            store.earliest_pending_stop_after(key.0, key.1, at(10)),
            Some(at(30))
        );
    }

    #[test]
    fn duplicate_pending_stops_are_deduplicated() {
        let mut store = TimerTokenStore::new();
        store.record_pending_stop(&"c".to_string(), &"tc".to_string(), at(30));
        store.record_pending_stop(&"c".to_string(), &"tc".to_string(), at(30));
        assert_eq!(
            store
                .pending_stops
                .get(&("c".to_string(), "tc".to_string()))
                .map(|v| v.len()),
            Some(1)
        );
    }

    #[test]
    fn mark_start_seen_without_a_token() {
        let mut store = TimerTokenStore::new();
        store.mark_start_seen(&"c".to_string(), &"tc".to_string());
        let eval = store.take_timer_tokens_for_evaluation(&"c".to_string(), &"tc".to_string());
        assert!(eval.start_ever_seen);
        assert!(!eval.has_tokens());
    }

    #[test]
    fn clear_timer_state_removes_everything() {
        let mut store = TimerTokenStore::new();
        let t = make_timer_token("c", "tc");
        let id = t.id;
        store.insert_timer_token(t);
        store.insert_timer_token(make_timer_token("c", "tc"));
        store.fire_timer_token(&id);
        store.record_pending_stop(&"c".to_string(), &"tc".to_string(), at(30));

        store.clear_timer_state(&"c".to_string(), &"tc".to_string());

        let eval = store.take_timer_tokens_for_evaluation(&"c".to_string(), &"tc".to_string());
        assert!(!eval.start_ever_seen);
        assert!(!eval.has_tokens());
        assert!(
            store
                .earliest_pending_stop_after(&"c".to_string(), &"tc".to_string(), at(0))
                .is_none()
        );
    }
}
