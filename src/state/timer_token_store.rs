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
    /// tokens that a stop event already settled
    /// should be kept because a stop event that is earlier in event time can still arrive afterwards and that earlier stop is the one that decides the verdict
    settled_timer_tokens: HashMap<(CaseId, ConstraintId), Vec<Token>>,
    /// stop event times observed for a (CaseId, ConstraintId) pair for late corrections
    pending_stops: HashMap<(CaseId, ConstraintId), Vec<EventTime>>,
}

/// data needed to evaluate when a time stop message arrives
#[derive(Debug, Default)]
pub struct TimerStopEvaluation {
    /// tokens that were live and are settled by this stop event
    pub newly_settled: Vec<Token>,
    /// tokens whose verdict needs to be corrected because this stop event has just become the earliest qualifying one
    pub corrections: Vec<Token>,
    pub start_ever_seen: bool,
}

impl TimerStopEvaluation {
    pub fn has_tokens(&self) -> bool {
        !self.newly_settled.is_empty() || !self.corrections.is_empty()
    }
}

/// the event time a timer token started at
pub fn token_start(token: &Token) -> EventTime {
    token.event_time.unwrap_or(token.created_at)
}

/// the earliest stop time in 'stops' that is not before 'start_time'
fn earliest_stop_after(stops: &[EventTime], start_time: EventTime) -> Option<EventTime> {
    stops.iter().find(|stop| **stop >= start_time).copied()
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

    /// insert a token that is already settled, without ever making it live
    /// used when the time start event is processed after a stop event that stops it was already observed
    pub fn insert_settled_token(&mut self, token: Token) {
        let key = (token.case_id.clone(), token.constraint_id.clone());
        self.timer_start_seen.insert(key.clone());
        self.settled_timer_tokens
            .entry(key)
            .or_default()
            .push(token);
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

    /// remember that a time stop event was observed for a (case, constraint) (for late corrections)
    /// called for every time stop message so a time start event arriving afterwards can still be evaluated against it
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
        let stops = self
            .pending_stops
            .get(&(case_id.clone(), constraint_id.clone()))?;
        earliest_stop_after(stops, start_time)
    }

    /// called when the time stop message is received
    /// collects the tokens that 'stop_time' settles and whether a start was ever seen
    pub fn take_tokens_settled_by(
        &mut self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
        stop_time: EventTime,
    ) -> TimerStopEvaluation {
        let key = (case_id.clone(), constraint_id.clone());
        let start_ever_seen = self.timer_start_seen.contains(&key);
        let stops = self.pending_stops.get(&key).cloned().unwrap_or_default();

        let settles =
            |token: &Token| earliest_stop_after(&stops, token_start(token)) == Some(stop_time);

        // live tokens that this stop event settles
        let live_ids = self.timer_index.get(&key).cloned().unwrap_or_default();
        let mut newly_settled = Vec::new();
        for id in live_ids {
            let takeable = match self.timer_tokens.get(&id) {
                Some(token) => settles(token),
                None => false,
            };
            if !takeable {
                continue;
            }
            if let Some(token) = self.remove_live_timer_token(&id) {
                newly_settled.push(token);
            }
        }

        // tokens that already have a verdict but for which this stop event is now the earliest qualifying one
        let mut corrections: Vec<Token> = Vec::new();
        for pool in [&self.fired_timer_tokens, &self.settled_timer_tokens] {
            if let Some(tokens) = pool.get(&key) {
                corrections.extend(tokens.iter().filter(|t| settles(t)).cloned());
            }
        }

        // keep the newly settled tokens so a stop arriving later but earlier in event time can still correct them
        if !newly_settled.is_empty() {
            self.settled_timer_tokens
                .entry(key)
                .or_default()
                .extend(newly_settled.iter().cloned());
        }

        TimerStopEvaluation {
            newly_settled,
            corrections,
            start_ever_seen,
        }
    }

    /// take every still live token for a (case, constraint)
    /// used at case end where a timer that is still running means no stop event ever arrived
    pub fn take_live_timer_tokens(
        &mut self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
    ) -> Vec<Token> {
        let key = (case_id.clone(), constraint_id.clone());
        match self.timer_index.remove(&key) {
            Some(ids) => ids
                .iter()
                .filter_map(|id| self.timer_tokens.remove(id))
                .collect(),
            None => vec![],
        }
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
        // remove from settled token map
        self.settled_timer_tokens.remove(&key);
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

    pub fn settled_timer_token_count(
        &self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
    ) -> usize {
        self.settled_timer_tokens
            .get(&(case_id.clone(), constraint_id.clone()))
            .map_or(0, |v| v.len())
    }

    pub fn start_ever_seen(&self, case_id: &CaseId, constraint_id: &ConstraintId) -> bool {
        self.timer_start_seen
            .contains(&(case_id.clone(), constraint_id.clone()))
    }
}

#[cfg(test)]
pub mod tests {
    use crate::types::ConstraintType;

    use super::*;

    fn at(secs: i64) -> EventTime {
        chrono::TimeZone::with_ymd_and_hms(&chrono::Utc, 2014, 1, 1, 0, 0, 0).unwrap()
            + chrono::Duration::seconds(secs)
    }

    /// a timer token with an explicit start time, so pairing against stop times is testable
    /// (make_timer_token from test_tools uses Utc::now(), which cannot be paired with a fixed stop)
    fn token_at(case_id: &str, constraint_id: &str, start_secs: i64) -> Token {
        Token::new(
            case_id.to_string(),
            constraint_id.to_string(),
            ConstraintType::Time,
            Some(at(start_secs)),
        )
    }

    fn c() -> CaseId {
        "c".to_string()
    }

    fn tc() -> ConstraintId {
        "tc".to_string()
    }

    /// record a stop and immediately collect what it settles, the way the handler does
    fn stop(store: &mut TimerTokenStore, stop_secs: i64) -> TimerStopEvaluation {
        store.record_pending_stop(&c(), &tc(), at(stop_secs));
        store.take_tokens_settled_by(&c(), &tc(), at(stop_secs))
    }

    #[test]
    fn never_started_when_no_insert() {
        let mut store = TimerTokenStore::new();
        let eval = stop(&mut store, 10);
        assert!(!eval.start_ever_seen);
        assert!(!eval.has_tokens());
    }

    #[test]
    fn live_token_is_settled_by_a_later_stop() {
        let mut store = TimerTokenStore::new();
        store.insert_timer_token(token_at("c", "tc", 10));
        let eval = stop(&mut store, 20);
        assert!(eval.start_ever_seen);
        assert_eq!(eval.newly_settled.len(), 1);
        assert!(eval.corrections.is_empty());
        assert_eq!(store.timer_token_count(&c(), &tc()), 0);
        assert_eq!(store.settled_timer_token_count(&c(), &tc()), 1);
    }

    /// the sepsis log contains traces where a lab result carries an earlier timestamp than the
    /// triage event that starts the timer. such a stop must not settle the timer, otherwise the
    /// elapsed time comes out negative, gets clamped to zero and the constraint is reported as
    /// trivially satisfied
    #[test]
    fn stop_before_the_start_does_not_settle_the_timer() {
        let mut store = TimerTokenStore::new();
        store.insert_timer_token(token_at("c", "tc", 100));
        let eval = stop(&mut store, 40);
        assert!(
            !eval.has_tokens(),
            "a stop before the start settles nothing"
        );
        assert_eq!(
            store.timer_token_count(&c(), &tc()),
            1,
            "the timer has to stay live"
        );
    }

    #[test]
    fn a_later_qualifying_stop_settles_a_timer_an_earlier_stop_could_not() {
        let mut store = TimerTokenStore::new();
        store.insert_timer_token(token_at("c", "tc", 100));
        assert!(!stop(&mut store, 40).has_tokens());
        let eval = stop(&mut store, 150);
        assert_eq!(eval.newly_settled.len(), 1);
        assert_eq!(token_start(&eval.newly_settled[0]), at(100));
    }

    /// the same set of stop events has to produce the same pairing whichever order they arrive in
    #[test]
    fn earliest_qualifying_stop_wins_regardless_of_arrival_order() {
        // ascending arrival: the earliest stop settles it, the later one changes nothing
        let mut ascending = TimerTokenStore::new();
        ascending.insert_timer_token(token_at("c", "tc", 100));
        assert_eq!(stop(&mut ascending, 150).newly_settled.len(), 1);
        assert!(
            !stop(&mut ascending, 900).has_tokens(),
            "a later stop must not re-decide"
        );

        // descending arrival: the later stop settles it first, then the earlier one corrects it
        let mut descending = TimerTokenStore::new();
        descending.insert_timer_token(token_at("c", "tc", 100));
        assert_eq!(stop(&mut descending, 900).newly_settled.len(), 1);
        let second = stop(&mut descending, 150);
        assert!(second.newly_settled.is_empty());
        assert_eq!(
            second.corrections.len(),
            1,
            "the verdict has to be corrected"
        );
    }

    #[test]
    fn fired_token_is_corrected_by_a_qualifying_stop() {
        let mut store = TimerTokenStore::new();
        let t = token_at("c", "tc", 10);
        let id = t.id;
        store.insert_timer_token(t);

        // watchdog fires
        assert!(store.fire_timer_token(&id).is_some());
        // no longer live
        assert_eq!(store.timer_token_count(&c(), &tc()), 0);
        // but preserved as fired
        assert_eq!(store.fired_timer_token_count(&c(), &tc()), 1);

        // late stop arrives
        let eval = stop(&mut store, 20);
        assert!(eval.start_ever_seen);
        assert!(eval.newly_settled.is_empty());
        assert_eq!(eval.corrections.len(), 1);
        assert_eq!(eval.corrections[0].id, id);
    }

    #[test]
    fn fired_token_is_not_corrected_by_a_stop_before_its_start() {
        let mut store = TimerTokenStore::new();
        let t = token_at("c", "tc", 100);
        let id = t.id;
        store.insert_timer_token(t);
        store.fire_timer_token(&id);

        assert!(!stop(&mut store, 40).has_tokens());
    }

    #[test]
    fn mix_of_live_and_fired_tokens() {
        let mut store = TimerTokenStore::new();
        let t_fired = token_at("c", "tc", 10);
        let fired_id = t_fired.id;
        store.insert_timer_token(t_fired);
        store.insert_timer_token(token_at("c", "tc", 20)); // stays live
        store.fire_timer_token(&fired_id);

        let eval = stop(&mut store, 30);
        assert_eq!(eval.newly_settled.len(), 1);
        assert_eq!(eval.corrections.len(), 1);
    }

    #[test]
    fn one_stop_settles_every_timer_it_qualifies_for() {
        let mut store = TimerTokenStore::new();
        store.insert_timer_token(token_at("c", "tc", 10));
        store.insert_timer_token(token_at("c", "tc", 20));
        store.insert_timer_token(token_at("c", "tc", 500)); // starts after the stop
        let eval = stop(&mut store, 30);
        assert_eq!(eval.newly_settled.len(), 2);
        assert_eq!(store.timer_token_count(&c(), &tc()), 1);
    }

    #[test]
    fn settled_token_inserted_directly_can_still_be_corrected() {
        let mut store = TimerTokenStore::new();
        store.record_pending_stop(&c(), &tc(), at(900));
        store.insert_settled_token(token_at("c", "tc", 100));
        assert!(store.start_ever_seen(&c(), &tc()));

        assert_eq!(stop(&mut store, 150).corrections.len(), 1);
    }

    #[test]
    fn take_live_timer_tokens_leaves_settled_ones_alone() {
        let mut store = TimerTokenStore::new();
        store.insert_timer_token(token_at("c", "tc", 10));
        store.insert_timer_token(token_at("c", "tc", 500));
        stop(&mut store, 30); // settles the first one only

        let live = store.take_live_timer_tokens(&c(), &tc());
        assert_eq!(live.len(), 1);
        assert_eq!(token_start(&live[0]), at(500));
        assert_eq!(store.settled_timer_token_count(&c(), &tc()), 1);
    }

    #[test]
    fn no_pending_stop_when_none_recorded() {
        let store = TimerTokenStore::new();
        assert!(
            store
                .earliest_pending_stop_after(&c(), &tc(), at(10))
                .is_none()
        );
    }

    #[test]
    fn pending_stop_before_the_start_is_not_returned() {
        let mut store = TimerTokenStore::new();
        store.record_pending_stop(&c(), &tc(), at(5));
        assert!(
            store
                .earliest_pending_stop_after(&c(), &tc(), at(10))
                .is_none()
        );
    }

    #[test]
    fn earliest_pending_stop_at_or_after_the_start_is_returned() {
        let mut store = TimerTokenStore::new();
        // recorded out of order on purpose
        store.record_pending_stop(&c(), &tc(), at(50));
        store.record_pending_stop(&c(), &tc(), at(5));
        store.record_pending_stop(&c(), &tc(), at(20));

        assert_eq!(
            store.earliest_pending_stop_after(&c(), &tc(), at(10)),
            Some(at(20))
        );
        // a stop exactly at the start time still stops the timer (elapsed zero)
        assert_eq!(
            store.earliest_pending_stop_after(&c(), &tc(), at(20)),
            Some(at(20))
        );
    }

    #[test]
    fn pending_stops_are_not_consumed_by_a_lookup() {
        let mut store = TimerTokenStore::new();
        store.record_pending_stop(&c(), &tc(), at(30));
        assert_eq!(
            store.earliest_pending_stop_after(&c(), &tc(), at(10)),
            Some(at(30))
        );
        assert_eq!(
            store.earliest_pending_stop_after(&c(), &tc(), at(10)),
            Some(at(30))
        );
    }

    #[test]
    fn duplicate_pending_stops_are_deduplicated() {
        let mut store = TimerTokenStore::new();
        store.record_pending_stop(&c(), &tc(), at(30));
        store.record_pending_stop(&c(), &tc(), at(30));
        assert_eq!(
            store.pending_stops.get(&(c(), tc())).map(|v| v.len()),
            Some(1)
        );
    }

    #[test]
    fn mark_start_seen_without_a_token() {
        let mut store = TimerTokenStore::new();
        store.mark_start_seen(&c(), &tc());
        assert!(store.start_ever_seen(&c(), &tc()));
        assert_eq!(store.timer_token_count(&c(), &tc()), 0);
    }

    #[test]
    fn clear_timer_state_removes_everything() {
        let mut store = TimerTokenStore::new();
        let t = token_at("c", "tc", 10);
        let id = t.id;
        store.insert_timer_token(t);
        store.insert_timer_token(token_at("c", "tc", 20));
        store.fire_timer_token(&id);
        store.record_pending_stop(&c(), &tc(), at(30));
        store.insert_settled_token(token_at("c", "tc", 40));

        store.clear_timer_state(&c(), &tc());

        assert!(!store.start_ever_seen(&c(), &tc()));
        assert_eq!(store.timer_token_count(&c(), &tc()), 0);
        assert_eq!(store.fired_timer_token_count(&c(), &tc()), 0);
        assert_eq!(store.settled_timer_token_count(&c(), &tc()), 0);
        assert!(
            store
                .earliest_pending_stop_after(&c(), &tc(), at(0))
                .is_none()
        );
    }
}
