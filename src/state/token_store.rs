use crate::types::{CaseId, ConstraintId, EventTime, Token, TokenId};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Default)]
pub struct TokenStore {
    pub token_out: HashMap<(CaseId, ConstraintId), Token>,
    pub token_in: HashMap<(CaseId, ConstraintId), Token>,

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

impl TokenStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn produce_with_overwrite(&mut self, token: Token) {
        let case_id = token.case_id.clone();
        let constraint_id = token.constraint_id.clone();
        if self.contains_out_token(&case_id, &constraint_id) {
            log::debug!(
                "produce: token for ({}, {}) already exists, overwriting",
                case_id,
                constraint_id
            );
        }
        self.token_out.insert((case_id, constraint_id), token);
    }

    /// returns
    ///     - true if token was inserted
    ///     - false if token was not inserted because there already exists a token for the same key
    pub fn produce_without_overwrite(&mut self, token: Token) -> bool {
        let case_id = token.case_id.clone();
        let constraint_id = token.constraint_id.clone();
        let mut inserted = false;
        if self.contains_out_token(&case_id, &constraint_id) {
            log::warn!(
                "produce: token for ({}, {}) already exists, keeping old token",
                case_id,
                constraint_id
            );
        } else {
            self.token_out.insert((case_id, constraint_id), token);
            inserted = true;
        }
        inserted
    }

    pub fn receive_with_overwrite(&mut self, token: Token) {
        let case_id = token.case_id.clone();
        let constraint_id = token.constraint_id.clone();
        if self.contains_in_token(&case_id, &constraint_id) {
            log::warn!(
                "receive: token for ({}, {}) already exists, overwriting",
                case_id,
                constraint_id
            );
        }
        self.token_in.insert((case_id, constraint_id), token);
    }

    pub fn receive_without_overwrite(&mut self, token: Token) {
        let case_id = token.case_id.clone();
        let constraint_id = token.constraint_id.clone();
        if self.contains_in_token(&case_id, &constraint_id) {
            log::warn!(
                "receive: token for ({}, {}) already exists, keeping old token",
                case_id,
                constraint_id
            );
        } else {
            self.token_in.insert((case_id, constraint_id), token);
        }
    }

    pub fn remove_out(&mut self, case_id: &CaseId, constraint_id: &ConstraintId) -> Option<Token> {
        self.token_out
            .remove(&(case_id.clone(), constraint_id.clone()))
    }

    pub fn remove_out_before(
        &mut self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
        timestamp: EventTime,
    ) -> Option<Token> {
        if let Some(token) = self
            .token_out
            .get(&(case_id.clone(), constraint_id.clone()))
        {
            let token_time = {
                if let Some(event_time) = token.event_time {
                    event_time
                } else {
                    token.created_at
                }
            };
            if token_time < timestamp {
                return self
                    .token_out
                    .remove(&(case_id.clone(), constraint_id.clone()));
            }
        }
        None
    }

    pub fn remove_in(&mut self, case_id: &CaseId, constraint_id: &ConstraintId) -> Option<Token> {
        self.token_in
            .remove(&(case_id.clone(), constraint_id.clone()))
    }

    pub fn contains_out_token(&self, case_id: &CaseId, constraint_id: &ConstraintId) -> bool {
        self.token_out
            .contains_key(&(case_id.clone(), constraint_id.clone()))
    }

    pub fn contains_out_token_before(
        &self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
        timestamp: EventTime,
    ) -> bool {
        if let Some(token) = self
            .token_out
            .get(&(case_id.clone(), constraint_id.clone()))
        {
            let token_time = {
                if let Some(event_time) = token.event_time {
                    event_time
                } else {
                    token.created_at
                }
            };
            return token_time < timestamp;
        }
        false
    }

    pub fn get_out_mut(
        &mut self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
    ) -> Option<&mut Token> {
        self.token_out
            .get_mut(&(case_id.clone(), constraint_id.clone()))
    }

    pub fn get_out_cloned(&self, case_id: &CaseId, constraint_id: &ConstraintId) -> Option<Token> {
        self.token_out
            .get(&(case_id.clone(), constraint_id.clone()))
            .cloned()
    }

    pub fn get_in_mut(
        &mut self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
    ) -> Option<&mut Token> {
        self.token_in
            .get_mut(&(case_id.clone(), constraint_id.clone()))
    }

    pub fn get_in_cloned(&self, case_id: &CaseId, constraint_id: &ConstraintId) -> Option<Token> {
        self.token_in
            .get(&(case_id.clone(), constraint_id.clone()))
            .cloned()
    }

    pub fn contains_in_token(&self, case_id: &CaseId, constraint_id: &ConstraintId) -> bool {
        self.token_in
            .contains_key(&(case_id.clone(), constraint_id.clone()))
    }

    // are there still open obligations e.g. are there tokens in token_out
    pub fn has_open_token(&self, case_id: &CaseId, constraint_id: &ConstraintId) -> bool {
        self.token_out
            .contains_key(&(case_id.clone(), constraint_id.clone()))
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

    /// Called by a stop-message handler. Atomically takes *both* the live and
    /// the fired tokens for a (case, constraint), plus whether a start was ever
    /// seen. Clears them from the store (the index for live, the fired vec for
    /// fired). The `timer_start_seen` flag is left intact for diagnostics; it's
    /// cleared by `clear_timer_state` at case end.
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

    /// cleanup of all timer state for a (case, constraint)
    ///
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
mod tests {
    use super::*;
    use crate::util::test_tools::{make_timer_token, make_token};

    #[test]
    fn produce_and_has_open_token() {
        let mut store = TokenStore::new();
        store.produce_with_overwrite(make_token("case-1", "constraint-a"));
        assert!(store.has_open_token(&"case-1".to_string(), &"constraint-a".to_string()));
    }

    #[test]
    fn remove_out_returns_token_and_clears_it() {
        let mut store = TokenStore::new();
        store.produce_with_overwrite(make_token("case-1", "constraint-a"));
        assert!(
            store
                .remove_out(&"case-1".to_string(), &"constraint-a".to_string())
                .is_some()
        );
        assert!(
            store
                .remove_out(&"case-1".to_string(), &"constraint-a".to_string())
                .is_none()
        );
    }

    #[test]
    fn never_started_when_no_insert() {
        let mut store = TokenStore::new();
        let eval = store.take_timer_tokens_for_evaluation(&"c".to_string(), &"tc".to_string());
        assert!(!eval.start_ever_seen);
        assert!(!eval.has_tokens());
    }

    #[test]
    fn live_tokens_found_after_insert() {
        let mut store = TokenStore::new();
        store.insert_timer_token(make_timer_token("c", "tc"));
        store.insert_timer_token(make_timer_token("c", "tc"));
        let eval = store.take_timer_tokens_for_evaluation(&"c".to_string(), &"tc".to_string());
        assert!(eval.start_ever_seen);
        assert_eq!(eval.live_tokens.len(), 2);
        assert_eq!(eval.fired_tokens.len(), 0);
    }

    #[test]
    fn fired_token_preserved_and_returned_on_evaluation() {
        let mut store = TokenStore::new();
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
        let mut store = TokenStore::new();
        let t_fired = make_timer_token("c", "tc");
        let fired_id = t_fired.id;
        store.insert_timer_token(t_fired);
        store.insert_timer_token(make_timer_token("c", "tc")); // stays live
        store.fire_timer_token(&fired_id);

        let eval = store.take_timer_tokens_for_evaluation(&"c".to_string(), &"tc".to_string());
        assert_eq!(eval.live_tokens.len(), 1);
        assert_eq!(eval.fired_tokens.len(), 1);
    }

    #[test]
    fn clear_timer_state_removes_everything() {
        let mut store = TokenStore::new();
        let t = make_timer_token("c", "tc");
        let id = t.id;
        store.insert_timer_token(t);
        store.insert_timer_token(make_timer_token("c", "tc"));
        store.fire_timer_token(&id);

        store.clear_timer_state(&"c".to_string(), &"tc".to_string());

        let eval = store.take_timer_tokens_for_evaluation(&"c".to_string(), &"tc".to_string());
        assert!(!eval.start_ever_seen);
        assert!(!eval.has_tokens());
    }

    #[test]
    fn timer_tokens_independent_of_token_out() {
        let mut store = TokenStore::new();
        store.produce_with_overwrite(make_token("c", "x"));
        store.insert_timer_token(make_timer_token("c", "x"));
        store.remove_out(&"c".to_string(), &"x".to_string());
        assert_eq!(
            store.timer_token_count(&"c".to_string(), &"x".to_string()),
            1
        );
    }
}
