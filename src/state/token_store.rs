use crate::types::{CaseId, ConstraintId, EventTime, Token, TokenId};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Default)]
pub struct TokenStore {
    pub token_out: HashMap<(CaseId, ConstraintId), Token>,
    pub token_in: HashMap<(CaseId, ConstraintId), Token>,

    /// Timer tokens need a different key because they need to be unique for multiple events happening
    /// even though constraint id, case id and event id are the same
    ///
    /// Flat map: TokenId → Token (one entry per active Part A event)
    timer_tokens: HashMap<TokenId, Token>,
    /// Index: (CaseId, ConstraintId) → all active TokenIds for that pair
    timer_index: HashMap<(CaseId, ConstraintId), Vec<TokenId>>,
    /// Tracks whether at least one timer start event was seen for a
    /// (CaseId, ConstraintId) pair, regardless of whether its token is still
    /// alive
    /// Makes it possible to distinguish "start never happened" from "all timers
    /// already fired" when a timer stop message arrives
    timer_start_seen: HashSet<(CaseId, ConstraintId)>,
}

/// state of time start node when time stop message comes in
#[derive(Debug, PartialEq)]
pub enum TimerStopOutcome {
    /// at least one live token was found and removed
    LiveTokensFound(Vec<Token>),
    /// Start events happened but all timers already fired before stop arrived.
    AllTimersFired,
    /// No start event was ever seen for this (case, constraint).
    NeverStarted,
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

    pub fn remove_timer_token(&mut self, token_id: &TokenId) -> Option<Token> {
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

    /// Called by a stop message handler. Returns a typed outcome either containing the live tokens to be cancelled
    /// or if no tokens are returned its still distinguishable if all timers fired or there were never any tokens because no timer start event happened
    pub fn take_timer_tokens_with_outcome(
        &mut self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
    ) -> TimerStopOutcome {
        let index_key = (case_id.clone(), constraint_id.clone());
        let start_seen = self.timer_start_seen.contains(&index_key);

        let ids = self.timer_index.remove(&index_key);

        match (start_seen, ids) {
            (false, _) => TimerStopOutcome::NeverStarted,
            (true, None) => TimerStopOutcome::AllTimersFired,
            (true, Some(ids)) => {
                let tokens = ids
                    .iter()
                    .filter_map(|id| self.timer_tokens.remove(id))
                    .collect();
                TimerStopOutcome::LiveTokensFound(tokens)
            }
        }
    }

    /// Clear the timer_start_seen flag for a (case, constraint)
    /// this is called for case end cleanup so state doesnt accumulate across cases
    pub fn clear_timer_start_seen(&mut self, case_id: &CaseId, constraint_id: &ConstraintId) {
        self.timer_start_seen
            .remove(&(case_id.clone(), constraint_id.clone()));
    }

    /// borrow a timer token by token id
    pub fn get_timer_token(&self, token_id: &TokenId) -> Option<&Token> {
        self.timer_tokens.get(token_id)
    }

    /// how many open timer tokens exist for a given (case, constraint)
    pub fn timer_token_count(&self, case_id: &CaseId, constraint_id: &ConstraintId) -> usize {
        self.timer_index
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
    fn no_open_token_before_produce() {
        let store = TokenStore::new();
        assert!(!store.has_open_token(&"case-1".to_string(), &"constraint-a".to_string()));
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
        assert!(!store.has_open_token(&"case-1".to_string(), &"constraint-a".to_string()));
    }

    #[test]
    fn timer_outcome_never_started_when_no_insert() {
        let mut store = TokenStore::new();
        let outcome =
            store.take_timer_tokens_with_outcome(&"case-1".to_string(), &"tc".to_string());
        assert_eq!(outcome, TimerStopOutcome::NeverStarted);
    }

    #[test]
    fn timer_outcome_live_tokens_found_after_insert() {
        let mut store = TokenStore::new();
        store.insert_timer_token(make_timer_token("case-1", "tc"));
        store.insert_timer_token(make_timer_token("case-1", "tc"));
        let outcome =
            store.take_timer_tokens_with_outcome(&"case-1".to_string(), &"tc".to_string());
        assert!(matches!(
            outcome,
            TimerStopOutcome::LiveTokensFound(ref v) if v.len() == 2
        ));
    }

    #[test]
    fn timer_outcome_all_timers_fired_after_remove() {
        let mut store = TokenStore::new();
        let t = make_timer_token("case-1", "tc");
        let id = t.id;
        store.insert_timer_token(t);
        store.remove_timer_token(&id);

        let outcome =
            store.take_timer_tokens_with_outcome(&"case-1".to_string(), &"tc".to_string());
        assert_eq!(outcome, TimerStopOutcome::AllTimersFired);
    }

    #[test]
    fn timer_start_seen_cleared_on_cleanup() {
        let mut store = TokenStore::new();
        store.insert_timer_token(make_timer_token("case-1", "tc"));
        store.clear_timer_start_seen(&"case-1".to_string(), &"tc".to_string());
        let outcome =
            store.take_timer_tokens_with_outcome(&"case-1".to_string(), &"tc".to_string());
        assert_eq!(outcome, TimerStopOutcome::NeverStarted);
    }

    #[test]
    fn insert_and_count_timer_tokens() {
        let mut store = TokenStore::new();
        store.insert_timer_token(make_timer_token("case-1", "tc"));
        store.insert_timer_token(make_timer_token("case-1", "tc"));
        assert_eq!(
            store.timer_token_count(&"case-1".to_string(), &"tc".to_string()),
            2
        );
    }

    #[test]
    fn remove_timer_token_by_id_cleans_index() {
        let mut store = TokenStore::new();
        let t = make_timer_token("case-1", "tc");
        let id = t.id;
        store.insert_timer_token(t);
        assert!(store.remove_timer_token(&id).is_some());
        assert_eq!(
            store.timer_token_count(&"case-1".to_string(), &"tc".to_string()),
            0
        );
    }

    #[test]
    fn remove_all_via_outcome_clears_index() {
        let mut store = TokenStore::new();
        store.insert_timer_token(make_timer_token("case-1", "tc"));
        store.insert_timer_token(make_timer_token("case-1", "tc"));
        store.insert_timer_token(make_timer_token("case-1", "tc"));
        store.take_timer_tokens_with_outcome(&"case-1".to_string(), &"tc".to_string());
        assert_eq!(
            store.timer_token_count(&"case-1".to_string(), &"tc".to_string()),
            0
        );
    }

    #[test]
    fn timer_tokens_independent_of_token_out() {
        let mut store = TokenStore::new();
        store.produce_with_overwrite(make_token("case-1", "c"));
        store.insert_timer_token(make_timer_token("case-1", "c"));
        store.remove_out(&"case-1".to_string(), &"c".to_string());
        assert_eq!(
            store.timer_token_count(&"case-1".to_string(), &"c".to_string()),
            1
        );
    }
}
