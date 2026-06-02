use crate::types::{CaseId, ConstraintId, EventTime, Token};
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct TokenStore {
    pub token_out: HashMap<(CaseId, ConstraintId), Token>,
    pub token_in: HashMap<(CaseId, ConstraintId), Token>,
}

impl TokenStore {
    pub fn new() -> Self {
        Self {
            token_in: HashMap::new(),
            token_out: HashMap::new(),
        }
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
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::types::{ConstraintType, Token};

    fn make_token(case_id: &str, constraint_id: &str) -> Token {
        Token::new(
            case_id.to_string(),
            constraint_id.to_string(),
            ConstraintType::Response,
            Some(Utc::now()),
            1,
            None,
        )
    }

    #[test]
    fn produce_and_has_open_token() {
        let mut store = TokenStore::new();
        let token = make_token("case-1", "constraint-a");
        store.produce_with_overwrite(token);
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
        let token = make_token("case-1", "constraint-a");
        store.produce_with_overwrite(token);

        let removed = store.remove_out(&"case-1".to_string(), &"constraint-a".to_string());
        assert!(removed.is_some());
        // second removal should be None
        assert!(
            store
                .remove_out(&"case-1".to_string(), &"constraint-a".to_string())
                .is_none()
        );
        // has_open_token should now be false
        assert!(!store.has_open_token(&"case-1".to_string(), &"constraint-a".to_string()));
    }

    #[test]
    fn remove_out_unknown_key_returns_none() {
        let mut store = TokenStore::new();
        assert!(
            store
                .remove_out(&"nope".to_string(), &"nope".to_string())
                .is_none()
        );
    }

    #[test]
    fn receive_and_remove_in() {
        let mut store = TokenStore::new();
        let token = make_token("case-2", "constraint-b");
        store.receive_with_overwrite(token);

        let removed = store.remove_in(&"case-2".to_string(), &"constraint-b".to_string());
        assert!(removed.is_some());
        assert!(
            store
                .remove_in(&"case-2".to_string(), &"constraint-b".to_string())
                .is_none()
        );
    }

    #[test]
    fn produce_overwrites_previous_token_for_same_key() {
        let mut store = TokenStore::new();
        let t1 = make_token("case-1", "c");
        let id1 = t1.id;
        store.produce_with_overwrite(t1);

        let t2 = make_token("case-1", "c");
        let id2 = t2.id;
        store.produce_with_overwrite(t2);

        // Should hold the newest token
        let removed = store
            .remove_out(&"case-1".to_string(), &"c".to_string())
            .unwrap();
        assert_eq!(removed.id, id2);
        assert_ne!(removed.id, id1);
    }

    #[test]
    fn token_out_and_token_in_are_independent() {
        let mut store = TokenStore::new();
        store.produce_with_overwrite(make_token("case-x", "c"));
        store.receive_with_overwrite(make_token("case-x", "c"));

        assert!(
            store
                .remove_out(&"case-x".to_string(), &"c".to_string())
                .is_some()
        );
        assert!(
            store
                .remove_in(&"case-x".to_string(), &"c".to_string())
                .is_some()
        );
    }
}
