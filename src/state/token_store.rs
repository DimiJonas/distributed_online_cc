use crate::types::{CaseId, ConstraintId, Token};
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

    pub fn produce(&mut self, token: Token) {
        let case_id = token.case_id.clone();
        let constraint_id = token.constraint_id.clone();
        if self.contains_out_token(&case_id, &constraint_id) {
            log::warn!(
                "token for ({}, {}) already exists, overwriting",
                case_id,
                constraint_id
            );
        }
        self.token_out.insert((case_id, constraint_id), token);
    }

    pub fn receive(&mut self, token: Token) {
        let case_id = token.case_id.clone();
        let constraint_id = token.constraint_id.clone();
        if self.contains_in_token(&case_id, &constraint_id) {
            log::warn!(
                "token for ({}, {}) already exists, overwriting",
                case_id,
                constraint_id
            );
        }
        self.token_in.insert((case_id, constraint_id), token);
    }

    pub fn remove_out(&mut self, case_id: &CaseId, constraint_id: &ConstraintId) -> Option<Token> {
        self.token_out
            .remove(&(case_id.clone(), constraint_id.clone()))
    }

    pub fn remove_in(&mut self, case_id: &CaseId, constraint_id: &ConstraintId) -> Option<Token> {
        self.token_in
            .remove(&(case_id.clone(), constraint_id.clone()))
    }

    pub fn contains_out_token(&self, case_id: &CaseId, constraint_id: &ConstraintId) -> bool {
        self.token_out
            .contains_key(&(case_id.clone(), constraint_id.clone()))
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
    use super::*;
    use crate::types::{ConstraintType, Token};

    fn make_token(case_id: &str, constraint_id: &str) -> Token {
        Token::new(
            case_id.to_string(),
            constraint_id.to_string(),
            ConstraintType::Response,
            1,
            None,
        )
    }

    #[test]
    fn produce_and_has_open_token() {
        let mut store = TokenStore::new();
        let token = make_token("case-1", "constraint-a");
        store.produce(token);
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
        store.produce(token);

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
        store.receive(token);

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
        store.produce(t1);

        let t2 = make_token("case-1", "c");
        let id2 = t2.id;
        store.produce(t2);

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
        store.produce(make_token("case-x", "c"));
        store.receive(make_token("case-x", "c"));

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
