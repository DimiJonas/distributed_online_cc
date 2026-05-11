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
        self.token_out
            .insert((token.case_id.clone(), token.constraint_id.clone()), token);
    }

    pub fn receive(&mut self, token: Token) {
        self.token_in
            .insert((token.case_id.clone(), token.constraint_id.clone()), token);
    }

    pub fn remove_out(&mut self, case_id: &CaseId, constraint_id: &ConstraintId) -> Option<Token> {
        self.token_out
            .remove(&(case_id.clone(), constraint_id.clone()))
    }

    pub fn remove_in(&mut self, case_id: &CaseId, constraint_id: &ConstraintId) -> Option<Token> {
        self.token_in
            .remove(&(case_id.clone(), constraint_id.clone()))
    }

    // are there still open obligations e.g. are there tokens in token_out
    pub fn has_open_token(&self, case_id: &CaseId, constraint_id: &ConstraintId) -> bool {
        self.token_out
            .contains_key(&(case_id.clone(), constraint_id.clone()))
    }
}
