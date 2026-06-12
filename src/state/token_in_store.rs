use crate::types::{CaseId, ConstraintHandlerId, ConstraintId, Token};
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct TokenInStore {
    // tokens pushed to or pulled by this node from another node
    token_in: HashMap<(CaseId, ConstraintId, ConstraintHandlerId), Token>,
}

impl TokenInStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// puts the token in token_in overwriting existing tokens
    pub fn receive_with_overwrite(&mut self, token: Token, handler_id: ConstraintHandlerId) {
        let case_id = token.case_id.clone();
        let constraint_id = token.constraint_id.clone();
        if self.contains_in_token(&case_id, &constraint_id, handler_id) {
            log::warn!(
                "receive: token for ({}, {}) already exists, overwriting",
                case_id,
                constraint_id
            );
        }
        self.token_in
            .insert((case_id, constraint_id, handler_id), token);
    }

    /// puts the token in token_in NOT overwriting existing tokens
    pub fn receive_without_overwrite(&mut self, token: Token, handler_id: ConstraintHandlerId) {
        let case_id = token.case_id.clone();
        let constraint_id = token.constraint_id.clone();
        if self.contains_in_token(&case_id, &constraint_id, handler_id) {
            log::warn!(
                "receive: token for ({}, {}) already exists, keeping old token",
                case_id,
                constraint_id
            );
        } else {
            self.token_in
                .insert((case_id, constraint_id, handler_id), token);
        }
    }

    pub fn remove_in(
        &mut self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
        handler_id: ConstraintHandlerId,
    ) -> Option<Token> {
        self.token_in
            .remove(&(case_id.clone(), constraint_id.clone(), handler_id))
    }

    pub fn get_in_mut(
        &mut self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
        handler_id: ConstraintHandlerId,
    ) -> Option<&mut Token> {
        self.token_in
            .get_mut(&(case_id.clone(), constraint_id.clone(), handler_id))
    }

    pub fn get_in_cloned(
        &self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
        handler_id: ConstraintHandlerId,
    ) -> Option<Token> {
        self.token_in
            .get(&(case_id.clone(), constraint_id.clone(), handler_id))
            .cloned()
    }

    pub fn contains_in_token(
        &self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
        handler_id: ConstraintHandlerId,
    ) -> bool {
        self.token_in
            .contains_key(&(case_id.clone(), constraint_id.clone(), handler_id))
    }
}

#[cfg(test)]
mod tests {}
