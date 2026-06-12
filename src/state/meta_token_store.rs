use crate::types::{CaseId, ConstraintHandlerId, ConstraintId, Token};
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct MetaTokenStore {
    // tokens for remembering some meta information for constraints
    meta_tokens: HashMap<(CaseId, ConstraintId, ConstraintHandlerId), Token>,
}

impl MetaTokenStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert_meta_token(&mut self, token: Token, handler_id: ConstraintHandlerId) {
        self.meta_tokens.insert(
            (
                token.case_id.clone(),
                token.constraint_id.clone(),
                handler_id,
            ),
            token,
        );
    }

    pub fn remove_meta_token(
        &mut self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
        handler_id: ConstraintHandlerId,
    ) -> Option<Token> {
        self.meta_tokens
            .remove(&(case_id.clone(), constraint_id.clone(), handler_id))
    }

    pub fn contains_meta_token(
        &self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
        handler_id: ConstraintHandlerId,
    ) -> bool {
        self.meta_tokens
            .contains_key(&(case_id.clone(), constraint_id.clone(), handler_id))
    }

    pub fn get_token_cloned(
        &self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
        handler_id: ConstraintHandlerId,
    ) -> Option<Token> {
        self.meta_tokens
            .get(&(case_id.clone(), constraint_id.clone(), handler_id))
            .cloned()
    }

    pub fn get_token_mut(
        &mut self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
        handler_id: ConstraintHandlerId,
    ) -> Option<&mut Token> {
        self.meta_tokens
            .get_mut(&(case_id.clone(), constraint_id.clone(), handler_id))
    }
}
