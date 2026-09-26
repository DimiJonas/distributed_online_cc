use crate::types::{CaseId, HandlerIndex, Token};
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct MetaTokenStore {
    // tokens for remembering some meta information for constraints
    meta_tokens: HashMap<CaseId, HashMap<HandlerIndex, Token>>,
}

impl MetaTokenStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert_meta_token(&mut self, token: Token, index: HandlerIndex) {
        self.meta_tokens
            .entry(token.case_id.clone())
            .or_default()
            .insert(index, token);
    }

    pub fn remove_meta_token(&mut self, case_id: &CaseId, index: HandlerIndex) -> Option<Token> {
        let inner_map = self.meta_tokens.get_mut(case_id)?;
        let token = inner_map.remove(&index);
        if inner_map.is_empty() {
            self.meta_tokens.remove(case_id);
        }
        token
    }

    pub fn case_count(&self) -> usize {
        self.meta_tokens.len()
    }

    pub fn contains_meta_token(&self, case_id: &CaseId, index: HandlerIndex) -> bool {
        self.meta_tokens
            .get(case_id)
            .is_some_and(|inner| inner.contains_key(&index))
    }

    pub fn get_token_cloned(&self, case_id: &CaseId, index: HandlerIndex) -> Option<Token> {
        self.meta_tokens.get(case_id)?.get(&index).cloned()
    }

    pub fn get_token_mut(&mut self, case_id: &CaseId, index: HandlerIndex) -> Option<&mut Token> {
        self.meta_tokens.get_mut(case_id)?.get_mut(&index)
    }
}
