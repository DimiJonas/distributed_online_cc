use crate::types::{CaseId, HandlerIndex, Token};
use std::collections::{HashMap, hash_map::Entry};

#[derive(Debug, Default)]
pub struct TokenInStore {
    // tokens pushed to or pulled by this node from another node
    token_in: HashMap<CaseId, HashMap<HandlerIndex, Token>>,
}

impl TokenInStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// puts the token in token_in overwriting existing tokens
    pub fn receive_with_overwrite(&mut self, token: Token, index: HandlerIndex) {
        self.token_in
            .entry(token.case_id.clone())
            .or_default()
            .insert(index, token);
    }

    /// puts the token in token_in NOT overwriting existing tokens
    pub fn receive_without_overwrite(&mut self, token: Token, index: HandlerIndex) {
        let inner_map = self.token_in.entry(token.case_id.clone()).or_default();

        if let Entry::Vacant(e) = inner_map.entry(index) {
            e.insert(token);
        }
    }

    pub fn remove_in(&mut self, case_id: &CaseId, index: HandlerIndex) -> Option<Token> {
        let inner_map = self.token_in.get_mut(case_id)?;
        let token = inner_map.remove(&index);
        if inner_map.is_empty() {
            self.token_in.remove(case_id);
        }
        token
    }

    pub fn get_in_mut(&mut self, case_id: &CaseId, index: HandlerIndex) -> Option<&mut Token> {
        self.token_in.get_mut(case_id)?.get_mut(&index)
    }

    pub fn get_in_cloned(&self, case_id: &CaseId, index: HandlerIndex) -> Option<Token> {
        self.token_in.get(case_id)?.get(&index).cloned()
    }

    pub fn case_count(&self) -> usize {
        self.token_in.len()
    }

    pub fn contains_in_token(&self, case_id: &CaseId, index: HandlerIndex) -> bool {
        self.token_in
            .get(case_id)
            .is_some_and(|inner| inner.contains_key(&index))
    }
}

#[cfg(test)]
mod tests {}
