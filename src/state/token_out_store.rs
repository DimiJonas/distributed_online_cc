use crate::types::{CaseId, EventTime, HandlerIndex, Token};
use std::collections::{HashMap, hash_map::Entry};

#[derive(Debug, Default)]
pub struct TokenOutStore {
    // tokens produced by the node destined to be pushed to or pulled by other nodes
    token_out: HashMap<CaseId, HashMap<HandlerIndex, Token>>,
}

impl TokenOutStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// puts the token into token_out overwriting existing tokens
    pub fn produce_with_overwrite(&mut self, token: Token, index: HandlerIndex) {
        self.token_out
            .entry(token.case_id.clone())
            .or_default()
            .insert(index, token);
    }

    pub fn produce_without_overwrite(&mut self, token: Token, index: HandlerIndex) {
        let inner_map = self.token_out.entry(token.case_id.clone()).or_default();
        if let Entry::Vacant(e) = inner_map.entry(index) {
            e.insert(token);
        }
    }

    pub fn remove_out(&mut self, case_id: &CaseId, index: HandlerIndex) -> Option<Token> {
        let inner_map = self.token_out.get_mut(case_id)?;
        let token = inner_map.remove(&index);
        if inner_map.is_empty() {
            self.token_out.remove(case_id);
        }
        token
    }

    pub fn remove_out_before(
        &mut self,
        case_id: &CaseId,
        index: HandlerIndex,
        timestamp: EventTime,
    ) -> Option<Token> {
        let inner_map = self.token_out.get_mut(case_id)?;
        let token_time = {
            let token = inner_map.get(&index)?;
            token.event_time.unwrap_or(token.created_at)
        };
        if token_time >= timestamp {
            return None;
        }

        let token = inner_map.remove(&index);
        if inner_map.is_empty() {
            self.token_out.remove(case_id);
        }
        token
    }

    pub fn case_count(&self) -> usize {
        self.token_out.len()
    }

    pub fn contains_out_token(&self, case_id: &CaseId, index: HandlerIndex) -> bool {
        self.token_out
            .get(case_id)
            .is_some_and(|inner| inner.contains_key(&index))
    }

    pub fn contains_out_token_before(
        &self,
        case_id: &CaseId,
        index: HandlerIndex,
        timestamp: EventTime,
    ) -> bool {
        self.token_out
            .get(case_id)
            .and_then(|inner| inner.get(&index))
            .is_some_and(|token| token.event_time.unwrap_or(token.created_at) < timestamp)
    }

    pub fn get_out_mut(&mut self, case_id: &CaseId, index: HandlerIndex) -> Option<&mut Token> {
        self.token_out.get_mut(case_id)?.get_mut(&index)
    }

    pub fn get_out_cloned(&self, case_id: &CaseId, index: HandlerIndex) -> Option<Token> {
        self.token_out.get(case_id)?.get(&index).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::test_tools::make_token;

    #[test]
    fn produce_and_has_open_token() {
        let mut store = TokenOutStore::new();
        store.produce_with_overwrite(make_token("case-1", "constraint-a"), 0);
        assert!(store.contains_out_token(&"case-1".to_string(), 0));
    }

    #[test]
    fn remove_out_returns_token_and_clears_it() {
        let mut store = TokenOutStore::new();
        store.produce_with_overwrite(make_token("case-1", "constraint-a"), 0);
        assert!(store.remove_out(&"case-1".to_string(), 0).is_some());
        assert!(store.remove_out(&"case-1".to_string(), 0).is_none());
    }
}
