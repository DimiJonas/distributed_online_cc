use crate::types::{CaseId, ConstraintHandlerId, ConstraintId, EventTime, Token};
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct TokenStore {
    // tokens produced by the node destined to be pushed to or pulled by other nodes
    token_out: HashMap<(CaseId, ConstraintId, ConstraintHandlerId), Token>,
    // tokens pushed to or pulled by this node from another node
    token_in: HashMap<(CaseId, ConstraintId, ConstraintHandlerId), Token>,
}

impl TokenStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// puts the token into token_out overwriting existing tokens
    pub fn produce_with_overwrite(&mut self, token: Token, handler_id: ConstraintHandlerId) {
        let case_id = token.case_id.clone();
        let constraint_id = token.constraint_id.clone();
        if self.contains_out_token(&case_id, &constraint_id, handler_id) {
            log::debug!(
                "produce: token for ({}, {}) already exists, overwriting",
                case_id,
                constraint_id
            );
        }
        self.token_out
            .insert((case_id, constraint_id, handler_id), token);
    }

    /// puts the token into token_out NOT overwriting existing tokens
    ///
    /// returns
    ///     - true if token was inserted
    ///     - false if token was not inserted because there already exists a token for the same key
    pub fn produce_without_overwrite(
        &mut self,
        token: Token,
        handler_id: ConstraintHandlerId,
    ) -> bool {
        let case_id = token.case_id.clone();
        let constraint_id = token.constraint_id.clone();
        let mut inserted = false;
        if self.contains_out_token(&case_id, &constraint_id, handler_id) {
            log::warn!(
                "produce: token for ({}, {}) already exists, keeping old token",
                case_id,
                constraint_id
            );
        } else {
            self.token_out
                .insert((case_id, constraint_id, handler_id), token);
            inserted = true;
        }
        inserted
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

    pub fn remove_out(
        &mut self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
        handler_id: ConstraintHandlerId,
    ) -> Option<Token> {
        self.token_out
            .remove(&(case_id.clone(), constraint_id.clone(), handler_id))
    }

    pub fn remove_out_before(
        &mut self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
        handler_id: ConstraintHandlerId,
        timestamp: EventTime,
    ) -> Option<Token> {
        if let Some(token) =
            self.token_out
                .get(&(case_id.clone(), constraint_id.clone(), handler_id))
        {
            let token_time = {
                if let Some(event_time) = token.event_time {
                    event_time
                } else {
                    token.created_at
                }
            };
            if token_time < timestamp {
                return self.token_out.remove(&(
                    case_id.clone(),
                    constraint_id.clone(),
                    handler_id,
                ));
            }
        }
        None
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

    pub fn contains_out_token(
        &self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
        handler_id: ConstraintHandlerId,
    ) -> bool {
        self.token_out
            .contains_key(&(case_id.clone(), constraint_id.clone(), handler_id))
    }

    pub fn contains_out_token_before(
        &self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
        handler_id: ConstraintHandlerId,
        timestamp: EventTime,
    ) -> bool {
        if let Some(token) =
            self.token_out
                .get(&(case_id.clone(), constraint_id.clone(), handler_id))
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
        handler_id: ConstraintHandlerId,
    ) -> Option<&mut Token> {
        self.token_out
            .get_mut(&(case_id.clone(), constraint_id.clone(), handler_id))
    }

    pub fn get_out_cloned(
        &self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
        handler_id: ConstraintHandlerId,
    ) -> Option<Token> {
        self.token_out
            .get(&(case_id.clone(), constraint_id.clone(), handler_id))
            .cloned()
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

    // are there still open obligations e.g. are there tokens in token_out
    pub fn has_open_token(
        &self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
        handler_id: ConstraintHandlerId,
    ) -> bool {
        self.token_out
            .contains_key(&(case_id.clone(), constraint_id.clone(), handler_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::test_tools::make_token;

    #[test]
    fn produce_and_has_open_token() {
        let mut store = TokenStore::new();
        store.produce_with_overwrite(
            make_token("case-1", "constraint-a"),
            ConstraintHandlerId::ResponseProducer,
        );
        assert!(store.has_open_token(
            &"case-1".to_string(),
            &"constraint-a".to_string(),
            ConstraintHandlerId::ResponseProducer
        ));
    }

    #[test]
    fn remove_out_returns_token_and_clears_it() {
        let mut store = TokenStore::new();
        store.produce_with_overwrite(
            make_token("case-1", "constraint-a"),
            ConstraintHandlerId::ResponseProducer,
        );
        assert!(
            store
                .remove_out(
                    &"case-1".to_string(),
                    &"constraint-a".to_string(),
                    ConstraintHandlerId::ResponseProducer
                )
                .is_some()
        );
        assert!(
            store
                .remove_out(
                    &"case-1".to_string(),
                    &"constraint-a".to_string(),
                    ConstraintHandlerId::ResponseProducer
                )
                .is_none()
        );
    }
}
