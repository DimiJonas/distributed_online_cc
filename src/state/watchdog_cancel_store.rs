use std::collections::HashMap;
use tokio_util::sync::CancellationToken;

use crate::types::{CaseId, ConstraintId, EventInstanceId};

/// Stores watchdog cancel handles keyed by EventInstanceId which is just the TokenId from the token since a token is created for each event instance
///
/// the parent tokens can be used to cancel all tokens for a certain caseid/constraintid combination at once
/// that happens when a time stop event comes in and it eliminates the a race condition
/// where cancelling the tokens one by one can take long enough for some tokens to time out and fire
#[derive(Default)]
pub struct WatchdogCancelStore {
    parent_tokens: HashMap<(CaseId, ConstraintId), CancellationToken>,
    cancel_tokens: HashMap<EventInstanceId, CancellationToken>,
    cancel_token_index: HashMap<(CaseId, ConstraintId), Vec<EventInstanceId>>,
    // used to remove a specific token/id from the index
    cancel_token_reverse_index: HashMap<EventInstanceId, (CaseId, ConstraintId)>,
}

impl WatchdogCancelStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// create a watchdog cancel token for a new watchdog
    /// also creates the parent token for this case_id, constraint_id pair if one does not exist yet.
    ///
    /// returns the cancel token to be passed into the watchdog task
    pub fn get_or_create_cancel_token(
        &mut self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
        event_instance_id: EventInstanceId,
    ) -> CancellationToken {
        let key = (case_id.clone(), constraint_id.clone());
        let parent = self.parent_tokens.entry(key.clone()).or_default();

        let child = parent.child_token();
        // Store the constraint key alongside the child token.
        self.cancel_tokens.insert(event_instance_id, child.clone());
        // update index
        self.cancel_token_index
            .entry(key)
            .or_default()
            .push(event_instance_id);
        // update reverse index
        self.cancel_token_reverse_index
            .insert(event_instance_id, (case_id.clone(), constraint_id.clone()));
        child
    }

    /// cancel all watchdogs for a CaseId, ConstraintId pair at once by cancelling the parent
    ///
    /// returns true if a parent existed that could be cancelled
    pub fn cancel_constraint(&mut self, case_id: &CaseId, constraint_id: &ConstraintId) -> bool {
        if let Some(parent) = self
            .parent_tokens
            .remove(&(case_id.clone(), constraint_id.clone()))
        {
            parent.cancel();
            true
        } else {
            false
        }
    }

    /// cancel a single watchdog
    pub fn cancel_watchdog(&mut self, event_instance_id: EventInstanceId) -> bool {
        if let Some(cancel_token) = self.cancel_tokens.get(&event_instance_id) {
            cancel_token.cancel();
            true
        } else {
            false
        }
    }

    /// remove a single token entry when its watchdog fires by timeout
    ///
    /// does NOT cancel anything - the watchdog timed out naturally and will
    /// handle its own result - only cleans up the store entry
    pub fn remove_token(&mut self, event_instance_id: &EventInstanceId) {
        if self.cancel_tokens.remove(event_instance_id).is_none() {
            log::debug!(
                "watchdog_store: remove_token called for token/event instance {} but no entry found (already cancelled or removed)",
                event_instance_id
            );
        }
        // update index using reverse index
        #[allow(clippy::collapsible_if)]
        if let Some(index_key) = self.cancel_token_reverse_index.remove(event_instance_id) {
            if let Some(ids) = self.cancel_token_index.get_mut(&index_key) {
                ids.retain(|id| id != event_instance_id);
                if ids.is_empty() {
                    self.cancel_token_index.remove(&index_key);
                }
            }
        }
    }

    /// remove the parent and all children for a constraint without cancelling
    /// used during case-end cleanup when watchdogs have already been dealt with.
    pub fn remove_constraint(&mut self, case_id: &CaseId, constraint_id: &ConstraintId) {
        let key = (case_id.clone(), constraint_id.clone());
        // remove parent token
        self.parent_tokens.remove(&key);
        // remove from index
        if let Some(ids) = self.cancel_token_index.remove(&key) {
            for id in ids {
                // remove child cancel token
                self.cancel_tokens.remove(&id);
                // remove from reverse index
                self.cancel_token_reverse_index.remove(&id);
            }
        }
    }

    pub fn has_parent(&self, case_id: &CaseId, constraint_id: &ConstraintId) -> bool {
        self.parent_tokens
            .contains_key(&(case_id.clone(), constraint_id.clone()))
    }
}
