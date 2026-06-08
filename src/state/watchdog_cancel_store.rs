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
        let parent = self
            .parent_tokens
            .entry(key.clone())
            .or_insert_with(CancellationToken::new);

        let child = parent.child_token();
        // Store the constraint key alongside the child token.
        self.cancel_tokens.insert(event_instance_id, child.clone());
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
    }

    /// remove the parent and all children for a constraint without cancelling
    /// used during case-end cleanup when watchdogs have already been dealt with.
    pub fn remove_constraint(
        &mut self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
        event_instance_ids: &[EventInstanceId],
    ) {
        self.parent_tokens
            .remove(&(case_id.clone(), constraint_id.clone()));
        for id in event_instance_ids {
            self.cancel_tokens.remove(id);
        }
    }

    pub fn has_parent(&self, case_id: &CaseId, constraint_id: &ConstraintId) -> bool {
        self.parent_tokens
            .contains_key(&(case_id.clone(), constraint_id.clone()))
    }
}
