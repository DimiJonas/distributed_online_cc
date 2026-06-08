/// this module buffers constraint check results before sending them
/// this helps realize the result emitter that in turn realizes a grace period or revocable results before a result is finalized
///
///  - GracePeriod means the result is buffered for a duration. If an update
///   arrives during that window the buffered result is overwritten. Once the
///   timer expires the final result is sent.
/// - Revocable means the result is sent immediately but can be overwritten by
///   an update within a duration. After the window the result is considered
///   final and can no longer be changed.
use std::{collections::HashMap, time::Duration};

use tokio_util::sync::CancellationToken;

use crate::types::{CaseId, ConstraintId, ConstraintResultType};

#[derive(Debug, Clone)]
pub enum ResultBufferPolicy {
    /// Hold the result for a duration; allow overwrites; send once timer fires
    GracePeriod { duration: Duration },
    /// Send immediately; allow overwrites for a duration; then finalise
    Revocable { duration: Duration },
}

#[derive(Debug)]
pub struct PendingResult {
    pub result: ConstraintResultType,
    pub info: String,
    pub cancel_token: CancellationToken,
}

#[derive(Default)]
pub struct ConstraintResultSendBuffer {
    pending: HashMap<(CaseId, ConstraintId), PendingResult>,
}

impl ConstraintResultSendBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or update a pending result
    ///
    /// If an entry already exists its timer task is cancelled before the new entry replaces it. The
    /// caller is responsible for spawning a new timer task using the returned
    /// cancel token
    ///
    /// Returns the cancel token for the new entry so the caller can
    /// pass it to the timer task it spawns
    pub fn insert_or_update(
        &mut self,
        case_id: CaseId,
        constraint_id: ConstraintId,
        result: ConstraintResultType,
        info: String,
    ) -> CancellationToken {
        let key = (case_id, constraint_id);

        // Cancel any existing timer task for this entry.
        if let Some(old) = self.pending.get(&key) {
            old.cancel_token.cancel();
        }

        let cancel_token = CancellationToken::new();
        self.pending.insert(
            key,
            PendingResult {
                result,
                info,
                cancel_token: cancel_token.clone(),
            },
        );
        cancel_token
    }

    /// Remove and return the pending result, e.g. when the timer fires and
    /// the result is ready to be sent
    pub fn take(
        &mut self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
    ) -> Option<PendingResult> {
        self.pending
            .remove(&(case_id.clone(), constraint_id.clone()))
    }

    /// Peek at the current buffered result without removing it
    pub fn get(&self, case_id: &CaseId, constraint_id: &ConstraintId) -> Option<&PendingResult> {
        self.pending.get(&(case_id.clone(), constraint_id.clone()))
    }

    pub fn has_pending(&self, case_id: &CaseId, constraint_id: &ConstraintId) -> bool {
        self.pending
            .contains_key(&(case_id.clone(), constraint_id.clone()))
    }
}
