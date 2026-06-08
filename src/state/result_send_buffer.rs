/// this module buffers constraint check results before sending them
/// this helps realize the result emitter that in turn realizes a grace period or revocable results before a result is finalized
///
/// - GracePeriod means the result is buffered for a duration. If an update
///   arrives during that window the buffered result is overwritten. Once the
///   timer expires the final result is sent.
/// - Revocable means the result is sent immediately but can be overwritten by
///   an update within a duration. After the window the result is considered
///   final and can no longer be changed.
use std::{collections::HashMap, fmt::Display, time::Duration};

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

impl Display for PendingResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PendingResult {{ result: {:?}, info: {} }}",
            self.result, self.info
        )
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn k(case: &str, constraint: &str) -> (CaseId, ConstraintId) {
        (case.to_string(), constraint.to_string())
    }

    #[test]
    fn empty_buffer_has_no_pending() {
        let buf = ConstraintResultSendBuffer::new();
        assert!(!buf.has_pending(&"c".to_string(), &"x".to_string()));
        assert!(buf.get(&"c".to_string(), &"x".to_string()).is_none());
        assert!(buf.pending.is_empty());
    }

    #[test]
    fn insert_or_update_stores_entry() {
        let mut buf = ConstraintResultSendBuffer::new();
        let token = buf.insert_or_update(
            "c".into(),
            "x".into(),
            ConstraintResultType::Satisfied,
            "ok".into(),
        );
        assert!(!token.is_cancelled());
        assert!(buf.has_pending(&"c".to_string(), &"x".to_string()));
        let pending = buf.get(&"c".to_string(), &"x".to_string()).unwrap();
        assert!(matches!(pending.result, ConstraintResultType::Satisfied));
        assert_eq!(pending.info, "ok");
    }

    #[test]
    fn insert_or_update_cancels_previous_token() {
        let mut buf = ConstraintResultSendBuffer::new();
        let first = buf.insert_or_update(
            "c".into(),
            "x".into(),
            ConstraintResultType::Satisfied,
            "first".into(),
        );
        assert!(!first.is_cancelled());

        let second = buf.insert_or_update(
            "c".into(),
            "x".into(),
            ConstraintResultType::Violated,
            "second".into(),
        );
        // The first cancel token was cancelled by the supersede; the second is fresh.
        assert!(first.is_cancelled());
        assert!(!second.is_cancelled());

        // And the stored entry reflects the second value.
        let pending = buf.get(&"c".to_string(), &"x".to_string()).unwrap();
        assert!(matches!(pending.result, ConstraintResultType::Violated));
        assert_eq!(pending.info, "second");
    }

    #[test]
    fn supersede_only_affects_matching_key() {
        let mut buf = ConstraintResultSendBuffer::new();
        let t_a = buf.insert_or_update(
            "c".into(),
            "a".into(),
            ConstraintResultType::Satisfied,
            "".into(),
        );
        let _t_b = buf.insert_or_update(
            "c".into(),
            "b".into(),
            ConstraintResultType::Satisfied,
            "".into(),
        );
        // Updating "a" must NOT cancel "b"'s token.
        let _t_a2 = buf.insert_or_update(
            "c".into(),
            "a".into(),
            ConstraintResultType::Violated,
            "".into(),
        );
        assert!(t_a.is_cancelled());
        // and "b" is still there + not cancelled
        assert!(buf.has_pending(&"c".to_string(), &"b".to_string()));
    }

    #[test]
    fn take_removes_and_returns_entry() {
        let mut buf = ConstraintResultSendBuffer::new();
        buf.insert_or_update(
            "c".into(),
            "x".into(),
            ConstraintResultType::Satisfied,
            "ok".into(),
        );
        let pending = buf.take(&"c".to_string(), &"x".to_string());
        assert!(pending.is_some());
        let pending = pending.unwrap();
        assert!(matches!(pending.result, ConstraintResultType::Satisfied));
        assert!(!buf.has_pending(&"c".to_string(), &"x".to_string()));
    }

    #[test]
    fn take_on_empty_returns_none() {
        let mut buf = ConstraintResultSendBuffer::new();
        assert!(buf.take(&"c".to_string(), &"x".to_string()).is_none());
    }

    #[test]
    fn keys_are_independent_by_pair() {
        let mut buf = ConstraintResultSendBuffer::new();
        buf.insert_or_update(
            "case-1".into(),
            "x".into(),
            ConstraintResultType::Satisfied,
            "1".into(),
        );
        buf.insert_or_update(
            "case-2".into(),
            "x".into(),
            ConstraintResultType::Violated,
            "2".into(),
        );
        // Same constraint, different cases — both entries exist.
        let p1 = buf.get(&"case-1".to_string(), &"x".to_string()).unwrap();
        let p2 = buf.get(&"case-2".to_string(), &"x".to_string()).unwrap();
        assert!(matches!(p1.result, ConstraintResultType::Satisfied));
        assert!(matches!(p2.result, ConstraintResultType::Violated));
        let _ = k("a", "b"); // suppress unused
    }
}
