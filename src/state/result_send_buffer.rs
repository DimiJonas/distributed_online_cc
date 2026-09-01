/// this module buffers constraint check results before sending them
/// this helps realize the result emitter that in turn realizes a grace period or revocable results before a result is finalized
///
/// - GracePeriod means the result is buffered for a duration and not sent. if an update arrives during that window the buffered result is overwritten in place.
///   once the timer expires the last buffered result is sent and the pair is final.
///   the sink sees exactly one message per pair
/// - Revocable means the result is sent immediately and every update inside the window is sent as well.
///   no value is buffered here, only the fact that the window is open.
///   after the window the result is final and can no longer be changed
use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    time::Duration,
};

use chrono::{DateTime, Utc};

use crate::{
    state::result_revision_store::RevisionNumber,
    types::{CaseId, ConstraintId, ConstraintResultType},
};

#[derive(Debug, Clone)]
pub enum ResultBufferPolicy {
    /// Hold the result for a duration; allow overwrites; send once timer fires
    GracePeriod { duration: Duration },
    /// Send immediately; allow overwrites for a duration; then finalise
    Revocable { duration: Duration },
}

impl Display for ResultBufferPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResultBufferPolicy::GracePeriod { duration } if duration.is_zero() => {
                write!(f, "immediate")
            }
            ResultBufferPolicy::GracePeriod { .. } => write!(f, "grace_period"),
            ResultBufferPolicy::Revocable { .. } => write!(f, "revocable"),
        }
    }
}

/// the value a GracePeriod pair will send when its window expires (Revocable results are never buffered)
#[derive(Debug)]
pub struct PendingResult {
    pub result: ConstraintResultType,
    pub info: String,
    pub revision: RevisionNumber,
    pub decided_at: DateTime<Utc>,
}

impl Display for PendingResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PendingResult {{ result: {:?}, revision: {}, info: {} }}",
            self.result, self.revision, self.info
        )
    }
}

#[derive(Default)]
pub struct ConstraintResultSendBuffer {
    /// GracePeriod: the value waiting to be sent
    pending: HashMap<(CaseId, ConstraintId), PendingResult>,
    /// Revocable: the pairs whose window is still open (no value since the verdicts are sent immediately)
    open: HashSet<(CaseId, ConstraintId)>,
}

impl ConstraintResultSendBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    /// buffer a result or overwrite the buffered one
    /// returns true if the call opened the window
    pub fn insert_or_update(
        &mut self,
        case_id: CaseId,
        constraint_id: ConstraintId,
        result: ConstraintResultType,
        info: String,
        revision: RevisionNumber,
        decided_at: DateTime<Utc>,
    ) -> bool {
        let entry = PendingResult {
            result,
            info,
            revision,
            decided_at,
        };
        self.pending
            .insert((case_id, constraint_id), entry)
            .is_none()
    }

    /// remove and return the pending result
    pub fn take(
        &mut self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
    ) -> Option<PendingResult> {
        self.pending
            .remove(&(case_id.clone(), constraint_id.clone()))
    }

    /// peek at the current buffered result without removing it
    pub fn get(&self, case_id: &CaseId, constraint_id: &ConstraintId) -> Option<&PendingResult> {
        self.pending.get(&(case_id.clone(), constraint_id.clone()))
    }

    pub fn has_pending(&self, case_id: &CaseId, constraint_id: &ConstraintId) -> bool {
        self.pending
            .contains_key(&(case_id.clone(), constraint_id.clone()))
    }

    /// Revocable: mark the window open
    /// returns true if the call that opened it
    pub fn open_window(&mut self, case_id: CaseId, constraint_id: ConstraintId) -> bool {
        self.open.insert((case_id, constraint_id))
    }

    /// Revocable: the window expired
    /// returns true if it was open
    pub fn close_window(&mut self, case_id: &CaseId, constraint_id: &ConstraintId) -> bool {
        self.open.remove(&(case_id.clone(), constraint_id.clone()))
    }

    pub fn is_open(&self, case_id: &CaseId, constraint_id: &ConstraintId) -> bool {
        self.open
            .contains(&(case_id.clone(), constraint_id.clone()))
    }

    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    pub fn open_window_count(&self) -> usize {
        self.open.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// distinct, increasing timestamps without depending on a particular chrono constructor
    fn at(step: i64) -> DateTime<Utc> {
        Utc::now() + chrono::Duration::seconds(step)
    }

    fn insert(
        buf: &mut ConstraintResultSendBuffer,
        case: &str,
        constraint: &str,
        result: ConstraintResultType,
        info: &str,
        revision: RevisionNumber,
    ) -> bool {
        buf.insert_or_update(
            case.to_string(),
            constraint.to_string(),
            result,
            info.to_string(),
            revision,
            at(revision),
        )
    }

    #[test]
    fn empty_buffer_has_no_pending() {
        let buf = ConstraintResultSendBuffer::new();
        assert!(!buf.has_pending(&"c".to_string(), &"x".to_string()));
        assert!(buf.get(&"c".to_string(), &"x".to_string()).is_none());
        assert!(!buf.is_open(&"c".to_string(), &"x".to_string()));
        assert_eq!(buf.pending_count(), 0);
        assert_eq!(buf.open_window_count(), 0);
    }

    #[test]
    fn insert_or_update_stores_entry_and_reports_the_open() {
        let mut buf = ConstraintResultSendBuffer::new();
        let opened = insert(&mut buf, "c", "x", ConstraintResultType::Satisfied, "ok", 1);
        assert!(opened, "the first verdict opens the window");
        assert!(buf.has_pending(&"c".to_string(), &"x".to_string()));
        let pending = buf.get(&"c".to_string(), &"x".to_string()).unwrap();
        assert!(matches!(pending.result, ConstraintResultType::Satisfied));
        assert_eq!(pending.info, "ok");
        assert_eq!(pending.revision, 1);
    }

    /// the key property: an update replaces the value but does NOT reopen the window, so the caller spawns no second
    /// timer and the original window still runs out on schedule
    #[test]
    fn update_replaces_the_value_without_reopening_the_window() {
        let mut buf = ConstraintResultSendBuffer::new();
        assert!(insert(
            &mut buf,
            "c",
            "x",
            ConstraintResultType::Satisfied,
            "first",
            1
        ));
        assert!(!insert(
            &mut buf,
            "c",
            "x",
            ConstraintResultType::Violated,
            "second",
            2
        ));
        let third = at(3);
        assert!(!buf.insert_or_update(
            "c".to_string(),
            "x".to_string(),
            ConstraintResultType::Violated,
            "third".to_string(),
            3,
            third,
        ));

        let pending = buf.get(&"c".to_string(), &"x".to_string()).unwrap();
        assert!(matches!(pending.result, ConstraintResultType::Violated));
        assert_eq!(pending.info, "third");
        // the revision and the timestamp travel with the value, not with the verdict that opened the window
        assert_eq!(pending.revision, 3);
        assert_eq!(pending.decided_at, third);
        assert_eq!(buf.pending_count(), 1);
    }

    #[test]
    fn updates_only_affect_the_matching_key() {
        let mut buf = ConstraintResultSendBuffer::new();
        insert(&mut buf, "c", "a", ConstraintResultType::Satisfied, "", 1);
        insert(&mut buf, "c", "b", ConstraintResultType::Satisfied, "", 1);
        insert(&mut buf, "c", "a", ConstraintResultType::Violated, "", 2);

        let a = buf.get(&"c".to_string(), &"a".to_string()).unwrap();
        let b = buf.get(&"c".to_string(), &"b".to_string()).unwrap();
        assert!(matches!(a.result, ConstraintResultType::Violated));
        assert!(matches!(b.result, ConstraintResultType::Satisfied));
        assert_eq!(buf.pending_count(), 2);
    }

    #[test]
    fn take_removes_and_returns_entry() {
        let mut buf = ConstraintResultSendBuffer::new();
        insert(&mut buf, "c", "x", ConstraintResultType::Satisfied, "ok", 1);
        let pending = buf.take(&"c".to_string(), &"x".to_string()).unwrap();
        assert!(matches!(pending.result, ConstraintResultType::Satisfied));
        assert!(!buf.has_pending(&"c".to_string(), &"x".to_string()));
        assert_eq!(buf.pending_count(), 0);
    }

    #[test]
    fn take_on_empty_returns_none() {
        let mut buf = ConstraintResultSendBuffer::new();
        assert!(buf.take(&"c".to_string(), &"x".to_string()).is_none());
    }

    /// after a take the pair may be inserted again and that counts as opening a new window. the emitter never does this
    /// because it marks the pair final first, but the buffer must not pretend otherwise
    #[test]
    fn insert_after_take_opens_a_new_window() {
        let mut buf = ConstraintResultSendBuffer::new();
        assert!(insert(
            &mut buf,
            "c",
            "x",
            ConstraintResultType::Satisfied,
            "1",
            1
        ));
        buf.take(&"c".to_string(), &"x".to_string());
        assert!(insert(
            &mut buf,
            "c",
            "x",
            ConstraintResultType::Satisfied,
            "2",
            2
        ));
    }

    #[test]
    fn keys_are_independent_by_pair() {
        let mut buf = ConstraintResultSendBuffer::new();
        insert(
            &mut buf,
            "case-1",
            "x",
            ConstraintResultType::Satisfied,
            "1",
            1,
        );
        insert(
            &mut buf,
            "case-2",
            "x",
            ConstraintResultType::Violated,
            "2",
            1,
        );
        let p1 = buf.get(&"case-1".to_string(), &"x".to_string()).unwrap();
        let p2 = buf.get(&"case-2".to_string(), &"x".to_string()).unwrap();
        assert!(matches!(p1.result, ConstraintResultType::Satisfied));
        assert!(matches!(p2.result, ConstraintResultType::Violated));
    }

    // ── revocable windows ────────────────────────────────────────────────

    #[test]
    fn open_window_reports_only_the_first_open() {
        let mut buf = ConstraintResultSendBuffer::new();
        assert!(buf.open_window("c".to_string(), "x".to_string()));
        assert!(!buf.open_window("c".to_string(), "x".to_string()));
        assert!(!buf.open_window("c".to_string(), "x".to_string()));
        assert!(buf.is_open(&"c".to_string(), &"x".to_string()));
        assert_eq!(buf.open_window_count(), 1);
    }

    #[test]
    fn close_window_reports_whether_it_was_open() {
        let mut buf = ConstraintResultSendBuffer::new();
        buf.open_window("c".to_string(), "x".to_string());
        assert!(buf.close_window(&"c".to_string(), &"x".to_string()));
        assert!(!buf.close_window(&"c".to_string(), &"x".to_string()));
        assert!(!buf.is_open(&"c".to_string(), &"x".to_string()));
    }

    /// revocable stores no value: opening a window must not create a pending entry for the grace period path
    #[test]
    fn open_window_does_not_create_a_pending_value() {
        let mut buf = ConstraintResultSendBuffer::new();
        buf.open_window("c".to_string(), "x".to_string());
        assert!(!buf.has_pending(&"c".to_string(), &"x".to_string()));
        assert_eq!(buf.pending_count(), 0);
    }

    #[test]
    fn windows_are_independent_by_pair() {
        let mut buf = ConstraintResultSendBuffer::new();
        buf.open_window("c".to_string(), "a".to_string());
        buf.open_window("c".to_string(), "b".to_string());
        buf.close_window(&"c".to_string(), &"a".to_string());
        assert!(!buf.is_open(&"c".to_string(), &"a".to_string()));
        assert!(buf.is_open(&"c".to_string(), &"b".to_string()));
    }
}
