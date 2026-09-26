use std::collections::HashMap;

use chrono::{DateTime, Utc};

use crate::types::{CaseId, ConstraintId};

pub type RevisionNumber = i64;

/// a revision number together with a timestamp when it was created
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecisionStamp {
    pub decided_at: DateTime<Utc>,
    pub revision: RevisionNumber,
}

/// the per (case, constraint) revision state
#[derive(Debug, Clone, Copy, Default)]
pub struct RevisionEntry {
    // last revision number that was handed out by next_revision()
    last_issued: RevisionNumber,
    // if correction window has closed
    finalized: bool,
    // highest revision that was published
    highest_published: Option<RevisionNumber>,
}

/// track revisions of (case, constraint) pair verdicts
#[derive(Debug, Default)]
pub struct ResultRevisionStore {
    revisions: HashMap<(CaseId, ConstraintId), RevisionEntry>,
}

impl ResultRevisionStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// draw the stamp for a result that is about to be emitted
    pub fn stamp(&mut self, case_id: &CaseId, constraint_id: &ConstraintId) -> DecisionStamp {
        let revision = self.next_revision(case_id, constraint_id);
        DecisionStamp {
            decided_at: Utc::now(),
            revision,
        }
    }

    pub fn next_revision(
        &mut self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
    ) -> RevisionNumber {
        let counter = &mut self
            .revisions
            .entry((case_id.clone(), constraint_id.clone()))
            .or_default()
            .last_issued;
        *counter += 1;
        *counter
    }

    pub fn current_revision(
        &self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
    ) -> RevisionNumber {
        self.revisions
            .get(&(case_id.clone(), constraint_id.clone()))
            .copied()
            .unwrap_or_default()
            .last_issued
    }

    pub fn stored_revision_count(&self) -> usize {
        self.revisions.len()
    }

    /// forget every counter of a case
    pub fn remove_case(&mut self, case_id: &CaseId) {
        self.revisions.retain(|(case, _), _| case != case_id);
    }

    /// if the correction window for this pair has closed
    pub fn is_final(&self, case_id: &CaseId, constraint_id: &ConstraintId) -> bool {
        self.revisions
            .get(&(case_id.clone(), constraint_id.clone()))
            .map(|entry| entry.finalized)
            .unwrap_or(false)
    }

    /// close the correction window
    pub fn mark_final(&mut self, case_id: &CaseId, constraint_id: &ConstraintId) {
        if let Some(entry) = self
            .revisions
            .get_mut(&(case_id.clone(), constraint_id.clone()))
        {
            entry.finalized = true;
        }
    }

    /// a newer revision for this pair has already been published, so this one is obsolete
    pub fn is_superseded(
        &self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
        revision: RevisionNumber,
    ) -> bool {
        self.revisions
            .get(&(case_id.clone(), constraint_id.clone()))
            .and_then(|entry| entry.highest_published)
            .map(|published| revision < published)
            .unwrap_or(false)
    }

    /// remember that this revision reached the sink
    pub fn note_published(
        &mut self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
        revision: RevisionNumber,
    ) {
        let Some(entry) = self
            .revisions
            .get_mut(&(case_id.clone(), constraint_id.clone()))
        else {
            return;
        };
        entry.highest_published = Some(match entry.highest_published {
            Some(previous) if previous > revision => previous,
            _ => revision,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn revisions_start_at_one_and_increase() {
        let mut store = ResultRevisionStore::new();
        let case = "case-1".to_string();
        let constraint = "c".to_string();

        assert_eq!(store.current_revision(&case, &constraint), 0);
        assert_eq!(store.next_revision(&case, &constraint), 1);
        assert_eq!(store.next_revision(&case, &constraint), 2);
        assert_eq!(store.current_revision(&case, &constraint), 2);
    }

    #[test]
    fn counters_are_independent_per_pair() {
        let mut store = ResultRevisionStore::new();
        let case_a = "a".to_string();
        let case_b = "b".to_string();
        let constraint_1 = "c1".to_string();
        let constraint_2 = "c2".to_string();

        assert_eq!(store.next_revision(&case_a, &constraint_1), 1);
        assert_eq!(store.next_revision(&case_a, &constraint_2), 1);
        assert_eq!(store.next_revision(&case_b, &constraint_1), 1);
        assert_eq!(store.next_revision(&case_a, &constraint_1), 2);
        assert_eq!(store.stored_revision_count(), 3);
    }

    #[test]
    fn remove_case_only_drops_that_case() {
        let mut store = ResultRevisionStore::new();
        let case_a = "a".to_string();
        let case_b = "b".to_string();
        let constraint = "c".to_string();

        store.next_revision(&case_a, &constraint);
        store.next_revision(&case_b, &constraint);
        store.remove_case(&case_a);

        assert_eq!(store.current_revision(&case_a, &constraint), 0);
        assert_eq!(store.current_revision(&case_b, &constraint), 1);
    }

    // ── finality ─────────────────────────────────────────────────────────

    #[test]
    fn pairs_are_not_final_until_marked() {
        let mut store = ResultRevisionStore::new();
        let case = "a".to_string();
        let constraint = "c".to_string();

        assert!(!store.is_final(&case, &constraint));
        store.next_revision(&case, &constraint);
        assert!(
            !store.is_final(&case, &constraint),
            "issuing a revision must not close the pair"
        );

        store.mark_final(&case, &constraint);
        assert!(store.is_final(&case, &constraint));
        // idempotent, and it does not disturb the counter
        store.mark_final(&case, &constraint);
        assert!(store.is_final(&case, &constraint));
        assert_eq!(store.current_revision(&case, &constraint), 1);
    }

    #[test]
    fn finality_is_per_pair() {
        let mut store = ResultRevisionStore::new();
        let case = "a".to_string();
        store.next_revision(&case, &"c1".to_string());
        store.next_revision(&case, &"c2".to_string());
        store.mark_final(&case, &"c1".to_string());
        assert!(store.is_final(&case, &"c1".to_string()));
        assert!(!store.is_final(&case, &"c2".to_string()));
        assert!(!store.is_final(&"b".to_string(), &"c1".to_string()));
    }

    /// remove_case forgets finality as well, so a late verdict would look like a fresh pair. the case janitor has to
    /// reject late work for reaped cases; this test pins the behaviour so the assumption is visible
    #[test]
    fn remove_case_also_forgets_finality() {
        let mut store = ResultRevisionStore::new();
        let case = "a".to_string();
        let constraint = "c".to_string();

        store.next_revision(&case, &constraint);
        store.mark_final(&case, &constraint);
        store.remove_case(&case);
        assert!(!store.is_final(&case, &constraint));
    }

    /// the window timers fire after the case was closed and its entries removed. closing or publishing a pair the
    /// store no longer knows must not put it back: every such entry would live until the node exits (one per
    /// (case, constraint) pair whose last verdict fell inside the window - which is nearly all of them)
    #[test]
    fn closing_or_publishing_an_unknown_pair_stores_nothing() {
        let mut store = ResultRevisionStore::new();
        let case = "a".to_string();
        let constraint = "c".to_string();

        store.next_revision(&case, &constraint);
        store.remove_case(&case);

        store.mark_final(&case, &constraint);
        store.note_published(&case, &constraint, 1);
        assert_eq!(store.stored_revision_count(), 0);
        assert!(!store.is_final(&case, &constraint));
        assert!(!store.is_superseded(&case, &constraint, 0));

        // a pair that was never stamped is not created by these calls either
        store.mark_final(&"b".to_string(), &constraint);
        store.note_published(&"b".to_string(), &constraint, 1);
        assert_eq!(store.stored_revision_count(), 0);
    }

    // ── superseded revisions ─────────────────────────────────────────────

    #[test]
    fn nothing_is_superseded_before_anything_is_published() {
        let store = ResultRevisionStore::new();
        assert!(!store.is_superseded(&"a".to_string(), &"c".to_string(), 1));
        assert!(!store.is_superseded(&"a".to_string(), &"c".to_string(), 99));
    }

    #[test]
    fn only_strictly_older_revisions_are_superseded() {
        let mut store = ResultRevisionStore::new();
        let case = "a".to_string();
        let constraint = "c".to_string();

        store.next_revision(&case, &constraint);
        store.note_published(&case, &constraint, 5);
        assert!(store.is_superseded(&case, &constraint, 4));
        // a retry of the same revision is not superseded
        assert!(!store.is_superseded(&case, &constraint, 5));
        assert!(!store.is_superseded(&case, &constraint, 6));
    }

    /// publishing out of order must not lower the watermark, otherwise a verdict that already lost the race would be
    /// accepted on a second attempt
    #[test]
    fn note_published_keeps_the_highest() {
        let mut store = ResultRevisionStore::new();
        let case = "a".to_string();
        let constraint = "c".to_string();

        store.next_revision(&case, &constraint);
        store.note_published(&case, &constraint, 7);
        store.note_published(&case, &constraint, 3);
        assert!(store.is_superseded(&case, &constraint, 6));
    }

    #[test]
    fn publishing_does_not_issue_revisions_or_close_the_pair() {
        let mut store = ResultRevisionStore::new();
        let case = "a".to_string();
        let constraint = "c".to_string();

        store.next_revision(&case, &constraint);
        store.note_published(&case, &constraint, 3);
        assert_eq!(store.current_revision(&case, &constraint), 1);
        assert!(!store.is_final(&case, &constraint));
    }

    /// stamps drawn from one store are ordered the same way by revision and by
    /// decided_at - the property `--rank-by revision` relies on
    #[test]
    fn stamp_orders_revision_and_time_the_same_way() {
        let mut store = ResultRevisionStore::new();
        let case = "a".to_string();
        let constraint = "c".to_string();

        let first = store.stamp(&case, &constraint);
        let second = store.stamp(&case, &constraint);

        assert!(first.revision < second.revision);
        assert!(first.decided_at <= second.decided_at);
    }
}
