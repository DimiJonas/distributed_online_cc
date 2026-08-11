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

/// monotonic revision numbers per (case, constraint) to determine the order in which results were created
#[derive(Debug, Default)]
pub struct ResultRevisionStore {
    revisions: HashMap<(CaseId, ConstraintId), RevisionNumber>,
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
        let counter = self
            .revisions
            .entry((case_id.clone(), constraint_id.clone()))
            .or_insert(0);
        *counter += 1;
        *counter
    }

    /// the highest revision used so far (0 when unused)
    pub fn current_revision(
        &self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
    ) -> RevisionNumber {
        self.revisions
            .get(&(case_id.clone(), constraint_id.clone()))
            .copied()
            .unwrap_or(0)
    }

    pub fn stored_revision_count(&self) -> usize {
        self.revisions.len()
    }

    /// forget every counter of a case
    pub fn remove_case(&mut self, case_id: &CaseId) {
        self.revisions.retain(|(case, _), _| case != case_id);
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
