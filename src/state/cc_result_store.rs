use crate::types::{CaseId, ConstraintId, ConstraintResult, ConstraintResultType, ConstraintType};
use std::collections::HashMap;

pub struct CCResultStore {
    results: HashMap<CaseId, HashMap<ConstraintId, ConstraintResult>>,
}

impl CCResultStore {
    pub fn new() -> Self {
        Self {
            results: HashMap::new(),
        }
    }

    // register a new case by initializing a constraint result map according to the constraint configuration
    pub fn register_case(&mut self, case_id: CaseId) {
        self.results.entry(case_id).or_default();
    }

    pub fn upsert(
        &mut self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
        constraint_type: ConstraintType,
        constraint_result: ConstraintResultType,
        info: String,
    ) -> Option<ConstraintResult> {
        self.results.entry(case_id.clone()).or_default().insert(
            constraint_id.clone(),
            ConstraintResult {
                constraint_type,
                constraint_result,
                info,
            },
        )
    }

    pub fn get_constraint(
        &self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
    ) -> Option<&ConstraintResult> {
        self.results.get(case_id)?.get(constraint_id)
    }

    // check if case already exists (or if it is a new case)
    pub fn has_case(&self, case_id: &CaseId) -> bool {
        self.results.contains_key(case_id)
    }

    pub fn has_violations(&self, case_id: &CaseId) -> bool {
        self.results.get(case_id).is_some_and(|case| {
            case.values().any(|r| {
                matches!(
                    r.constraint_result,
                    ConstraintResultType::Violated | ConstraintResultType::TemporarilyViolated
                )
            })
        })
    }

    pub fn violations(&self, case_id: &CaseId) -> Vec<(&ConstraintId, &ConstraintResult)> {
        self.results
            .get(case_id)
            .map(|case| {
                case.iter()
                    .filter(|(_, r)| {
                        matches!(
                            r.constraint_result,
                            ConstraintResultType::Violated
                                | ConstraintResultType::TemporarilyViolated
                        )
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn summary(&self, case_id: &CaseId) -> String {
        self.results
            .get(case_id)
            .map(|case| {
                case.iter()
                    .map(|(id, r)| format!("{}: {:?} — {}", id, r.constraint_result, r.info))
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .unwrap_or_else(|| format!("no results for case {}", case_id))
    }

    // remove case after case end
    pub fn remove_case(
        &mut self,
        case_id: &CaseId,
    ) -> Option<HashMap<ConstraintId, ConstraintResult>> {
        self.results.remove(case_id)
    }
}

impl Default for CCResultStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ConstraintResultType, ConstraintType};

    #[test]
    fn new_store_has_no_cases() {
        let store = CCResultStore::new();
        assert!(!store.has_case(&"case-1".to_string()));
    }

    #[test]
    fn upsert_changes_result() {
        let mut store = CCResultStore::new();
        store.register_case("case-1".to_string());

        store.upsert(
            &"case-1".to_string(),
            &"c-response".to_string(),
            ConstraintType::Response,
            ConstraintResultType::Violated,
            "token still open".to_string(),
        );

        let result = store
            .get_constraint(&"case-1".to_string(), &"c-response".to_string())
            .unwrap();
        assert!(matches!(
            result.constraint_result,
            ConstraintResultType::Violated
        ));
        assert_eq!(result.info, "token still open");
    }

    #[test]
    fn get_constraint_unknown_case_returns_none() {
        let store = CCResultStore::new();
        assert!(
            store
                .get_constraint(&"ghost".to_string(), &"c".to_string())
                .is_none()
        );
    }

    #[test]
    fn get_constraint_unknown_constraint_returns_none() {
        let mut store = CCResultStore::new();
        store.register_case("case-1".to_string());
        assert!(
            store
                .get_constraint(&"case-1".to_string(), &"ghost".to_string())
                .is_none()
        );
    }

    #[test]
    fn has_violations_false_when_all_satisfied() {
        let mut store = CCResultStore::new();
        store.register_case("case-1".to_string());
        assert!(!store.has_violations(&"case-1".to_string()));
    }

    #[test]
    fn has_violations_true_after_violated_update() {
        let mut store = CCResultStore::new();
        store.register_case("case-1".to_string());
        store.upsert(
            &"case-1".to_string(),
            &"c-response".to_string(),
            ConstraintType::Response,
            ConstraintResultType::Violated,
            "violated".to_string(),
        );
        assert!(store.has_violations(&"case-1".to_string()));
    }

    #[test]
    fn has_violations_true_for_temporarily_violated() {
        let mut store = CCResultStore::new();
        store.register_case("case-1".to_string());
        store.upsert(
            &"case-1".to_string(),
            &"c-response".to_string(),
            ConstraintType::Response,
            ConstraintResultType::TemporarilyViolated,
            "temp".to_string(),
        );
        assert!(store.has_violations(&"case-1".to_string()));
    }

    #[test]
    fn violations_returns_only_violated_constraints() {
        let mut store = CCResultStore::new();
        store.register_case("case-1".to_string());
        store.upsert(
            &"case-1".to_string(),
            &"c-response".to_string(),
            ConstraintType::Response,
            ConstraintResultType::Violated,
            "bad".to_string(),
        );

        let viols = store.violations(&"case-1".to_string());
        assert_eq!(viols.len(), 1);
        assert_eq!(*viols[0].0, "c-response".to_string());
    }

    #[test]
    fn remove_case_cleans_up() {
        let mut store = CCResultStore::new();
        store.register_case("case-1".to_string());
        let removed = store.remove_case(&"case-1".to_string());
        assert!(removed.is_some());
        assert!(!store.has_case(&"case-1".to_string()));
    }

    #[test]
    fn summary_returns_message_for_unknown_case() {
        let store = CCResultStore::new();
        let s = store.summary(&"ghost".to_string());
        assert!(s.contains("ghost"));
        assert!(s.contains("no results"));
    }
}
