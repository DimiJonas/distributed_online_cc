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

    // register a new case by initializing a constraint result map with according to the constraint configuration
    pub fn register_case(
        &mut self,
        case_id: CaseId,
        constraints: &[(ConstraintId, ConstraintType)],
    ) {
        let case_results = constraints
            .iter()
            .map(|(cid, ctype)| {
                (
                    cid.clone(),
                    ConstraintResult {
                        constraint_type: ctype.clone(),
                        constraint_result: ctype.initial_result(),
                        info: format!("{:?}: initialized constraint", ctype),
                        history: String::new(),
                    },
                )
            })
            .collect();
        self.results.insert(case_id, case_results);
    }

    /*
       update a constraint result
       info gives some information on the result change
       history preserves the previous states of the constraint result with corresponding info
    */
    pub fn update(
        &mut self,
        case_id: &CaseId,
        constraint_id: &ConstraintId,
        new_result: ConstraintResultType,
        info: String,
    ) {
        if let Some(case) = self.results.get_mut(case_id) {
            if let Some(existing) = case.get_mut(constraint_id) {
                let history_entry = format!(
                    "{}\n{:?}: {}",
                    existing.history, existing.constraint_result, existing.info
                );
                existing.constraint_result = new_result;
                existing.info = info;
                existing.history = history_entry;
            }
        }
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
        self.results.get(case_id).is_some()
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
