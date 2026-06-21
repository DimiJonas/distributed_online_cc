use std::collections::HashMap;

use crate::types::CaseId;

/// case lifecycle: open -> ended -> closed
#[derive(Debug, Hash, Eq, PartialEq, Clone, Copy)]
pub enum CaseState {
    Open,
    Ended,
    Closed,
}

pub struct CaseStateMap {
    case_state: HashMap<CaseId, CaseState>,
}

impl CaseStateMap {
    pub fn new() -> Self {
        Self {
            case_state: HashMap::new(),
        }
    }

    pub fn has_case(&self, case_id: &CaseId) -> bool {
        self.case_state.contains_key(case_id)
    }

    pub fn is_ended(&self, case_id: &CaseId) -> bool {
        self.case_state.get(case_id) == Some(&CaseState::Ended)
    }

    pub fn is_closed(&self, case_id: &CaseId) -> bool {
        self.case_state.get(case_id) == Some(&CaseState::Closed)
    }

    pub fn open_case(&mut self, case_id: CaseId) {
        self.case_state.insert(case_id, CaseState::Open);
    }

    pub fn end_case(&mut self, case_id: CaseId) {
        self.case_state.insert(case_id, CaseState::Ended);
    }

    pub fn close_case(&mut self, case_id: CaseId) {
        self.case_state.insert(case_id, CaseState::Closed);
    }

    pub fn remove(&mut self, case_id: &CaseId) -> Option<CaseState> {
        self.case_state.remove(case_id)
    }
}

impl Default for CaseStateMap {
    fn default() -> Self {
        Self::new()
    }
}
