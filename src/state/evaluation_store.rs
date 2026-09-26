//! keeps the evaluation of a trigger based check so that a late correction of the check is possible
//! late corrections can happen if the information of a timely event is delayed by network or processing queues

use std::collections::HashMap;
use std::net::SocketAddr;

use crate::types::{CaseId, EventInstanceId, EventTime, HandlerIndex};

#[derive(Debug, Clone)]
pub enum EvaluationDetail {
    /// AppearanceCounterHandler: what the trigger evaluated
    Counter { evaluated_count: u64 },
    /// SensorCollectorHandler: the event time of the value the trigger used
    /// None when no value was present
    SensorValue { value_time: Option<EventTime> },
    /// DistributedAppearanceCounterHandler: where to send a late counter update
    DistributedCounterReport { trigger_address: SocketAddr },
    /// DistributedAppearanceTriggerHandler: what each counter peer reported at trigger time.
    /// 'pending_pull' is true while the trigger is still collecting the counter tokens
    DistributedCounterTotals {
        per_peer: HashMap<SocketAddr, u64>,
        pending_pull: bool,
    },
}

/// result of applying a late delta: the corrected total, and whether the trigger is still pulling
#[derive(Debug, Clone, Copy)]
pub struct PeerTotalUpdate {
    pub total: u64,
    pub pending_pull: bool,
}

#[derive(Debug, Clone)]
pub struct LastEvaluation {
    /// event time of the trigger event
    pub trigger_time: EventTime,
    /// instance id of the triggering event, for measurement attribution
    pub triggering_event: Option<EventInstanceId>,
    pub detail: EvaluationDetail,
}

/// (case, handler) -> the evaluation that the handler last performed
#[derive(Debug, Default)]
pub struct EvaluationStore {
    evaluations: HashMap<CaseId, HashMap<HandlerIndex, LastEvaluation>>,
}

impl EvaluationStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, case_id: &CaseId, index: HandlerIndex, evaluation: LastEvaluation) {
        self.evaluations
            .entry(case_id.clone())
            .or_default()
            .insert(index, evaluation);
    }

    pub fn get_cloned(&self, case_id: &CaseId, index: HandlerIndex) -> Option<LastEvaluation> {
        self.evaluations.get(case_id)?.get(&index).cloned()
    }

    // returns None when no evaluation was recorded or it is not a counter evaluation
    pub fn increment_evaluated_count(
        &mut self,
        case_id: &CaseId,
        index: HandlerIndex,
    ) -> Option<u64> {
        let evaluation = self.evaluations.get_mut(case_id)?.get_mut(&index)?;
        match &mut evaluation.detail {
            EvaluationDetail::Counter { evaluated_count } => {
                *evaluated_count += 1;
                Some(*evaluated_count)
            }
            _ => None,
        }
    }

    /// move the "value the trigger used" forward after a late sensor value was added so that a further late value can chain onto this correction
    #[allow(clippy::collapsible_if)]
    pub fn set_evaluated_value_time(
        &mut self,
        case_id: &CaseId,
        index: HandlerIndex,
        value_time: EventTime,
    ) {
        if let Some(evaluation) = self
            .evaluations
            .get_mut(case_id)
            .and_then(|inner| inner.get_mut(&index))
        {
            if let EvaluationDetail::SensorValue { value_time: slot } = &mut evaluation.detail {
                *slot = Some(value_time);
            }
        }
    }

    /// apply a late delta reported by a counter peer and return the corrected total across all
    /// peers, together with whether the trigger is still pulling. while a pull is outstanding the
    /// caller must NOT emit a verdict: the total is incomplete and the pull will include the delta
    /// when it merges its own results
    pub fn add_to_peer_total(
        &mut self,
        case_id: &CaseId,
        index: HandlerIndex,
        peer: SocketAddr,
        delta: u64,
    ) -> Option<PeerTotalUpdate> {
        let evaluation = self.evaluations.get_mut(case_id)?.get_mut(&index)?;
        match &mut evaluation.detail {
            EvaluationDetail::DistributedCounterTotals {
                per_peer,
                pending_pull,
            } => {
                *per_peer.entry(peer).or_insert(0) += delta;
                Some(PeerTotalUpdate {
                    total: per_peer.values().sum(),
                    pending_pull: *pending_pull,
                })
            }
            _ => None,
        }
    }

    /// add the counts a completed pull collected to whatever is already recorded and close the pull (this enables late deltas)
    pub fn merge_peer_totals(
        &mut self,
        case_id: &CaseId,
        index: HandlerIndex,
        pulled: HashMap<SocketAddr, u64>,
    ) -> Option<u64> {
        let evaluation = self.evaluations.get_mut(case_id)?.get_mut(&index)?;
        match &mut evaluation.detail {
            EvaluationDetail::DistributedCounterTotals {
                per_peer,
                pending_pull,
            } => {
                for (peer, count) in pulled {
                    *per_peer.entry(peer).or_insert(0) += count;
                }
                *pending_pull = false;
                Some(per_peer.values().sum())
            }
            _ => None,
        }
    }

    pub fn case_count(&self) -> usize {
        self.evaluations.len()
    }

    pub fn remove(&mut self, case_id: &CaseId, index: HandlerIndex) -> Option<LastEvaluation> {
        let inner = self.evaluations.get_mut(case_id)?;
        let evaluation = inner.remove(&index);
        if inner.is_empty() {
            self.evaluations.remove(case_id);
        }
        evaluation
    }

    pub fn remove_case(
        &mut self,
        case_id: &CaseId,
    ) -> Option<HashMap<HandlerIndex, LastEvaluation>> {
        self.evaluations.remove(case_id)
    }
}
