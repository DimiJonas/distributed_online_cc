use std::net::SocketAddr;

use async_trait::async_trait;

use crate::{
    config::node_config::{AppearanceCounterReset, MinMaxMode},
    conformance_checking::{
        constraint_logic::constraint_handler_trait::ConstraintHandler,
        result_emitter::{emit_result_stamped, stamp_decision},
    },
    impl_constraint_handler_fields,
    measurement::{
        self,
        record::{CorrectionDropReason, ResultTrigger},
    },
    network::{
        message_types::{CCMessage, CCMessageType, PeerMessage},
        tcp_router::send_to_conformance_peer,
    },
    state::{
        app_state::AppState,
        evaluation_store::{EvaluationDetail, LastEvaluation},
    },
    types::{
        CaseId, ConstraintId, ConstraintResultType, ConstraintRole, ConstraintType, EventId,
        HandlerIndex, RequestId, Token, TokenPayload, TracedEvent,
    },
};

pub struct AppearanceCounterHandler {
    pub index: HandlerIndex,
    pub role: ConstraintRole,
    pub event_id: EventId,
    pub constraint_id: ConstraintId,
    pub mode: MinMaxMode,
    pub frequency: u64,
    pub reset: AppearanceCounterReset,
}

#[async_trait]
impl ConstraintHandler for AppearanceCounterHandler {
    impl_constraint_handler_fields!(AppearanceCounterHandler);

    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::info!(
            "appearance counter handler: received configured event for constraint {}; event={}",
            self.constraint_id,
            event
        );

        // check if a trigger event already evaluated the counter (and update the counter atomically)
        let (late_evaluation, count) = {
            let evaluations = state.evaluations.lock().unwrap();
            let late_evaluation = match evaluations.get_cloned(&event.case_id, self.index) {
                Some(evaluation) if event.timestamp <= evaluation.trigger_time => Some(evaluation),
                _ => None,
            };

            // a late event belongs to the window that was already evaluated and should be counted there (depending on the reset policy)
            let skip_count = match &late_evaluation {
                None => false,
                Some(evaluation) => match self.reset {
                    AppearanceCounterReset::Keep => false,
                    AppearanceCounterReset::Zero => true,
                    AppearanceCounterReset::Subtract => match &evaluation.detail {
                        EvaluationDetail::Counter { evaluated_count } => {
                            *evaluated_count < self.frequency
                        }
                        // not a counter evaluation - fall back to counting it
                        _ => false,
                    },
                },
            };

            let count = if skip_count {
                None
            } else {
                let mut meta_tokens = state.meta_token_store.lock().unwrap();
                Some(
                    match meta_tokens.get_token_mut(&event.case_id, self.index) {
                        Some(counter_token) => {
                            counter_token.payload.increment_counter();
                            counter_token.payload.get_count()
                        }
                        None => {
                            log::info!(
                                "appearance counter handler: starting counter for event_id={} constraint_id={} case_id={}",
                                self.event_id,
                                self.constraint_id,
                                event.case_id
                            );

                            let counter_token = Token::new_with_payload(
                                event.case_id.clone(),
                                self.constraint_id.clone(),
                                ConstraintType::Appearance,
                                None,
                                TokenPayload::Counter(1),
                            );
                            meta_tokens.insert_meta_token(counter_token.clone(), self.index);
                            counter_token.payload.get_count()
                        }
                    },
                )
            };

            (late_evaluation, count)
        };

        if let Some(evaluation) = late_evaluation {
            self.correct_with_late_event(event, evaluation, state.clone())
                .await;
        }

        match count {
            Some(count) => log::info!("appearance counter handler: counter is now at {}", count),
            None => log::info!(
                "appearance counter handler: event was folded into the recorded evaluation, counter left unchanged"
            ),
        }
    }

    async fn handle_message(
        &self,
        state: AppState,
        message: CCMessage,
        _request_id: Option<RequestId>,
    ) {
        log::info!(
            "appearance counter handler: received message for constraint {}; message={}",
            self.constraint_id,
            message
        );
        log::debug!("message={:?}", message);

        if message.message_type != CCMessageType::TriggerCheck {
            log::warn!(
                "appearance counter handler: unexpected message type {} for constraint {}, ignoring",
                message.message_type,
                self.constraint_id
            );
            return;
        }

        let pending = {
            let mut evaluations = state.evaluations.lock().unwrap();
            let count = self.get_and_reset_counter(&message.case_id, state.clone());

            // remember the evaluation for late corrections
            match message.timestamp {
                Some(trigger_time) => {
                    evaluations.insert(
                        &message.case_id,
                        self.index,
                        LastEvaluation {
                            trigger_time,
                            triggering_event: message.triggering_event,
                            detail: EvaluationDetail::Counter {
                                evaluated_count: count,
                            },
                        },
                    );
                }
                None => {
                    log::warn!(
                        "appearance counter handler: TriggerCheck for ({}/{}) has no timestamp. late events will not be able to correct this result",
                        message.case_id,
                        self.constraint_id
                    );
                }
            }

            self.evaluate_counter(count).map(|(info, result)| {
                (
                    stamp_decision(&state, &message.case_id, &self.constraint_id),
                    info,
                    result,
                )
            })
        };

        if let Some((stamp, info, result)) = pending {
            emit_result_stamped(
                stamp,
                &message.case_id,
                &self.constraint_id,
                ConstraintType::Appearance,
                result,
                info,
                state.clone(),
                ResultTrigger::Event,
                message.triggering_event,
            )
            .await;
        }
    }

    async fn handle_case_end(&self, case_id: &CaseId, state: AppState) {
        log::info!(
            "appearance counter handler: received case end signal for constraint {}; case={}",
            self.constraint_id,
            case_id
        );

        let pending = {
            let results = state.cc_results.lock().unwrap();
            if results
                .get_constraint(case_id, &self.constraint_id)
                .is_some()
            {
                None
            } else {
                Some(stamp_decision(&state, case_id, &self.constraint_id))
            }
        };
        if let Some(stamp) = pending {
            // produce result because none was emitted yet
            emit_result_stamped(
                stamp,
                case_id,
                &self.constraint_id,
                ConstraintType::Appearance,
                ConstraintResultType::Satisfied,
                "counter evaluation was never triggered".to_string(),
                state.clone(),
                ResultTrigger::CaseEnd,
                None,
            )
            .await;
        }
    }

    async fn clean_up_state(&self, case_id: &CaseId, state: AppState) {
        log::debug!(
            "appearance counter handler: clean up state for ({}/{})",
            case_id,
            self.constraint_id
        );

        {
            let mut evaluations = state.evaluations.lock().unwrap();
            evaluations.remove(case_id, self.index);
        }
        // remove counter token (meta)
        {
            let mut meta_tokens = state.meta_token_store.lock().unwrap();
            meta_tokens.remove_meta_token(case_id, self.index);
        }
    }
}

impl AppearanceCounterHandler {
    async fn correct_with_late_event(
        &self,
        event: &TracedEvent,
        evaluation: LastEvaluation,
        state: AppState,
    ) {
        let (corrected, stamp) = {
            let mut evaluations = state.evaluations.lock().unwrap();
            match evaluations.increment_evaluated_count(&event.case_id, self.index) {
                Some(count) => (
                    count,
                    stamp_decision(&state, &event.case_id, &self.constraint_id),
                ),
                None => {
                    log::error!(
                        "appearance counter handler: no counter evaluation recorded for ({}/{}). cannot correct with late event",
                        event.case_id,
                        self.constraint_id
                    );
                    measurement::api::correction_dropped(
                        &event.case_id,
                        &self.constraint_id,
                        ConstraintType::Appearance,
                        CorrectionDropReason::NoRecordedEvaluation,
                        None,
                    );
                    return;
                }
            }
        };

        log::info!(
            "appearance counter handler: late event for ({}/{}) (event time {} <= trigger time {}). corrected counter is now {}",
            event.case_id,
            self.constraint_id,
            event.timestamp,
            evaluation.trigger_time,
            corrected
        );

        let (info, result) = match self.evaluate_counter(corrected) {
            Some((info, result)) => (format!("{info} (late correction)"), result),
            None => (
                format!(
                    "CounterOk: counter {:?} within limit {:?} (late correction)",
                    corrected, self.frequency
                ),
                ConstraintResultType::Satisfied,
            ),
        };

        emit_result_stamped(
            stamp,
            &event.case_id,
            &self.constraint_id,
            ConstraintType::Appearance,
            result,
            info,
            state.clone(),
            ResultTrigger::Event,
            Some(event.instance_id),
        )
        .await;
    }

    /// read and reset the counter under the meta token lock
    fn get_and_reset_counter(&self, case_id: &CaseId, state: AppState) -> u64 {
        let mut meta_tokens = state.meta_token_store.lock().unwrap();
        match meta_tokens.get_token_mut(case_id, self.index) {
            Some(counter_token) => {
                let count = counter_token.payload.get_count();
                // reset counter while store is locked
                match self.reset {
                    AppearanceCounterReset::Keep => {}
                    AppearanceCounterReset::Subtract => {
                        counter_token.payload.subtract_counter(self.frequency);
                    }
                    AppearanceCounterReset::Zero => {
                        counter_token.payload.reset_counter();
                    }
                }
                count
            }
            None => 0,
        }
    }

    fn evaluate_counter(&self, count: u64) -> Option<(String, ConstraintResultType)> {
        match self.mode {
            MinMaxMode::Min => {
                if count < self.frequency {
                    Some((
                        format!(
                            "CounterMin: counter {:?} < limit {:?}",
                            count, self.frequency
                        ),
                        ConstraintResultType::Violated,
                    ))
                } else {
                    None
                }
            }
            MinMaxMode::Max => {
                if count > self.frequency {
                    Some((
                        format!(
                            "CounterMax: counter {:?} > limit {:?}",
                            count, self.frequency
                        ),
                        ConstraintResultType::Violated,
                    ))
                } else {
                    None
                }
            }
        }
    }
}

pub struct AppearanceTriggerHandler {
    pub index: HandlerIndex,
    pub role: ConstraintRole,
    pub event_id: EventId,
    pub constraint_id: ConstraintId,
    pub counter_peers: Vec<SocketAddr>,
}

#[async_trait]
impl ConstraintHandler for AppearanceTriggerHandler {
    impl_constraint_handler_fields!(AppearanceTriggerHandler);

    fn configured_peers(&self) -> Vec<SocketAddr> {
        self.counter_peers.clone()
    }

    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::info!(
            "appearance trigger handler: received configured event for constraint {}; event={}",
            self.constraint_id,
            event
        );

        log::info!("appearance trigger handler: triggering counter evaluation on counter peers");

        let message = PeerMessage::conformance(CCMessage::trigger_check(
            event.case_id.clone(),
            self.constraint_id.clone(),
            event.timestamp,
            event.instance_id,
        ));
        for &peer_address in self.counter_peers.iter() {
            if let Err(e) =
                send_to_conformance_peer(peer_address, message.clone(), state.clone()).await
            {
                log::error!(
                    "appearance trigger handler: error sending cc trigger message to {}: {}",
                    peer_address,
                    e
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use uuid::Uuid;

    use crate::{
        config::node_config::{AppearanceCounterReset, ConstraintConfig, MinMaxMode},
        conformance_checking::constraint_logic::{
            appearance_constraint_handler::AppearanceCounterHandler,
            constraint_handler_trait::ConstraintHandler,
        },
        network::message_types::CCMessage,
        state::app_state::AppState,
        types::{ConstraintResultType, ConstraintRole, EventInstanceId},
        util::test_tools::{
            make_config_with_constraints_and_port, make_state_from_config,
            make_traced_event_with_fixed_timestamp,
        },
    };

    const CID: &str = "app";
    const EID: &str = "count";

    fn counter_state(port: u16, case: &str, mode: MinMaxMode, frequency: u64) -> AppState {
        let config = make_config_with_constraints_and_port(
            vec![ConstraintConfig::AppearanceCounter {
                constraint_id: CID.to_string(),
                event_id: EID.to_string(),
                mode,
                frequency,
                reset: AppearanceCounterReset::Keep,
            }],
            port,
        );
        let state = make_state_from_config(config);
        state
            .cc_results
            .lock()
            .unwrap()
            .register_case(case.to_string());
        state
    }

    fn handler(mode: MinMaxMode, frequency: u64) -> AppearanceCounterHandler {
        AppearanceCounterHandler {
            index: 1,
            role: ConstraintRole::AppearanceCounter,
            event_id: EID.to_string(),
            constraint_id: CID.to_string(),
            mode,
            frequency,
            reset: AppearanceCounterReset::Keep,
        }
    }

    fn trigger(case: &str) -> CCMessage {
        CCMessage::trigger_check(
            case.to_string(),
            CID.to_string(),
            Utc::now(),
            EventInstanceId::new_v4(),
        )
    }

    fn result_type(state: &AppState, case: &str) -> Option<ConstraintResultType> {
        let store = state.cc_results.lock().unwrap();
        store
            .get_constraint(&case.to_string(), &CID.to_string())
            .map(|r| r.constraint_result)
    }

    /// none (no counter, no trigger), Max mode → count 0 <= freq → Satisfied at case end.
    #[tokio::test]
    async fn none_satisfied_at_case_end_max() {
        let case = "c1";
        let state = counter_state(25000, case, MinMaxMode::Max, 2);
        handler(MinMaxMode::Max, 2)
            .handle_case_end(&case.to_string(), state.clone())
            .await;
        assert!(matches!(
            result_type(&state, case),
            Some(ConstraintResultType::Satisfied)
        ));
    }

    /// only trigger event, no counter events → evaluated against counter 0.
    /// Min mode, frequency 1: 0 >= 1 is false → Violated.
    #[tokio::test]
    async fn only_trigger_evaluated_against_zero() {
        let case = "c2";
        let state = counter_state(25010, case, MinMaxMode::Min, 1);
        handler(MinMaxMode::Min, 1)
            .handle_message(state.clone(), trigger(case), Some(Uuid::new_v4()))
            .await;
        assert!(matches!(
            result_type(&state, case),
            Some(ConstraintResultType::Violated)
        ));
    }

    /// both events, counter satisfies Min frequency -> no violation/result.
    #[tokio::test]
    async fn both_min_no_violation() {
        let case = "c3";
        let state = counter_state(25020, case, MinMaxMode::Min, 2);
        let h = handler(MinMaxMode::Min, 2);
        h.handle_event(
            &make_traced_event_with_fixed_timestamp(case, EID),
            state.clone(),
        )
        .await;
        h.handle_event(
            &make_traced_event_with_fixed_timestamp(case, EID),
            state.clone(),
        )
        .await;
        h.handle_message(state.clone(), trigger(case), Some(Uuid::new_v4()))
            .await;
        assert!(result_type(&state, case).is_none());
    }

    /// both events, counter below Min frequency -> Violated.
    #[tokio::test]
    async fn both_min_violated() {
        let case = "c4";
        let state = counter_state(25030, case, MinMaxMode::Min, 3);
        let h = handler(MinMaxMode::Min, 3);
        h.handle_event(
            &make_traced_event_with_fixed_timestamp(case, EID),
            state.clone(),
        )
        .await;
        h.handle_event(
            &make_traced_event_with_fixed_timestamp(case, EID),
            state.clone(),
        )
        .await;
        h.handle_message(state.clone(), trigger(case), Some(Uuid::new_v4()))
            .await;
        assert!(matches!(
            result_type(&state, case),
            Some(ConstraintResultType::Violated)
        ));
    }

    /// only counter events (no trigger) → Satisfied, because the
    /// constraint should only be judged when a trigger checks it.
    #[tokio::test]
    async fn only_counter_events_satisfied_desired() {
        let case = "c5";
        let state = counter_state(25040, case, MinMaxMode::Max, 2);
        let h = handler(MinMaxMode::Max, 2);
        for _ in 0..5 {
            h.handle_event(
                &make_traced_event_with_fixed_timestamp(case, EID),
                state.clone(),
            )
            .await;
        }
        h.handle_case_end(&case.to_string(), state.clone()).await;
        assert!(
            matches!(
                result_type(&state, case),
                Some(ConstraintResultType::Satisfied)
            ),
            "counter-only (no trigger) should be Satisfied"
        );
    }
}
