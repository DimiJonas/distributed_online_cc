use std::{net::SocketAddr, time::Duration};

use async_trait::async_trait;
use chrono::Utc;
use process_mining::core::event_data::case_centric::AttributeValue;

use crate::{
    config::node_config::SensorConstraintMode,
    conformance_checking::{
        constraint_logic::constraint_handler_trait::ConstraintHandler,
        result_emitter::emit_result_at,
    },
    impl_constraint_handler_fields,
    measurement::record::ResultTrigger,
    network::{
        message_types::{CCMessage, CCMessageType, PeerMessage},
        tcp_router::send_to_conformance_peer,
    },
    state::{
        app_state::AppState,
        evaluation_store::{EvaluationDetail, LastEvaluation},
        sensor_event_store::SensorValueRecorded,
    },
    types::{
        CaseId, ConstraintId, ConstraintResultType, ConstraintRole, ConstraintType, EventId,
        HandlerIndex, RequestId, SensorAttributeThreshold, Token, TracedEvent,
    },
};

pub struct SensorCollectorHandler {
    pub index: HandlerIndex,
    pub role: ConstraintRole,
    pub constraint_id: ConstraintId,
    pub event_id: EventId,
    pub attribute_key: String,
    pub value: SensorAttributeThreshold,
    pub mode: SensorConstraintMode,
    pub max_value_age: Duration,
}

#[async_trait]
impl ConstraintHandler for SensorCollectorHandler {
    impl_constraint_handler_fields!(SensorCollectorHandler);

    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::info!(
            "sensor constraint handler: received configured sensor event for constraint {}; event={}",
            self.constraint_id,
            event
        );

        let attribute_value = match event.get_attribute_by_key(&self.attribute_key) {
            Some(value) => match value {
                AttributeValue::Float(f) => f,
                AttributeValue::Int(i) => &(*i as f64),
                _ => {
                    log::error!(
                        "sensor constraint handler: cannot compare attribute {} with value {}, because it is not a numeric value",
                        self.attribute_key,
                        value
                    );
                    return;
                }
            },
            None => {
                log::error!(
                    "sensor constraint handler: event {} does not contain a value for attribute {}. ignoring",
                    event,
                    self.attribute_key
                );
                return;
            }
        };

        // collecting the value and checking whether a trigger already evaluated atomically
        let late_evaluation = {
            let mut sensor_events = state.sensor_events.lock().unwrap();
            let record_outcome = sensor_events.record(
                event.case_id.clone(),
                self.attribute_key.clone(),
                *attribute_value,
                event.timestamp,
            );
            match record_outcome {
                SensorValueRecorded::Duplicate => {
                    log::info!("sensor collector handler: sensor value NOT collected (duplicate)",);
                }
                _ => {
                    log::info!(
                        "sensor collector handler: sensor value collected ({:?})",
                        record_outcome
                    );
                }
            }

            // check if late correction
            let evaluations = state.evaluations.lock().unwrap();
            match evaluations.get_cloned(&event.case_id, self.index) {
                Some(evaluation) if event.timestamp <= evaluation.trigger_time => Some(evaluation),
                _ => None,
            }
        };
        if let Some(evaluation) = late_evaluation {
            self.correct_with_late_value(event, evaluation, state).await;
        }
    }

    async fn handle_message(
        &self,
        state: AppState,
        message: CCMessage,
        _request_id: Option<RequestId>,
    ) {
        log::info!(
            "sensor collector handler: received message for constraint {}; message={}",
            self.constraint_id,
            message
        );
        log::debug!("message={:?}", message);

        if message.message_type != CCMessageType::TriggerCheck {
            log::warn!(
                "sensor collector handler: unexpected message type {:?} for constraint {}, ignoring",
                message.message_type,
                self.constraint_id
            );
            return;
        }

        let trigger_event_time = match message.timestamp {
            Some(event_time) => event_time,
            None => {
                log::error!(
                    "sensor collector handler: TriggerCheck message for constraint {} has no timestamp, cannot evaluate",
                    self.constraint_id
                );
                return;
            }
        };

        let (sensor_event_map_entry, entry_read_at) = {
            let sensor_events = state.sensor_events.lock().unwrap();
            let entry = sensor_events.value_at_cloned(
                &message.case_id,
                &self.attribute_key,
                trigger_event_time,
            );

            // remember the evaluation for late corrections
            {
                let mut evaluations = state.evaluations.lock().unwrap();
                evaluations.insert(
                    &message.case_id,
                    self.index,
                    LastEvaluation {
                        trigger_time: trigger_event_time,
                        triggering_event: message.triggering_event,
                        detail: EvaluationDetail::SensorValue {
                            value_time: entry.as_ref().map(|e| e.timestamp),
                        },
                    },
                );
            }

            (entry, Utc::now())
        };

        // get value, emit violated result if none exists or its outdated
        let attribute_value: f64 = {
            match sensor_event_map_entry {
                Some(entry) => {
                    // check if entry is outdated
                    let value_age = trigger_event_time - entry.timestamp;
                    if value_age.to_std().unwrap_or(Duration::MAX) > self.max_value_age {
                        let info = format!(
                            "sensor value for case={} key={} is outdated. value age relative to trigger event: {} > {} (max allowed age)",
                            message.case_id,
                            self.attribute_key,
                            humantime::format_duration(value_age.to_std().unwrap_or_default()),
                            humantime::format_duration(self.max_value_age),
                        );
                        emit_result_at(
                            entry_read_at,
                            &message.case_id,
                            &self.constraint_id,
                            ConstraintType::Sensor,
                            ConstraintResultType::Violated,
                            info,
                            state.clone(),
                            ResultTrigger::Event,
                            message.triggering_event,
                        )
                        .await;
                        return;
                    }
                    entry.value
                }
                None => {
                    // no value exists
                    let info = format!(
                        "no sensor value exists for case={} key={}",
                        message.case_id, self.attribute_key
                    );
                    emit_result_at(
                        entry_read_at,
                        &message.case_id,
                        &self.constraint_id,
                        ConstraintType::Sensor,
                        ConstraintResultType::Violated,
                        info,
                        state.clone(),
                        ResultTrigger::Event,
                        message.triggering_event,
                    )
                    .await;
                    return;
                }
            }
        };

        // evaluate value and produce result result
        let condition_met = self.mode.evaluate_condition(attribute_value, self.value);
        log::info!(
            "sensor collector handler: evaluated ({}/{}): {} {} {} -> {}",
            message.case_id,
            self.constraint_id,
            attribute_value,
            self.mode,
            self.value,
            if condition_met {
                "condition met, no violation emitted"
            } else {
                "condition not met, emitting violation"
            }
        );
        if !condition_met {
            // produce negative result
            emit_result_at(
                entry_read_at,
                &message.case_id,
                &self.constraint_id,
                ConstraintType::Sensor,
                ConstraintResultType::Violated,
                format!("{} {} {} is false", attribute_value, self.mode, self.value),
                state.clone(),
                ResultTrigger::Event,
                message.triggering_event,
            )
            .await;
        };

        // record that value was checked via creating a meta token for better result information
        {
            let mut meta_tokens = state.meta_token_store.lock().unwrap();
            meta_tokens.insert_meta_token(
                Token::new(
                    message.case_id.clone(),
                    self.constraint_id.clone(),
                    ConstraintType::Sensor,
                    None,
                    state.config.node_id,
                ),
                self.index,
            );
        }
    }

    async fn handle_case_end(&self, case_id: &CaseId, state: AppState) {
        log::info!(
            "sensor collector handler: received case end signal for constraint {}; case={}",
            self.constraint_id,
            case_id
        );

        let result_exists = {
            let results = state.cc_results.lock().unwrap();
            results
                .get_constraint(case_id, &self.constraint_id)
                .is_some()
        };
        let (value_was_checked, state_read_at) = {
            let meta_tokens = state.meta_token_store.lock().unwrap();
            (
                meta_tokens.contains_meta_token(case_id, self.index),
                Utc::now(),
            )
        };
        if !result_exists {
            // produce result because none was emitted yet
            let info_str = {
                if value_was_checked {
                    // conditions was met because no violation was emitted
                    "condition was met"
                } else {
                    "sensor trigger event never happened"
                }
            };
            emit_result_at(
                state_read_at,
                case_id,
                &self.constraint_id,
                ConstraintType::Sensor,
                ConstraintResultType::Satisfied,
                info_str.to_string(),
                state.clone(),
                ResultTrigger::CaseEnd,
                None,
            )
            .await;
        }
    }

    async fn clean_up_state(&self, case_id: &CaseId, state: AppState) {
        log::debug!(
            "sensor collector handler: clean up state for ({}/{})",
            case_id,
            self.constraint_id
        );

        // remove sensor events
        {
            let mut sensor_events = state.sensor_events.lock().unwrap();
            sensor_events.remove_case(case_id);
        }
        // remove evaluations
        {
            let mut evaluations = state.evaluations.lock().unwrap();
            evaluations.remove(case_id, self.index);
        }

        // remove meta token
        {
            let mut meta_tokens = state.meta_token_store.lock().unwrap();
            meta_tokens.remove_meta_token(case_id, self.index);
        }
    }
}

impl SensorCollectorHandler {
    /// re-evaluate after a value arrived that the trigger should have seen (based on its timestamp) and emit
    /// the corrected verdict
    async fn correct_with_late_value(
        &self,
        event: &TracedEvent,
        evaluation: LastEvaluation,
        state: AppState,
    ) {
        let evaluated_value_time = match evaluation.detail {
            EvaluationDetail::SensorValue { value_time } => value_time,
            _ => {
                log::error!(
                    "sensor collector handler: recorded evaluation for ({}/{}) is not a sensor evaluation",
                    event.case_id,
                    self.constraint_id
                );
                return;
            }
        };

        let (sensor_value, value_read_at) = {
            let sensor_events = state.sensor_events.lock().unwrap();
            (
                sensor_events.value_at_cloned(
                    &event.case_id,
                    &self.attribute_key,
                    evaluation.trigger_time,
                ),
                Utc::now(),
            )
        };
        let sensor_value = match sensor_value {
            Some(entry) => entry,
            None => {
                // nothing known at or before the trigger
                return;
            }
        };

        // check if the trigger already evaluated exactly this value
        if evaluated_value_time == Some(sensor_value.timestamp) {
            log::debug!(
                "sensor collector handler: late value for ({}/{}) at {} does not change the value as of trigger time {}. no correction needed",
                event.case_id,
                self.constraint_id,
                event.timestamp,
                evaluation.trigger_time
            );
            return;
        }

        let value_age = evaluation.trigger_time - sensor_value.timestamp;
        let (result, info) = if value_age.to_std().unwrap_or(Duration::MAX) > self.max_value_age {
            (
                ConstraintResultType::Violated,
                format!(
                    "sensor value for case={} key={} is outdated. value age relative to trigger event: {} > {} (max allowed age) (late correction)",
                    event.case_id,
                    self.attribute_key,
                    humantime::format_duration(value_age.to_std().unwrap_or_default()),
                    humantime::format_duration(self.max_value_age),
                ),
            )
        } else if self.mode.evaluate_condition(sensor_value.value, self.value) {
            (
                ConstraintResultType::Satisfied,
                format!(
                    "{} {} {} is true (late correction)",
                    sensor_value.value, self.mode, self.value
                ),
            )
        } else {
            (
                ConstraintResultType::Violated,
                format!(
                    "{} {} {} is false (late correction)",
                    sensor_value.value, self.mode, self.value
                ),
            )
        };

        log::debug!(
            "sensor collector handler: late value for ({}/{}) at {} changed the value as of trigger time {} from {:?} to {}; correcting to {:?}",
            event.case_id,
            self.constraint_id,
            event.timestamp,
            evaluation.trigger_time,
            evaluated_value_time,
            sensor_value.timestamp,
            result
        );

        emit_result_at(
            value_read_at,
            &event.case_id,
            &self.constraint_id,
            ConstraintType::Sensor,
            result,
            info,
            state.clone(),
            ResultTrigger::Event,
            Some(event.instance_id),
        )
        .await;

        // record new evaluation
        {
            let mut evaluations = state.evaluations.lock().unwrap();
            evaluations.set_evaluated_value_time(
                &event.case_id,
                self.index,
                sensor_value.timestamp,
            );
        }
    }
}

pub struct SensorTriggerHandler {
    pub index: HandlerIndex,
    pub role: ConstraintRole,
    pub constraint_id: ConstraintId,
    pub event_id: EventId,
    pub collector_peers: Vec<SocketAddr>,
}

#[async_trait]
impl ConstraintHandler for SensorTriggerHandler {
    impl_constraint_handler_fields!(SensorTriggerHandler);

    fn configured_peers(&self) -> Vec<SocketAddr> {
        self.collector_peers.clone()
    }

    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::info!(
            "sensor trigger handler: received configured event for constraint {}; event={}",
            self.constraint_id,
            event
        );

        log::info!(
            "sensor trigger handler: triggering sensor evaluation on colector peers for case={} constraint={}",
            event.case_id,
            self.constraint_id
        );
        let message = PeerMessage::conformance(CCMessage::trigger_check(
            event.case_id.clone(),
            self.constraint_id.clone(),
            event.timestamp,
            event.instance_id,
        ));
        for &peer_address in self.collector_peers.iter() {
            if let Err(e) =
                send_to_conformance_peer(peer_address, message.clone(), state.clone()).await
            {
                log::error!(
                    "sensor trigger handler: error sending cc trigger message to {}: {}",
                    peer_address,
                    e
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::{TimeZone, Utc};

    use crate::{
        config::node_config::{ConstraintConfig, SensorConstraintMode},
        conformance_checking::constraint_logic::{
            constraint_handler_trait::ConstraintHandler,
            sensor_constraint_handler::SensorCollectorHandler,
        },
        network::message_types::CCMessage,
        state::app_state::AppState,
        types::{ConstraintResultType, ConstraintRole, EventInstanceId},
        util::test_tools::{
            make_config_with_constraints_and_port, make_sensor_event, make_state_from_config,
        },
    };

    const CID: &str = "sens";
    const EID: &str = "collect";
    const KEY: &str = "temp";
    const THRESHOLD: f64 = 50.0;

    fn collector_state(port: u16, case: &str) -> AppState {
        let config = make_config_with_constraints_and_port(
            vec![ConstraintConfig::SensorCollector {
                constraint_id: CID.to_string(),
                event_id: EID.to_string(),
                attribute_key: KEY.to_string(),
                value: THRESHOLD,
                mode: SensorConstraintMode::Gt,
                max_value_age: Duration::from_secs(60),
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

    fn handler() -> SensorCollectorHandler {
        SensorCollectorHandler {
            index: 0,
            role: ConstraintRole::SensorCollector,
            constraint_id: CID.to_string(),
            event_id: EID.to_string(),
            attribute_key: KEY.to_string(),
            value: THRESHOLD,
            mode: SensorConstraintMode::Gt,
            max_value_age: Duration::from_secs(60),
        }
    }

    /// TriggerCheck message at the given offset (seconds from 2024-01-01).
    fn trigger_at(case: &str, secs: i64) -> CCMessage {
        let base = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let ts = base + chrono::Duration::seconds(secs);
        CCMessage::trigger_check(
            case.to_string(),
            CID.to_string(),
            ts,
            EventInstanceId::new_v4(),
        )
    }

    fn result_type(state: &AppState, case: &str) -> Option<ConstraintResultType> {
        let store = state.cc_results.lock().unwrap();
        store
            .get_constraint(&case.to_string(), &CID.to_string())
            .map(|r| r.constraint_result)
    }

    /// neither collector nor trigger event → Satisfied at case end.
    #[tokio::test]
    async fn neither_satisfied_at_case_end() {
        let case = "c1";
        let state = collector_state(23000, case);
        handler()
            .handle_case_end(&case.to_string(), state.clone())
            .await;
        assert!(matches!(
            result_type(&state, case),
            Some(ConstraintResultType::Satisfied)
        ));
    }

    /// only collector events → Satisfied at case end.
    #[tokio::test]
    async fn only_collector_satisfied_at_case_end() {
        let case = "c2";
        let state = collector_state(23010, case);
        handler()
            .handle_event(&make_sensor_event(case, EID, KEY, 80.0, 0), state.clone())
            .await;
        assert!(result_type(&state, case).is_none());
        handler()
            .handle_case_end(&case.to_string(), state.clone())
            .await;
        assert!(matches!(
            result_type(&state, case),
            Some(ConstraintResultType::Satisfied)
        ));
    }

    /// only trigger event, no values → Violated.
    #[tokio::test]
    async fn only_trigger_violated_no_values() {
        let case = "c3";
        let state = collector_state(23020, case);
        handler()
            .handle_message(state.clone(), trigger_at(case, 10), None)
            .await;
        assert!(matches!(
            result_type(&state, case),
            Some(ConstraintResultType::Violated)
        ));
    }

    /// both, condition met (value 80 > 50) → Satisfied at case end.
    #[tokio::test]
    async fn both_condition_met_satisfied() {
        let case = "c4";
        let state = collector_state(23030, case);
        handler()
            .handle_event(&make_sensor_event(case, EID, KEY, 80.0, 0), state.clone())
            .await;
        handler()
            .handle_message(state.clone(), trigger_at(case, 10), None)
            .await;
        assert!(result_type(&state, case).is_none());
        handler()
            .handle_case_end(&case.to_string(), state.clone())
            .await;
        assert!(matches!(
            result_type(&state, case),
            Some(ConstraintResultType::Satisfied)
        ));
    }

    /// both, condition not met (value 30 > 50 is false) → Violated.
    #[tokio::test]
    async fn both_condition_unmet_violated() {
        let case = "c5";
        let state = collector_state(23040, case);
        handler()
            .handle_event(&make_sensor_event(case, EID, KEY, 30.0, 0), state.clone())
            .await;
        handler()
            .handle_message(state.clone(), trigger_at(case, 10), None)
            .await;
        assert!(matches!(
            result_type(&state, case),
            Some(ConstraintResultType::Violated)
        ));
    }

    /// value is stale (age 120s > 60s max) at trigger time → treated as no value → Violated.
    #[tokio::test]
    async fn stale_value_violated() {
        let case = "c6";
        let state = collector_state(23050, case);
        handler()
            .handle_event(&make_sensor_event(case, EID, KEY, 80.0, 0), state.clone())
            .await;
        handler()
            .handle_message(state.clone(), trigger_at(case, 120), None)
            .await;
        assert!(matches!(
            result_type(&state, case),
            Some(ConstraintResultType::Violated)
        ));
    }
}
