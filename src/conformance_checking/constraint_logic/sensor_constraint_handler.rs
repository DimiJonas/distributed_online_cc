use std::{net::SocketAddr, time::Duration};

use async_trait::async_trait;
use process_mining::core::event_data::case_centric::AttributeValue;

use crate::{
    config::node_config::SensorConstraintMode,
    conformance_checking::{
        constraint_logic::constraint_handler_trait::ConstraintHandler, result_emitter::emit_result,
    },
    impl_constraint_handler_fields,
    network::{
        message_types::{CCMessage, CCMessageType, PeerMessage},
        tcp_router::send_to_conformance_peer,
    },
    state::app_state::AppState,
    types::{
        CaseId, ConstraintId, ConstraintResultType, ConstraintRole, ConstraintType, EventId,
        HandlerIndex, RequestId, SensorAttributeThreshold, TracedEvent,
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

        // collect
        {
            let mut sensor_events = state.sensor_events.lock().unwrap();
            match sensor_events.insert_or_update(
                event.case_id.clone(),
                self.attribute_key.clone(),
                *attribute_value,
                event.timestamp,
            ) {
                Some(_) => {
                    log::info!("sensor collector handler: sensor event collected (update)");
                }
                None => {
                    log::info!("sensor collector handler: sensor event collected (insert)");
                }
            }
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

        let sensor_event_map_entry = {
            let sensor_events = state.sensor_events.lock().unwrap();
            sensor_events.get_cloned(&message.case_id, &self.attribute_key)
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
                        emit_result(
                            &message.case_id,
                            &self.constraint_id,
                            ConstraintType::Sensor,
                            ConstraintResultType::Violated,
                            info,
                            state.clone(),
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
                    emit_result(
                        &message.case_id,
                        &self.constraint_id,
                        ConstraintType::Sensor,
                        ConstraintResultType::Violated,
                        info,
                        state.clone(),
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
            emit_result(
                &message.case_id,
                &self.constraint_id,
                ConstraintType::Sensor,
                ConstraintResultType::Violated,
                format!("{} {} {} is false", attribute_value, self.mode, self.value),
                state.clone(),
            )
            .await;
        };
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
        if !result_exists {
            // produce result because none was emitted yet
            emit_result(
                case_id,
                &self.constraint_id,
                ConstraintType::Sensor,
                ConstraintResultType::Satisfied,
                "condition was met or sensor trigger event never happened".to_string(),
                state.clone(),
            )
            .await;
        }
    }

    async fn clean_up_state(&self, case_id: &CaseId, state: AppState) {
        log::info!(
            "sensor collector handler: clean up state for ({}/{})",
            case_id,
            self.constraint_id
        );

        // remove sensor events
        {
            let mut sensor_events = state.sensor_events.lock().unwrap();
            sensor_events.remove_case(case_id);
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
        types::{ConstraintResultType, ConstraintRole},
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
        CCMessage::trigger_check(case.to_string(), CID.to_string(), ts)
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
