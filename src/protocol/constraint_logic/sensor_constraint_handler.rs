use std::{net::SocketAddr, time::Duration};

use async_trait::async_trait;
use chrono::Utc;
use process_mining::core::event_data::case_centric::AttributeValue;

use crate::{
    config::node_config::SensorConstraintMode,
    impl_constraint_handler_fields,
    network::{
        message_types::{CCMessage, CCMessageType, PeerMessage},
        tcp_router::send_to_peer,
    },
    protocol::{
        constraint_logic::constraint_handler_trait::ConstraintHandler, result_emitter::emit_result,
    },
    state::app_state::AppState,
    types::{
        CaseId, ConstraintHandlerId, ConstraintId, ConstraintResultType, ConstraintType, EventId,
        TracedEvent,
    },
};

pub struct SensorCollectorHandler {
    pub handler_id: ConstraintHandlerId,
    pub constraint_id: ConstraintId,
    pub event_id: EventId,
    pub attribute_key: String,
    pub value: f64,
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

        let sensor_event_time = {
            if let Some(timestamp) = event.timestamp_utc() {
                timestamp
            } else {
                log::warn!(
                    "sensor collector handler: sensor collector event did not contain a timestamp - using current time as event time"
                );
                Utc::now()
            }
        };

        let attribute_value = match event.get_attribute_by_key(&self.attribute_key) {
            Some(value) => match value {
                AttributeValue::Float(f) => f,
                AttributeValue::Int(i) => i as f64,
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
                // TODO: maybe emit a violated result
                log::error!(
                    "sensor constraint handler: event {} does not contain a value for attribute {}",
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
                attribute_value,
                sensor_event_time,
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

    async fn handle_message(&self, state: AppState, message: CCMessage) {
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
                    if value_age.to_std().unwrap_or(std::time::Duration::MAX) > self.max_value_age {
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
        let (info, constraint_result) = if self.mode.evaluate(attribute_value, self.value) {
            // produce positive result
            (
                format!("{} {} {} is true", attribute_value, self.mode, self.value),
                ConstraintResultType::Satisfied,
            )
        } else {
            // produce negative result
            (
                format!("{} {} {} is false", attribute_value, self.mode, self.value),
                ConstraintResultType::Violated,
            )
        };

        emit_result(
            &message.case_id,
            &self.constraint_id,
            ConstraintType::Sensor,
            constraint_result,
            info,
            state.clone(),
        )
        .await;
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
                "sensor trigger event never happened".to_string(),
                state.clone(),
            )
            .await;
        }
    }

    async fn clean_up_state(&self, case_id: &CaseId, _state: AppState) {
        log::info!(
            "sensor collector handler: clean up state for ({}/{})",
            case_id,
            self.constraint_id
        );
    }
}

pub struct SensorTriggerHandler {
    pub handler_id: ConstraintHandlerId,
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

        let trigger_event_time = {
            if let Some(timestamp) = event.timestamp_utc() {
                timestamp
            } else {
                log::warn!(
                    "sensor trigger handler: sensor trigger event did not contain a timestamp - using current time as event time"
                );
                Utc::now()
            }
        };

        log::info!(
            "sensor trigger handler: triggering sensor evaluation on colector peers for case={} constraint={}",
            event.case_id,
            self.constraint_id
        );
        let message = PeerMessage::conformance(CCMessage::trigger_check(
            event.case_id.clone(),
            self.constraint_id.clone(),
            trigger_event_time,
        ));
        for &peer_address in self.collector_peers.iter() {
            if let Err(e) = send_to_peer(peer_address, message.clone(), state.clone()).await {
                log::error!(
                    "sensor trigger handler: error sending cc trigger message to {}: {}",
                    peer_address,
                    e
                );
            }
        }
    }

    async fn clean_up_state(&self, case_id: &CaseId, _state: AppState) {
        log::info!(
            "sensor trigger handler: clean up state for ({}/{})",
            case_id,
            self.constraint_id
        );
    }
}
