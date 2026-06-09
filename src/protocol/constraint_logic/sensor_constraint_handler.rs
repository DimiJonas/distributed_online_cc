use async_trait::async_trait;
use process_mining::core::event_data::case_centric::AttributeValue;

use crate::{
    impl_constraint_handler_fields,
    protocol::{
        constraint_logic::constraint_handler_trait::ConstraintHandler, result_emitter::emit_result,
    },
    state::app_state::AppState,
    types::{
        ConstraintHandlerId, ConstraintId, ConstraintResultType, EventId, SensorConstraintMode,
        TracedEvent,
    },
};

pub struct SensorConstraintHandler {
    pub handler_id: ConstraintHandlerId,
    pub constraint_id: ConstraintId,
    pub event_id: EventId,
    pub attribute_key: String,
    pub value: f64,
    pub mode: SensorConstraintMode,
}

#[async_trait]
impl ConstraintHandler for SensorConstraintHandler {
    impl_constraint_handler_fields!(SensorHandler);

    async fn handle_event(&self, event: &TracedEvent, state: AppState) {
        log::info!(
            "received configured sensor event for constraint {}; event={}",
            self.constraint_id,
            event
        );

        let attribute_value = match event.get_attribute_by_key(&self.attribute_key) {
            Some(value) => match value {
                AttributeValue::Float(f) => f,
                AttributeValue::Int(i) => i as f64,
                _ => {
                    log::error!(
                        "cannot compare attribute {} with value {}, because it is not a numeric value",
                        self.attribute_key,
                        value
                    );
                    return;
                }
            },
            None => {
                log::error!(
                    "event {} does not contain a value for attribute {}",
                    event,
                    self.attribute_key
                );
                return;
            }
        };

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
            &event.case_id,
            &self.constraint_id,
            constraint_result,
            info,
            state.clone(),
        )
        .await;
    }
}
