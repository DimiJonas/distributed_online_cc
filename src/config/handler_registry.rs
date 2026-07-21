use crate::config::node_config::{ConstraintConfig, NodeConfig};
use crate::conformance_checking::constraint_logic::appearance_constraint_handler::{
    AppearanceCounterHandler, AppearanceTriggerHandler,
};
use crate::conformance_checking::constraint_logic::constraint_handler_trait::ConstraintHandler;
use crate::conformance_checking::constraint_logic::distributed_appearance_constraint_handler::{
    DistributedAppearanceCounterHandler, DistributedAppearanceTriggerHandler,
};
use crate::conformance_checking::constraint_logic::end_activity_handler::EndActivityHandler;
use crate::conformance_checking::constraint_logic::not_coexistence_handler::{
    NotCoexistenceAHandler, NotCoexistenceBHandler,
};
use crate::conformance_checking::constraint_logic::order_constraint_handler::{
    OrderFirstHandler, OrderSecondHandler,
};
use crate::conformance_checking::constraint_logic::precedence_constraint_handler::{
    PrecedenceConsumerHandler, PrecedenceProducerHandler,
};
use crate::conformance_checking::constraint_logic::response_constraint_handler::{
    ResponseConsumerHandler, ResponseProducerHandler,
};
use crate::conformance_checking::constraint_logic::sensor_constraint_handler::{
    SensorCollectorHandler, SensorTriggerHandler,
};
use crate::conformance_checking::constraint_logic::time_constraint_handler::{
    TimeStartHandler, TimeStopHandler,
};
use crate::types::{ConstraintId, ConstraintRole, EventId};
use std::collections::HashMap;

pub struct HandlerRegistry {
    // basic vector of the configured constraints
    handlers: Vec<Box<dyn ConstraintHandler>>,
    // maps event id -> index for handler vector
    by_event_id: HashMap<EventId, Vec<usize>>,
    // maps constraint id -> index for handler vector
    by_constraint_id: HashMap<ConstraintId, Vec<usize>>,
}

impl HandlerRegistry {
    pub fn new() -> Self {
        Self {
            handlers: Vec::new(),
            by_event_id: HashMap::new(),
            by_constraint_id: HashMap::new(),
        }
    }

    // build handler registry from configuration
    pub fn build(config: &NodeConfig) -> Self {
        let mut registry = HandlerRegistry::new();

        for constraint in &config.constraints {
            let index = registry.handlers.len();
            let handler: Box<dyn ConstraintHandler> = match constraint {
                ConstraintConfig::ResponseProducer {
                    constraint_id,
                    event_id,
                } => Box::new(ResponseProducerHandler {
                    index,
                    role: ConstraintRole::ResponseProducer,
                    constraint_id: constraint_id.clone(),
                    event_id: event_id.clone(),
                }),
                ConstraintConfig::ResponseConsumer {
                    constraint_id,
                    event_id,
                    producer_peers,
                } => Box::new(ResponseConsumerHandler {
                    index,
                    role: ConstraintRole::ResponseConsumer,
                    constraint_id: constraint_id.clone(),
                    event_id: event_id.clone(),
                    producer_peers: producer_peers.clone(),
                }),
                ConstraintConfig::EndActivity {
                    constraint_id,
                    event_id,
                } => Box::new(EndActivityHandler {
                    index,
                    role: ConstraintRole::EndActivity,
                    constraint_id: constraint_id.clone(),
                    event_id: event_id.clone(),
                }),
                ConstraintConfig::PrecedenceProducer {
                    constraint_id,
                    event_id,
                    consumer_peers,
                } => Box::new(PrecedenceProducerHandler {
                    index,
                    role: ConstraintRole::PrecedenceProducer,
                    constraint_id: constraint_id.clone(),
                    event_id: event_id.clone(),
                    consumer_peers: consumer_peers.clone(),
                }),
                ConstraintConfig::PrecedenceConsumer {
                    constraint_id,
                    event_id,
                } => Box::new(PrecedenceConsumerHandler {
                    index,
                    role: ConstraintRole::PrecedenceConsumer,
                    constraint_id: constraint_id.clone(),
                    event_id: event_id.clone(),
                }),
                ConstraintConfig::OrderFirst {
                    constraint_id,
                    event_id,
                } => Box::new(OrderFirstHandler {
                    index,
                    role: ConstraintRole::OrderFirst,
                    constraint_id: constraint_id.clone(),
                    event_id: event_id.clone(),
                }),
                ConstraintConfig::OrderSecond {
                    constraint_id,
                    event_id,
                    order_first_peers,
                } => Box::new(OrderSecondHandler {
                    index,
                    role: ConstraintRole::OrderSecond,
                    constraint_id: constraint_id.clone(),
                    event_id: event_id.clone(),
                    order_first_peers: order_first_peers.clone(),
                }),
                ConstraintConfig::SensorCollector {
                    constraint_id,
                    event_id,
                    attribute_key,
                    value,
                    mode,
                    max_value_age,
                } => Box::new(SensorCollectorHandler {
                    index,
                    role: ConstraintRole::SensorCollector,
                    constraint_id: constraint_id.clone(),
                    event_id: event_id.clone(),
                    attribute_key: attribute_key.clone(),
                    value: *value,
                    mode: mode.clone(),
                    max_value_age: *max_value_age,
                }),
                ConstraintConfig::SensorTrigger {
                    constraint_id,
                    event_id,
                    collector_peers,
                } => Box::new(SensorTriggerHandler {
                    index,
                    role: ConstraintRole::SensorTrigger,
                    constraint_id: constraint_id.clone(),
                    event_id: event_id.clone(),
                    collector_peers: collector_peers.clone(),
                }),
                ConstraintConfig::TimeStart {
                    constraint_id,
                    event_id,
                    duration,
                    mode,
                } => Box::new(TimeStartHandler {
                    index,
                    role: ConstraintRole::TimeStart,
                    constraint_id: constraint_id.clone(),
                    event_id: event_id.clone(),
                    duration: *duration,
                    mode: mode.clone(),
                }),
                ConstraintConfig::TimeStop {
                    constraint_id,
                    event_id,
                    time_start_peers,
                } => Box::new(TimeStopHandler {
                    index,
                    role: ConstraintRole::TimeStop,
                    constraint_id: constraint_id.clone(),
                    event_id: event_id.clone(),
                    time_start_peers: time_start_peers.clone(),
                }),
                ConstraintConfig::NotCoexistenceA {
                    constraint_id,
                    event_id,
                    b_peer,
                } => Box::new(NotCoexistenceAHandler {
                    index,
                    role: ConstraintRole::NotCoexistenceA,
                    constraint_id: constraint_id.clone(),
                    event_id: event_id.clone(),
                    b_peer: *b_peer,
                }),
                ConstraintConfig::NotCoexistenceB {
                    constraint_id,
                    event_id,
                    a_peer,
                } => Box::new(NotCoexistenceBHandler {
                    index,
                    role: ConstraintRole::NotCoexistenceB,
                    constraint_id: constraint_id.clone(),
                    event_id: event_id.clone(),
                    a_peer: *a_peer,
                }),
                ConstraintConfig::AppearanceCounter {
                    constraint_id,
                    event_id,
                    mode,
                    frequency,
                    reset,
                } => Box::new(AppearanceCounterHandler {
                    index,
                    role: ConstraintRole::AppearanceCounter,
                    constraint_id: constraint_id.clone(),
                    event_id: event_id.clone(),
                    mode: mode.clone(),
                    frequency: *frequency,
                    reset: reset.clone(),
                }),
                ConstraintConfig::AppearanceTrigger {
                    constraint_id,
                    event_id,
                    counter_peers,
                } => Box::new(AppearanceTriggerHandler {
                    index,
                    role: ConstraintRole::AppearanceTrigger,
                    constraint_id: constraint_id.clone(),
                    event_id: event_id.clone(),
                    counter_peers: counter_peers.clone(),
                }),
                ConstraintConfig::DistributedAppearanceCounter {
                    constraint_id,
                    event_id,
                    reset,
                } => Box::new(DistributedAppearanceCounterHandler {
                    index,
                    role: ConstraintRole::DistributedAppearanceCounter,
                    constraint_id: constraint_id.clone(),
                    event_id: event_id.clone(),
                    reset: *reset,
                }),
                ConstraintConfig::DistributedAppearanceTrigger {
                    constraint_id,
                    event_id,
                    mode,
                    frequency,
                    counter_peers,
                } => Box::new(DistributedAppearanceTriggerHandler {
                    index,
                    role: ConstraintRole::DistributedAppearanceTrigger,
                    constraint_id: constraint_id.clone(),
                    event_id: event_id.clone(),
                    counter_peers: counter_peers.clone(),
                    mode: mode.clone(),
                    frequency: *frequency,
                }),
            };
            registry.register(handler, index);
        }

        registry
    }

    pub fn get_by_event_id(&self, event_id: &str) -> Vec<&dyn ConstraintHandler> {
        self.by_event_id
            .get(event_id)
            .map(|indices| indices.iter().map(|&i| self.handlers[i].as_ref()).collect())
            .unwrap_or_default()
    }

    pub fn get_by_constraint_id(
        &self,
        constraint_id: &ConstraintId,
    ) -> Vec<&dyn ConstraintHandler> {
        self.by_constraint_id
            .get(constraint_id)
            .map(|indices| indices.iter().map(|&i| self.handlers[i].as_ref()).collect())
            .unwrap_or_default()
    }

    pub fn all(&self) -> impl Iterator<Item = &dyn ConstraintHandler> {
        self.handlers.iter().map(|h| h.as_ref())
    }

    pub fn register(&mut self, handler: Box<dyn ConstraintHandler>, index: usize) {
        self.by_event_id
            .entry(handler.event_id().to_string())
            .or_default()
            .push(index);

        self.by_constraint_id
            .entry(handler.constraint_id().to_string())
            .or_default()
            .push(index);

        self.handlers.push(handler);
    }
}

impl Default for HandlerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::node_config::ConstraintConfig;
    use crate::util::test_tools::make_config_with_constraints;

    impl HandlerRegistry {
        // returns the first index by constraint id
        pub fn get_handler_index_by_constraint_id(
            &self,
            constraint_id: &ConstraintId,
        ) -> Option<usize> {
            self.by_constraint_id.get(constraint_id)?.first().copied()
        }
    }

    #[test]
    fn empty_registry_returns_nothing() {
        let registry = HandlerRegistry::new();
        assert!(registry.get_by_event_id("any").is_empty());
    }

    #[test]
    fn build_registers_response_producer() {
        let config = make_config_with_constraints(vec![ConstraintConfig::ResponseProducer {
            constraint_id: "rp".to_string(),
            event_id: "order_placed".to_string(),
        }]);
        let registry = HandlerRegistry::build(&config);

        let handlers = registry.get_by_event_id("order_placed");
        assert_eq!(handlers.len(), 1);
        assert_eq!(handlers[0].constraint_id(), "rp");
    }

    // #[test]
    // fn get_by_constraint_id_finds_handler() {
    //     let config = make_config_with_constraints(vec![ConstraintConfig::EndActivity {
    //         constraint_id: "ea".to_string(),
    //         event_id: "case_end".to_string(),
    //     }]);
    //     let registry = HandlerRegistry::build(&config);

    //     assert!(registry.get_by_constraint_id(&"ea".to_string()).is_some());
    // }

    // #[test]
    // fn get_by_constraint_id_returns_none_for_unknown() {
    //     let registry = HandlerRegistry::new();
    //     assert!(
    //         registry
    //             .get_by_constraint_id(&"ghost".to_string())
    //             .is_none()
    //     );
    // }

    #[test]
    fn multiple_constraints_for_same_event_id() {
        let config = make_config_with_constraints(vec![
            ConstraintConfig::ResponseProducer {
                constraint_id: "rp-1".to_string(),
                event_id: "shared_event".to_string(),
            },
            ConstraintConfig::EndActivity {
                constraint_id: "ea-1".to_string(),
                event_id: "shared_event".to_string(),
            },
        ]);
        let registry = HandlerRegistry::build(&config);

        assert_eq!(registry.get_by_event_id("shared_event").len(), 2);
    }

    #[test]
    fn all_returns_all_handlers() {
        let config = make_config_with_constraints(vec![
            ConstraintConfig::ResponseProducer {
                constraint_id: "rp".to_string(),
                event_id: "e1".to_string(),
            },
            ConstraintConfig::EndActivity {
                constraint_id: "ea".to_string(),
                event_id: "e2".to_string(),
            },
        ]);
        let registry = HandlerRegistry::build(&config);
        assert_eq!(registry.all().count(), 2);
    }
}
