use crate::config::node_config::{ConstraintConfig, NodeConfig};
use crate::protocol::constraint_logic::constraint_handler_trait::ConstraintHandler;
use crate::protocol::constraint_logic::end_activity_handler::EndActivityHandler;
use crate::protocol::constraint_logic::response_handler::{
    ResponseConsumerHandler, ResponseProducerHandler,
};
use crate::types::ConstraintId;
use std::collections::HashMap;

pub struct HandlerRegistry {
    // basic vector of the configured constraints
    handlers: Vec<Box<dyn ConstraintHandler>>,
    // maps event id -> index for handler vector
    by_event_id: HashMap<String, Vec<usize>>,
    // maps constraint id -> index for handler vector
    by_constraint_id: HashMap<ConstraintId, usize>,
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
            let handler: Box<dyn ConstraintHandler> = match constraint {
                ConstraintConfig::ResponseProducer {
                    constraint_id,
                    event_id,
                } => Box::new(ResponseProducerHandler {
                    constraint_id: constraint_id.clone(),
                    event_id: event_id.clone(),
                }),
                ConstraintConfig::ResponseConsumer {
                    constraint_id,
                    event_id,
                    producer_peers,
                } => Box::new(ResponseConsumerHandler {
                    constraint_id: constraint_id.clone(),
                    event_id: event_id.clone(),
                    producer_peers: producer_peers.clone(),
                }),
                ConstraintConfig::EndActivity {
                    constraint_id,
                    event_id,
                } => Box::new(EndActivityHandler {
                    constraint_id: constraint_id.clone(),
                    event_id: event_id.clone(),
                }),
            };
            registry.register(handler);
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
    ) -> Option<&dyn ConstraintHandler> {
        self.by_constraint_id
            .get(constraint_id)
            .map(|&i| self.handlers[i].as_ref())
    }

    pub fn all(&self) -> impl Iterator<Item = &dyn ConstraintHandler> {
        self.handlers.iter().map(|h| h.as_ref())
    }

    pub fn register(&mut self, handler: Box<dyn ConstraintHandler>) {
        let idx = self.handlers.len();

        self.by_event_id
            .entry(handler.event_id().to_string())
            .or_default()
            .push(idx);

        self.by_constraint_id
            .insert(handler.constraint_id().to_string(), idx);

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
    use crate::config::node_config::{ConstraintConfig, NodeConfig};

    fn make_config(constraints: Vec<ConstraintConfig>) -> NodeConfig {
        NodeConfig {
            node_id: 1,
            listen_address: "127.0.0.1:9000".parse().unwrap(),
            unix_socket: "/tmp/test.sock".to_string(),
            peers: vec![],
            event_queue_size: 100,
            tcp_channel_queue_size: 100,
            constraints,
        }
    }

    #[test]
    fn empty_registry_returns_nothing() {
        let registry = HandlerRegistry::new();
        assert!(registry.get_by_event_id("any").is_empty());
    }

    #[test]
    fn build_registers_response_producer() {
        let config = make_config(vec![ConstraintConfig::ResponseProducer {
            constraint_id: "rp".to_string(),
            event_id: "order_placed".to_string(),
        }]);
        let registry = HandlerRegistry::build(&config);

        let handlers = registry.get_by_event_id("order_placed");
        assert_eq!(handlers.len(), 1);
        assert_eq!(handlers[0].constraint_id(), "rp");
    }

    #[test]
    fn get_by_constraint_id_finds_handler() {
        let config = make_config(vec![ConstraintConfig::EndActivity {
            constraint_id: "ea".to_string(),
            event_id: "case_end".to_string(),
        }]);
        let registry = HandlerRegistry::build(&config);

        assert!(registry.get_by_constraint_id(&"ea".to_string()).is_some());
    }

    #[test]
    fn get_by_constraint_id_returns_none_for_unknown() {
        let registry = HandlerRegistry::new();
        assert!(
            registry
                .get_by_constraint_id(&"ghost".to_string())
                .is_none()
        );
    }

    #[test]
    fn multiple_constraints_for_same_event_id() {
        let config = make_config(vec![
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
        let config = make_config(vec![
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
