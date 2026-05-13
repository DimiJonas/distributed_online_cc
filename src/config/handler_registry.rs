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
