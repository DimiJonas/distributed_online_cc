//! the constraint set of a deployment, reassembled from its node configs
//!
//! a constraint is split across nodes by role (producer here, consumer there),
//! so neither a single config nor a single node knows the whole constraint. this
//! module reads a directory of `*-config.toml` files and pairs the roles back up
//! into one catalog.
//!
//! this used to live inside the `gen-events` binary. it is shared now because
//! `expected-results` needs exactly the same view of the model: if the two
//! disagreed about what the constraint set is, the ground truth would silently
//! stop matching the deployment under test.

use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::Path;

use crate::config::node_config::{
    AppearanceCounterReset, ConstraintConfig, DistributedAppearanceCounterReset, MinMaxMode,
    NodeConfig, SensorConstraintMode,
};
use crate::types::{ConstraintId, EventId, SensorAttributeKey, SensorAttributeThreshold};

/// a constraint whose two halves are two different events in a fixed order
#[derive(Debug, Clone)]
pub struct PairSpec {
    pub constraint_id: ConstraintId,
    /// the event that must come first (e.g. producer)
    pub event_id_1: EventId,
    /// the event that must come second (e.g. consumer)
    pub event_id_2: EventId,
}

#[derive(Debug, Clone)]
pub struct AppearanceSpec {
    pub constraint_id: ConstraintId,
    pub counter_event_id: EventId,
    pub trigger_event_id: EventId,
    pub mode: MinMaxMode,
    pub frequency: u64,
    pub reset: AppearanceCounterReset,
}

#[derive(Debug, Clone)]
pub struct DistAppearanceSpec {
    pub constraint_id: ConstraintId,
    pub counter_event_ids: Vec<EventId>,
    pub trigger_event_id: EventId,
    pub mode: MinMaxMode,
    pub frequency: u64,
    pub reset: DistributedAppearanceCounterReset,
}

#[derive(Debug, Clone)]
pub struct SensorSpec {
    pub constraint_id: ConstraintId,
    pub collector_event_id: EventId,
    pub trigger_event_id: EventId,
    pub attribute_key: SensorAttributeKey,
    pub mode: SensorConstraintMode,
    pub threshold: SensorAttributeThreshold,
    pub max_value_age_secs: f64,
}

#[derive(Debug, Clone)]
pub struct TimeSpec {
    pub constraint_id: ConstraintId,
    pub start_event_id: EventId,
    pub stop_event_id: EventId,
    pub duration_secs: f64,
    pub mode: MinMaxMode,
}

#[derive(Default, Debug, Clone)]
pub struct Catalog {
    pub order: Vec<PairSpec>,
    pub response: Vec<PairSpec>,
    pub precedence: Vec<PairSpec>,
    pub notcoex: Vec<PairSpec>,
    pub appearance: Vec<AppearanceSpec>,
    pub dist_appearance: Vec<DistAppearanceSpec>,
    pub sensor: Vec<SensorSpec>,
    pub time: Vec<TimeSpec>,
    pub end_event_id: Option<EventId>,
}

impl Catalog {
    /// all constraint ids that emit results (end activity does not)
    pub fn result_constraint_ids(&self) -> Vec<ConstraintId> {
        let mut constraint_ids = Vec::new();
        for p in self
            .order
            .iter()
            .chain(&self.response)
            .chain(&self.precedence)
            .chain(&self.notcoex)
        {
            constraint_ids.push(p.constraint_id.clone());
        }
        constraint_ids.extend(self.appearance.iter().map(|a| a.constraint_id.clone()));
        constraint_ids.extend(self.dist_appearance.iter().map(|a| a.constraint_id.clone()));
        constraint_ids.extend(self.sensor.iter().map(|s| s.constraint_id.clone()));
        constraint_ids.extend(self.time.iter().map(|t| t.constraint_id.clone()));
        constraint_ids.sort();
        constraint_ids.dedup();
        constraint_ids
    }

    /// every event id that takes part in some constraint (plus the end activity)
    pub fn all_event_ids(&self) -> Vec<EventId> {
        let mut event_ids = Vec::new();
        for p in self
            .order
            .iter()
            .chain(&self.response)
            .chain(&self.precedence)
            .chain(&self.notcoex)
        {
            event_ids.push(p.event_id_1.clone());
            event_ids.push(p.event_id_2.clone());
        }
        for a in &self.appearance {
            event_ids.push(a.counter_event_id.clone());
            event_ids.push(a.trigger_event_id.clone());
        }
        for a in &self.dist_appearance {
            event_ids.extend(a.counter_event_ids.iter().cloned());
            event_ids.push(a.trigger_event_id.clone());
        }
        for s in &self.sensor {
            event_ids.push(s.collector_event_id.clone());
            event_ids.push(s.trigger_event_id.clone());
        }
        for t in &self.time {
            event_ids.push(t.start_event_id.clone());
            event_ids.push(t.stop_event_id.clone());
        }
        if let Some(end) = &self.end_event_id {
            event_ids.push(end.clone());
        }
        event_ids.sort();
        event_ids.dedup();
        event_ids
    }

    pub fn constraint_count(&self) -> usize {
        self.result_constraint_ids().len()
    }
}

/// read every `*.toml` in `dir` as a node config
pub fn load_configs(dir: &Path) -> Vec<NodeConfig> {
    let mut configs = Vec::new();
    let mut paths: Vec<_> = fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", dir.display()))
        .map(|e| e.unwrap().path())
        .collect();
    paths.sort();
    for p in paths {
        if p.extension().and_then(|e| e.to_str()) == Some("toml") {
            let text = fs::read_to_string(&p).unwrap();
            let cfg: NodeConfig =
                toml::from_str(&text).unwrap_or_else(|e| panic!("parse {}: {e}", p.display()));
            configs.push(cfg);
        }
    }
    configs
}

// pair up the roles of each constraint (spread across node configs) into the catalog
// BTreeMaps keep the iteration order deterministic
pub fn build_catalog(configs: &[NodeConfig]) -> Catalog {
    let mut catalog = Catalog::default();

    // per-constraint role collection
    let mut halves: BTreeMap<ConstraintId, BTreeMap<&'static str, EventId>> = BTreeMap::new();
    let mut half = |constraint_id: &str, role: &'static str, event_id: &str| {
        halves
            .entry(constraint_id.to_string())
            .or_default()
            .insert(role, event_id.to_string());
    };
    #[allow(clippy::type_complexity)]
    let mut appearance: BTreeMap<
        ConstraintId,
        (
            Option<(EventId, MinMaxMode, u64, AppearanceCounterReset)>,
            Option<EventId>,
        ),
    > = BTreeMap::new();
    #[allow(clippy::type_complexity)]
    let mut dist_appearance: BTreeMap<
        ConstraintId,
        (
            Vec<EventId>,
            Option<(EventId, MinMaxMode, u64)>,
            Option<DistributedAppearanceCounterReset>,
        ),
    > = BTreeMap::new();
    #[allow(clippy::type_complexity)]
    let mut sensor_collectors: BTreeMap<
        ConstraintId,
        (
            EventId,
            SensorAttributeKey,
            SensorConstraintMode,
            SensorAttributeThreshold,
            f64,
        ),
    > = BTreeMap::new();
    let mut sensor_triggers: BTreeMap<ConstraintId, EventId> = BTreeMap::new();
    #[allow(clippy::type_complexity)]
    let mut times: BTreeMap<
        ConstraintId,
        (Option<(EventId, f64, MinMaxMode)>, Option<EventId>),
    > = BTreeMap::new();

    let mut seen: HashSet<String> = HashSet::new(); // dedupe identical role entries across configs
    for config in configs {
        for constraint_config in &config.constraints {
            let key = format!(
                "{}#{}#{:?}",
                constraint_config.constraint_id(),
                constraint_config.event_id(),
                std::mem::discriminant(constraint_config)
            );
            if !seen.insert(key) {
                continue;
            }
            match constraint_config {
                ConstraintConfig::OrderFirst {
                    constraint_id,
                    event_id,
                } => half(constraint_id, "first", event_id),
                ConstraintConfig::OrderSecond {
                    constraint_id,
                    event_id,
                    ..
                } => half(constraint_id, "second", event_id),
                ConstraintConfig::ResponseProducer {
                    constraint_id,
                    event_id,
                } => half(constraint_id, "rproducer", event_id),
                ConstraintConfig::ResponseConsumer {
                    constraint_id,
                    event_id,
                    ..
                } => half(constraint_id, "rconsumer", event_id),
                ConstraintConfig::PrecedenceProducer {
                    constraint_id,
                    event_id,
                    ..
                } => half(constraint_id, "pproducer", event_id),
                ConstraintConfig::PrecedenceConsumer {
                    constraint_id,
                    event_id,
                } => half(constraint_id, "pconsumer", event_id),
                ConstraintConfig::NotCoexistenceA {
                    constraint_id,
                    event_id,
                    ..
                } => half(constraint_id, "a", event_id),
                ConstraintConfig::NotCoexistenceB {
                    constraint_id,
                    event_id,
                    ..
                } => half(constraint_id, "b", event_id),
                ConstraintConfig::AppearanceCounter {
                    constraint_id,
                    event_id,
                    mode,
                    frequency,
                    reset,
                } => {
                    appearance.entry(constraint_id.clone()).or_default().0 =
                        Some((event_id.clone(), mode.clone(), *frequency, reset.clone()));
                }
                ConstraintConfig::AppearanceTrigger {
                    constraint_id,
                    event_id,
                    ..
                } => {
                    appearance.entry(constraint_id.clone()).or_default().1 = Some(event_id.clone());
                }
                ConstraintConfig::DistributedAppearanceCounter {
                    constraint_id,
                    event_id,
                    reset,
                } => {
                    let entry = dist_appearance.entry(constraint_id.clone()).or_default();
                    if !entry.0.contains(event_id) {
                        entry.0.push(event_id.clone());
                    }
                    entry.2 = Some(*reset);
                }
                ConstraintConfig::DistributedAppearanceTrigger {
                    constraint_id,
                    event_id,
                    mode,
                    frequency,
                    ..
                } => {
                    dist_appearance.entry(constraint_id.clone()).or_default().1 =
                        Some((event_id.clone(), mode.clone(), *frequency));
                }
                ConstraintConfig::SensorCollector {
                    constraint_id,
                    event_id,
                    attribute_key,
                    value,
                    mode,
                    max_value_age,
                } => {
                    sensor_collectors.insert(
                        constraint_id.clone(),
                        (
                            event_id.clone(),
                            attribute_key.clone(),
                            mode.clone(),
                            *value,
                            max_value_age.as_secs_f64(),
                        ),
                    );
                }
                ConstraintConfig::SensorTrigger {
                    constraint_id,
                    event_id,
                    ..
                } => {
                    sensor_triggers.insert(constraint_id.clone(), event_id.clone());
                }
                ConstraintConfig::TimeStart {
                    constraint_id,
                    event_id,
                    duration,
                    mode,
                } => {
                    times.entry(constraint_id.clone()).or_default().0 =
                        Some((event_id.clone(), duration.as_secs_f64(), mode.clone()));
                }
                ConstraintConfig::TimeStop {
                    constraint_id,
                    event_id,
                    ..
                } => {
                    times.entry(constraint_id.clone()).or_default().1 = Some(event_id.clone());
                }
                ConstraintConfig::EndActivity { event_id, .. } => {
                    catalog.end_event_id = Some(event_id.clone());
                }
            }
        }
    }

    let pair = |m: &BTreeMap<&'static str, String>,
                k1: &str,
                k2: &str|
     -> Option<(String, String)> { Some((m.get(k1)?.clone(), m.get(k2)?.clone())) };
    for (constraint_id, roles) in &halves {
        if let Some((event_id_1, event_id_2)) = pair(roles, "first", "second") {
            catalog.order.push(PairSpec {
                constraint_id: constraint_id.clone(),
                event_id_1,
                event_id_2,
            });
        }
        if let Some((event_id_1, event_id_2)) = pair(roles, "rproducer", "rconsumer") {
            catalog.response.push(PairSpec {
                constraint_id: constraint_id.clone(),
                event_id_1,
                event_id_2,
            });
        }
        if let Some((event_id_1, event_id_2)) = pair(roles, "pproducer", "pconsumer") {
            catalog.precedence.push(PairSpec {
                constraint_id: constraint_id.clone(),
                event_id_1,
                event_id_2,
            });
        }
        if let Some((event_id_1, event_id_2)) = pair(roles, "a", "b") {
            catalog.notcoex.push(PairSpec {
                constraint_id: constraint_id.clone(),
                event_id_1,
                event_id_2,
            });
        }
    }
    for (constraint_id, (counter, trigger)) in appearance {
        if let (Some((counter_event_id, mode, frequency, reset)), Some(trigger_event_id)) =
            (counter, trigger)
        {
            catalog.appearance.push(AppearanceSpec {
                constraint_id,
                counter_event_id,
                trigger_event_id,
                mode,
                frequency,
                reset,
            });
        }
    }
    for (constraint_id, (counter_event_ids, trigger, reset)) in dist_appearance {
        if let Some((trigger_event_id, mode, frequency)) = trigger {
            catalog.dist_appearance.push(DistAppearanceSpec {
                constraint_id,
                counter_event_ids,
                trigger_event_id,
                mode,
                frequency,
                reset: reset.unwrap_or(DistributedAppearanceCounterReset::Keep),
            });
        }
    }
    for (constraint_id, (collector_event_id, attribute_key, mode, threshold, max_value_age_secs)) in
        sensor_collectors
    {
        let trigger_event_id = sensor_triggers
            .get(&constraint_id)
            .cloned()
            .unwrap_or_else(|| panic!("sensor constraint {} has no trigger role", constraint_id));
        catalog.sensor.push(SensorSpec {
            constraint_id,
            collector_event_id,
            trigger_event_id,
            attribute_key,
            mode,
            threshold,
            max_value_age_secs,
        });
    }
    for (constraint_id, (start, stop)) in times {
        if let (Some((start_event_id, duration_secs, mode)), Some(stop_event_id)) = (start, stop) {
            catalog.time.push(TimeSpec {
                constraint_id,
                start_event_id,
                stop_event_id,
                duration_secs,
                mode,
            });
        }
    }
    catalog
}
