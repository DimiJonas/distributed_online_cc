//! gen-config — generate node configuration files for both distributed and central system
//!
//! Two orthogonal axes:
//!
//!   * Addressing (`--addressing`)
//!       - `local`: every node on one host, ports offset by node id
//!       - `remote`: one host per node, fixed ports (one IP per node)
//!
//!   * MODE (how conformance is checked)
//!       - distributed: constraints are checked on the nodes that receive the events; nodes form a gossip/overlay ring
//!         node 0 is the dedicated result sink, nodes 1..N have the constraints
//!       - central: forwarders receive events and forward them to a single central node (node 0) that has all constraints
//!         forwarder f mirrors distributed node f: it accepts exactly the event ids placed on that node
//!
//!   * ROLE ASSIGNMENT (how constraint roles are assigned to nodes)
//!       - `round-robin` (default): roles are handed out one at a time, so the roles of one constraint spread out
//!       - `by-event`: every role observing the same event id lands on the same node, so a node monitors exactly
//!         one activity and holds all constraint roles configured for it
//!         by default one constraint node is generated per role (round-robin) or per distinct event id (by-event), plus the result sink
//!         `--nodes` overrides that and the roles wrap around if there are fewer nodes
//!
//! `--catalog` picks the constraint catalog: `default` (built-in test catalog) or `sepsis` (sepsis event log)
//! the sepsis catalog reuses event ids across constraints, so it has to be combined with `--role-assignment by-event`
//!
//! A single invocation always emits both a distributed system and a central system built from the same set of constraints
//! Each system is self contained under its own directory:
//!
//! <out-dir>/
//!   distributed/
//!     configs/   node-<id>-config.toml
//!     logs/      node-<id>.log
//!     cc-results.db
//!   central/
//!     configs/   node-0-central-config.toml, node-<id>-forwarder-config.toml
//!     logs/      central-node.log, forwarder-<id>.log
//!     cc-results.db
//!
//! The db paths written into the configs are relative to the launch cwd, so the nodes must be started from the same directory `gen-config` was run from

use clap::{Parser, ValueEnum};
use online_cc::config::config_builder::NodeConfigBuilder;
use online_cc::config::node_config::{
    AppearanceCounterReset, ConstraintConfig, DistributedAppearanceCounterReset, ForwarderConfig,
    MinMaxMode, ResultBufferConfig, ResultSinkConfig, SensorConstraintMode,
};
use online_cc::types::{ConstraintId, EventId, SensorAttributeKey, SensorAttributeThreshold};
use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;

// ------------------------------
// overlay / gossip tuning knobs
// ------------------------------

#[derive(Clone)]
struct CyclonParams {
    shuffle_interval: Duration,
    shuffle_length: usize,
    cache_size: usize,
    idle_timeout: Option<Duration>,
    ring_degree: u64,
}
impl Default for CyclonParams {
    fn default() -> Self {
        Self {
            shuffle_interval: Duration::from_secs(5),
            shuffle_length: 4,
            cache_size: 20,
            idle_timeout: None,
            ring_degree: 2,
        }
    }
}

#[derive(Clone)]
struct GossipParams {
    anti_entropy_interval: Duration,
    anti_entropy_fanout: usize,
    push_fanout: usize,
}
impl Default for GossipParams {
    fn default() -> Self {
        Self {
            anti_entropy_interval: Duration::from_secs(1),
            anti_entropy_fanout: 1,
            push_fanout: 4,
        }
    }
}

// -------------------------------------------------------------------------------------------------
// addressing: does the system run on one host locally or is it spread among multiple remote hosts
// -------------------------------------------------------------------------------------------------

enum Addressing {
    Local {
        ip: IpAddr,
        overlay_base: u16,
        conformance_base: u16,
    },
    Remote {
        hosts: Vec<IpAddr>,
        overlay_port: u16,
        conformance_port: u16,
        bind_all: bool,
    },
}
impl Addressing {
    // base port + id offset
    fn port(base: u16, id: u64) -> u16 {
        let p = base as u64 + id;
        assert!(p <= u16::MAX as u64, "port {p} overflows u16");
        p as u16
    }
    fn listen_overlay(&self, id: u64) -> SocketAddr {
        match self {
            Self::Local {
                ip, overlay_base, ..
            } => SocketAddr::new(*ip, Self::port(*overlay_base, id)),
            Self::Remote {
                hosts,
                overlay_port,
                bind_all,
                ..
            } => {
                let ip = if *bind_all {
                    IpAddr::V4(Ipv4Addr::UNSPECIFIED)
                } else {
                    hosts[id as usize]
                };
                SocketAddr::new(ip, *overlay_port)
            }
        }
    }
    fn listen_conformance(&self, id: u64) -> SocketAddr {
        match self {
            Self::Local {
                ip,
                conformance_base,
                ..
            } => SocketAddr::new(*ip, Self::port(*conformance_base, id)),
            Self::Remote {
                hosts,
                conformance_port,
                bind_all,
                ..
            } => {
                let ip = if *bind_all {
                    IpAddr::V4(Ipv4Addr::UNSPECIFIED)
                } else {
                    hosts[id as usize]
                };
                SocketAddr::new(ip, *conformance_port)
            }
        }
    }
    // helps build the initial_peers for the overlay
    fn reachable_overlay(&self, id: u64) -> SocketAddr {
        match self {
            Self::Local {
                ip, overlay_base, ..
            } => SocketAddr::new(*ip, Self::port(*overlay_base, id)),
            Self::Remote {
                hosts,
                overlay_port,
                ..
            } => SocketAddr::new(hosts[id as usize], *overlay_port),
        }
    }

    // helps auto-generate the constraint config peers, result sink addresses and central node address
    fn reachable_conformance(&self, id: u64) -> SocketAddr {
        match self {
            Self::Local {
                ip,
                conformance_base,
                ..
            } => SocketAddr::new(*ip, Self::port(*conformance_base, id)),
            Self::Remote {
                hosts,
                conformance_port,
                ..
            } => SocketAddr::new(hosts[id as usize], *conformance_port),
        }
    }

    // how many nodes can be addressed
    // for remote this is the host count
    fn capacity(&self) -> Option<u64> {
        match self {
            Self::Remote { hosts, .. } => Some(hosts.len() as u64),
            Self::Local { .. } => None,
        }
    }
}

/// generates a list of node ids for the initial_peers of the overlay
/// constructs a Chord like ring
fn bootstrap_ids(node_id: u64, modulus: u64, ring_degree: u64) -> Vec<u64> {
    let mut peers = BTreeSet::new();
    let ring_degree = ring_degree.min(modulus.saturating_sub(1)); // avoid u64 underflow when ring degree >= n
    for distance in 1..=ring_degree {
        peers.insert((node_id + distance) % modulus); // successors
        peers.insert((node_id + modulus - distance) % modulus); // predecessors
    }
    let mut jump = 1u64;
    while jump < modulus {
        peers.insert((node_id + jump) % modulus); // chord-style fingers
        jump = jump.saturating_mul(2);
    }
    peers.remove(&node_id);
    peers.into_iter().collect()
}

// --------------------
// constraint catalog
// --------------------
//
// a template is one constraint decomposed into multiple roles
// instantiating a template produces concrete roles that share a `constraint_id` and each carry their own `event_id`
// roles reference sibling roles by their index within the instance (e.g. a trigger references its counter), so peer
// addresses can be filled in after the roles are placed on nodes
//
// to spread more constraints across a system the whole catalog is instantiated `--repeat` times
// copy `c > 0` gets a `_c` suffix on every constraint_id and event_id

// a role within a constraint instance
// `usize` fields are indices of sibling roles within the same instance that are resolved to peer addresses later
#[derive(Clone)]
enum Role {
    AppearanceCounter {
        mode: MinMaxMode,
        frequency: u64,
        reset: AppearanceCounterReset,
    },
    AppearanceTrigger {
        counters: Vec<usize>,
    },
    DistAppearanceCounter {
        reset: DistributedAppearanceCounterReset,
    },
    DistAppearanceTrigger {
        mode: MinMaxMode,
        frequency: u64,
        counters: Vec<usize>,
    },
    NotCoexistenceA {
        b: usize,
    },
    NotCoexistenceB {
        a: usize,
    },
    OrderFirst,
    OrderSecond {
        first: usize,
    },
    PrecedenceConsumer,
    PrecedenceProducer {
        consumers: Vec<usize>,
    },
    ResponseProducer,
    ResponseConsumer {
        producers: Vec<usize>,
    },
    SensorCollector {
        attribute_key: SensorAttributeKey,
        value: SensorAttributeThreshold,
        mode: SensorConstraintMode,
        max_value_age: Duration,
    },
    SensorTrigger {
        collectors: Vec<usize>,
    },
    TimeStart {
        duration: Duration,
        mode: MinMaxMode,
    },
    TimeStop {
        start: usize,
    },
    EndActivity,
}

// one concrete role after instantiation
#[derive(Clone)]
struct RoleInstance {
    kind: Role,
    constraint_id: ConstraintId,
    event_id: EventId,
    buffer: Option<ResultBufferConfig>,
}

// a constraint instance is a group of roles sharing one constraint_id
#[derive(Clone)]
struct Instance {
    roles: Vec<RoleInstance>,
}

// template used to create instances with unique ids
struct Template {
    // e.g. "response_test"
    constraint_stem: &'static str,
    // (role, event_stem, buffer) per role
    roles: Vec<(Role, &'static str, Option<ResultBufferConfig>)>,
}

fn suffix(stem: &str, copy: u64) -> String {
    if copy == 0 {
        stem.to_string()
    } else {
        format!("{stem}_{copy}")
    }
}

// which built-in catalog gets instantiated
#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
enum CatalogKind {
    /// the built-in default catalog, one constraint per constraint type and one distinct event per constraint per role
    Default,
    /// the catalog for the sepsis event log with one node per activity of the log
    Sepsis,
}
impl CatalogKind {
    fn templates(self) -> Vec<Template> {
        match self {
            Self::Default => catalog(),
            Self::Sepsis => catalog_sepsis(),
        }
    }
    fn name(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Sepsis => "sepsis",
        }
    }
}

fn catalog_sepsis() -> Vec<Template> {
    vec![
        Template {
            constraint_stem: "time_triage_antibiotics_1h",
            roles: vec![
                (
                    Role::TimeStart {
                        duration: Duration::from_hours(1),
                        mode: MinMaxMode::Max,
                    },
                    "ER Sepsis Triage",
                    Some(ResultBufferConfig::Revocable {
                        duration: Duration::from_mins(20),
                    }),
                ),
                (Role::TimeStop { start: 0 }, "IV Antibiotics", None),
            ],
        },
        Template {
            constraint_stem: "time_triage_lacticacid_3h",
            roles: vec![
                (
                    Role::TimeStart {
                        duration: Duration::from_hours(3),
                        mode: MinMaxMode::Max,
                    },
                    "ER Sepsis Triage",
                    Some(ResultBufferConfig::Revocable {
                        duration: Duration::from_mins(20),
                    }),
                ),
                (Role::TimeStop { start: 0 }, "LacticAcid", None),
            ],
        },
        Template {
            constraint_stem: "prec_registration_lab",
            roles: vec![
                (Role::PrecedenceConsumer, "LacticAcid", None),
                (Role::PrecedenceConsumer, "Leucocytes", None),
                (Role::PrecedenceConsumer, "CRP", None),
                (
                    Role::PrecedenceProducer {
                        consumers: vec![0, 1, 2],
                    },
                    "ER Registration",
                    None,
                ),
            ],
        },
        Template {
            constraint_stem: "prec_sepsis_triage_antibiotics",
            roles: vec![
                (Role::PrecedenceConsumer, "IV Antibiotics", None),
                (
                    Role::PrecedenceProducer { consumers: vec![0] },
                    "ER Sepsis Triage",
                    None,
                ),
            ],
        },
        Template {
            constraint_stem: "prec_sepsis_triage_ivliquid",
            roles: vec![
                (Role::PrecedenceConsumer, "IV Liquid", None),
                (
                    Role::PrecedenceProducer { consumers: vec![0] },
                    "ER Sepsis Triage",
                    None,
                ),
            ],
        },
        Template {
            constraint_stem: "any_release_before_return",
            roles: vec![
                (
                    Role::DistAppearanceCounter {
                        reset: DistributedAppearanceCounterReset::Zero,
                    },
                    "Release A",
                    None,
                ),
                (
                    Role::DistAppearanceCounter {
                        reset: DistributedAppearanceCounterReset::Zero,
                    },
                    "Release B",
                    None,
                ),
                (
                    Role::DistAppearanceCounter {
                        reset: DistributedAppearanceCounterReset::Zero,
                    },
                    "Release C",
                    None,
                ),
                (
                    Role::DistAppearanceCounter {
                        reset: DistributedAppearanceCounterReset::Zero,
                    },
                    "Release D",
                    None,
                ),
                (
                    Role::DistAppearanceCounter {
                        reset: DistributedAppearanceCounterReset::Zero,
                    },
                    "Release E",
                    None,
                ),
                (
                    Role::DistAppearanceTrigger {
                        mode: MinMaxMode::Min,
                        frequency: 1,
                        counters: vec![0, 1, 2, 3, 4],
                    },
                    "Return ER",
                    None,
                ),
            ],
        },
        Template {
            constraint_stem: "response_registration_triage",
            roles: vec![
                (Role::ResponseProducer, "ER Registration", None),
                (
                    Role::ResponseConsumer { producers: vec![0] },
                    "ER Triage",
                    None,
                ),
            ],
        },
        Template {
            constraint_stem: "order_no_triage_after_sepsis_triage",
            roles: vec![
                (Role::OrderFirst, "ER Triage", None),
                (Role::OrderSecond { first: 0 }, "ER Sepsis Triage", None),
            ],
        },
        Template {
            constraint_stem: "order_no_sepsis_triage_after_admission_NC",
            roles: vec![
                (Role::OrderFirst, "ER Sepsis Triage", None),
                (Role::OrderSecond { first: 0 }, "Admission NC", None),
            ],
        },
        Template {
            constraint_stem: "order_no_sepsis_triage_after_admission_IC",
            roles: vec![
                (Role::OrderFirst, "ER Sepsis Triage", None),
                (Role::OrderSecond { first: 0 }, "Admission IC", None),
            ],
        },
        Template {
            constraint_stem: "notcoex_release_ab",
            roles: vec![
                (Role::NotCoexistenceA { b: 1 }, "Release A", None),
                (Role::NotCoexistenceB { a: 0 }, "Release B", None),
            ],
        },
        Template {
            constraint_stem: "notcoex_release_ac",
            roles: vec![
                (Role::NotCoexistenceA { b: 1 }, "Release A", None),
                (Role::NotCoexistenceB { a: 0 }, "Release C", None),
            ],
        },
        Template {
            constraint_stem: "notcoex_release_ad",
            roles: vec![
                (Role::NotCoexistenceA { b: 1 }, "Release A", None),
                (Role::NotCoexistenceB { a: 0 }, "Release D", None),
            ],
        },
        Template {
            constraint_stem: "notcoex_release_ae",
            roles: vec![
                (Role::NotCoexistenceA { b: 1 }, "Release A", None),
                (Role::NotCoexistenceB { a: 0 }, "Release E", None),
            ],
        },
        Template {
            constraint_stem: "notcoex_release_bc",
            roles: vec![
                (Role::NotCoexistenceA { b: 1 }, "Release B", None),
                (Role::NotCoexistenceB { a: 0 }, "Release C", None),
            ],
        },
        Template {
            constraint_stem: "notcoex_release_bd",
            roles: vec![
                (Role::NotCoexistenceA { b: 1 }, "Release B", None),
                (Role::NotCoexistenceB { a: 0 }, "Release D", None),
            ],
        },
        Template {
            constraint_stem: "notcoex_release_be",
            roles: vec![
                (Role::NotCoexistenceA { b: 1 }, "Release B", None),
                (Role::NotCoexistenceB { a: 0 }, "Release E", None),
            ],
        },
        Template {
            constraint_stem: "notcoex_release_cd",
            roles: vec![
                (Role::NotCoexistenceA { b: 1 }, "Release C", None),
                (Role::NotCoexistenceB { a: 0 }, "Release D", None),
            ],
        },
        Template {
            constraint_stem: "notcoex_release_ce",
            roles: vec![
                (Role::NotCoexistenceA { b: 1 }, "Release C", None),
                (Role::NotCoexistenceB { a: 0 }, "Release E", None),
            ],
        },
        Template {
            constraint_stem: "notcoex_release_de",
            roles: vec![
                (Role::NotCoexistenceA { b: 1 }, "Release D", None),
                (Role::NotCoexistenceB { a: 0 }, "Release E", None),
            ],
        },
        Template {
            constraint_stem: "card_leucocytes_min1",
            roles: vec![
                (
                    Role::AppearanceCounter {
                        mode: MinMaxMode::Min,
                        frequency: 1,
                        reset: AppearanceCounterReset::Zero,
                    },
                    "Leucocytes",
                    None,
                ),
                (
                    Role::AppearanceTrigger { counters: vec![0] },
                    "Case End",
                    None,
                ),
            ],
        },
        Template {
            constraint_stem: "card_leucocytes_max9",
            roles: vec![
                (
                    Role::AppearanceCounter {
                        mode: MinMaxMode::Max,
                        frequency: 9,
                        reset: AppearanceCounterReset::Zero,
                    },
                    "Leucocytes",
                    None,
                ),
                (
                    Role::AppearanceTrigger { counters: vec![0] },
                    "Case End",
                    None,
                ),
            ],
        },
        Template {
            constraint_stem: "card_crp_min1",
            roles: vec![
                (
                    Role::AppearanceCounter {
                        mode: MinMaxMode::Min,
                        frequency: 1,
                        reset: AppearanceCounterReset::Zero,
                    },
                    "CRP",
                    None,
                ),
                (
                    Role::AppearanceTrigger { counters: vec![0] },
                    "Case End",
                    None,
                ),
            ],
        },
        Template {
            constraint_stem: "card_crp_max8",
            roles: vec![
                (
                    Role::AppearanceCounter {
                        mode: MinMaxMode::Max,
                        frequency: 8,
                        reset: AppearanceCounterReset::Zero,
                    },
                    "CRP",
                    None,
                ),
                (
                    Role::AppearanceTrigger { counters: vec![0] },
                    "Case End",
                    None,
                ),
            ],
        },
        Template {
            constraint_stem: "normal_lacticacid_at_release_a",
            roles: vec![
                (
                    Role::SensorCollector {
                        attribute_key: "LacticAcid".to_string(),
                        value: 3.2,
                        mode: SensorConstraintMode::Lte,
                        max_value_age: Duration::from_hours(408),
                    },
                    "LacticAcid",
                    None,
                ),
                (
                    Role::SensorTrigger {
                        collectors: vec![0],
                    },
                    "Release A",
                    None,
                ),
            ],
        },
        Template {
            constraint_stem: "normal_lacticacid_at_release_b",
            roles: vec![
                (
                    Role::SensorCollector {
                        attribute_key: "LacticAcid".to_string(),
                        value: 3.2,
                        mode: SensorConstraintMode::Lte,
                        max_value_age: Duration::from_hours(408),
                    },
                    "LacticAcid",
                    None,
                ),
                (
                    Role::SensorTrigger {
                        collectors: vec![0],
                    },
                    "Release B",
                    None,
                ),
            ],
        },
        Template {
            constraint_stem: "normal_lacticacid_at_release_c",
            roles: vec![
                (
                    Role::SensorCollector {
                        attribute_key: "LacticAcid".to_string(),
                        value: 3.2,
                        mode: SensorConstraintMode::Lte,
                        max_value_age: Duration::from_hours(408),
                    },
                    "LacticAcid",
                    None,
                ),
                (
                    Role::SensorTrigger {
                        collectors: vec![0],
                    },
                    "Release C",
                    None,
                ),
            ],
        },
        Template {
            constraint_stem: "normal_lacticacid_at_release_d",
            roles: vec![
                (
                    Role::SensorCollector {
                        attribute_key: "LacticAcid".to_string(),
                        value: 3.2,
                        mode: SensorConstraintMode::Lte,
                        max_value_age: Duration::from_hours(408),
                    },
                    "LacticAcid",
                    None,
                ),
                (
                    Role::SensorTrigger {
                        collectors: vec![0],
                    },
                    "Release D",
                    None,
                ),
            ],
        },
        Template {
            constraint_stem: "normal_lacticacid_at_release_e",
            roles: vec![
                (
                    Role::SensorCollector {
                        attribute_key: "LacticAcid".to_string(),
                        value: 3.2,
                        mode: SensorConstraintMode::Lte,
                        max_value_age: Duration::from_hours(408),
                    },
                    "LacticAcid",
                    None,
                ),
                (
                    Role::SensorTrigger {
                        collectors: vec![0],
                    },
                    "Release E",
                    None,
                ),
            ],
        },
        Template {
            constraint_stem: "case_end",
            roles: vec![(Role::EndActivity, "Case End", None)],
        },
    ]
}

// default test catalog
fn catalog() -> Vec<Template> {
    vec![
        Template {
            constraint_stem: "appearance_test",
            roles: vec![
                (
                    Role::AppearanceCounter {
                        mode: MinMaxMode::Max,
                        frequency: 3,
                        reset: AppearanceCounterReset::Zero,
                    },
                    "appearance_counter_event",
                    None,
                ),
                (
                    Role::AppearanceTrigger { counters: vec![0] },
                    "appearance_trigger_event",
                    None,
                ),
            ],
        },
        Template {
            constraint_stem: "distributed_appearance_test",
            roles: vec![
                (
                    Role::DistAppearanceCounter {
                        reset: DistributedAppearanceCounterReset::Zero,
                    },
                    "distributed_appearance_counter1_event",
                    None,
                ),
                (
                    Role::DistAppearanceCounter {
                        reset: DistributedAppearanceCounterReset::Zero,
                    },
                    "distributed_appearance_counter2_event",
                    None,
                ),
                (
                    Role::DistAppearanceTrigger {
                        mode: MinMaxMode::Min,
                        frequency: 5,
                        counters: vec![0, 1],
                    },
                    "distributed_appearance_trigger_event",
                    None,
                ),
            ],
        },
        Template {
            constraint_stem: "not_coexistence_test",
            roles: vec![
                (
                    Role::NotCoexistenceA { b: 1 },
                    "not_coexistence_a_event",
                    None,
                ),
                (
                    Role::NotCoexistenceB { a: 0 },
                    "not_coexistence_b_event",
                    None,
                ),
            ],
        },
        Template {
            constraint_stem: "order_test",
            roles: vec![
                (Role::OrderFirst, "order_first_event", None),
                (Role::OrderSecond { first: 0 }, "order_second_event", None),
            ],
        },
        Template {
            constraint_stem: "precedence_test",
            roles: vec![
                (Role::PrecedenceConsumer, "precedence_consumer_event", None),
                (
                    Role::PrecedenceProducer { consumers: vec![0] },
                    "precedence_producer_event",
                    None,
                ),
            ],
        },
        Template {
            constraint_stem: "response_test",
            roles: vec![
                (Role::ResponseProducer, "response_producer_event", None),
                (
                    Role::ResponseConsumer { producers: vec![0] },
                    "response_consumer_event",
                    None,
                ),
            ],
        },
        Template {
            constraint_stem: "sensor_gt_test",
            roles: vec![
                (
                    Role::SensorCollector {
                        attribute_key: "value_gt".to_string(),
                        value: 5.0,
                        mode: SensorConstraintMode::Gt,
                        max_value_age: Duration::from_hours(96),
                    },
                    "sensor_gt_collector_event",
                    None,
                ),
                (
                    Role::SensorTrigger {
                        collectors: vec![0],
                    },
                    "sensor_gt_trigger_event",
                    None,
                ),
            ],
        },
        Template {
            constraint_stem: "sensor_lt_test",
            roles: vec![
                (
                    Role::SensorCollector {
                        attribute_key: "value_lt".to_string(),
                        value: 15.0,
                        mode: SensorConstraintMode::Lt,
                        max_value_age: Duration::from_hours(96),
                    },
                    "sensor_lt_collector_event",
                    None,
                ),
                (
                    Role::SensorTrigger {
                        collectors: vec![0],
                    },
                    "sensor_lt_trigger_event",
                    None,
                ),
            ],
        },
        Template {
            constraint_stem: "sensor_gte_test",
            roles: vec![
                (
                    Role::SensorCollector {
                        attribute_key: "value_gte".to_string(),
                        value: 8.0,
                        mode: SensorConstraintMode::Gte,
                        max_value_age: Duration::from_hours(96),
                    },
                    "sensor_gte_collector_event",
                    None,
                ),
                (
                    Role::SensorTrigger {
                        collectors: vec![0],
                    },
                    "sensor_gte_trigger_event",
                    None,
                ),
            ],
        },
        Template {
            constraint_stem: "time_test",
            roles: vec![
                (
                    Role::TimeStart {
                        duration: Duration::from_secs(10),
                        mode: MinMaxMode::Min,
                    },
                    "time_start_event",
                    Some(ResultBufferConfig::Revocable {
                        duration: Duration::from_secs(10),
                    }),
                ),
                (Role::TimeStop { start: 0 }, "time_stop_event", None),
            ],
        },
        Template {
            constraint_stem: "end_activity",
            roles: vec![(Role::EndActivity, "dummy_end_activity", None)],
        },
    ]
}

// clone a catalog role for copy `copy`
// needed because for sensor collector roles the attribute key must also get a suffix
fn instantiate_role(kind: &Role, copy: u64) -> Role {
    match kind {
        Role::SensorCollector {
            attribute_key,
            value,
            mode,
            max_value_age,
        } => Role::SensorCollector {
            attribute_key: suffix(attribute_key, copy),
            value: *value,
            mode: mode.clone(),
            max_value_age: *max_value_age,
        },
        other => other.clone(),
    }
}

// instantiate the catalog `repeat` times into concrete instances
fn build_instances(repeat: u64, catalog_kind: CatalogKind) -> Vec<Instance> {
    let templates = catalog_kind.templates();
    let mut repeated_catalog = Vec::new();
    for copy in 0..repeat {
        for template in &templates {
            let constraint_id = suffix(template.constraint_stem, copy);
            let roles = template
                .roles
                .iter()
                .map(|(kind, event, buf)| RoleInstance {
                    kind: instantiate_role(kind, copy),
                    constraint_id: constraint_id.clone(),
                    event_id: suffix(event, copy),
                    buffer: buf.clone(),
                })
                .collect();
            repeated_catalog.push(Instance { roles });
        }
    }
    repeated_catalog
}

// turn a role into a concrete ConstraintConfig
// `peer` resolves a sibling role index (within the same instance) to a reachable conformance address
fn to_constraint(
    role: &Role,
    constraint_id: &str,
    event_id: &str,
    peer: &dyn Fn(usize) -> SocketAddr,
) -> ConstraintConfig {
    let constraint_id = constraint_id.to_string();
    let event_id = event_id.to_string();
    match role {
        Role::AppearanceCounter {
            mode,
            frequency,
            reset,
        } => ConstraintConfig::AppearanceCounter {
            constraint_id,
            event_id,
            mode: mode.clone(),
            frequency: *frequency,
            reset: reset.clone(),
        },
        Role::AppearanceTrigger { counters } => ConstraintConfig::AppearanceTrigger {
            constraint_id,
            event_id,
            counter_peers: counters.iter().map(|&i| peer(i)).collect(),
        },
        Role::DistAppearanceCounter { reset } => ConstraintConfig::DistributedAppearanceCounter {
            constraint_id,
            event_id,
            reset: *reset,
        },
        Role::DistAppearanceTrigger {
            mode,
            frequency,
            counters,
        } => ConstraintConfig::DistributedAppearanceTrigger {
            constraint_id,
            event_id,
            mode: mode.clone(),
            frequency: *frequency,
            counter_peers: counters.iter().map(|&i| peer(i)).collect(),
        },
        Role::NotCoexistenceA { b } => ConstraintConfig::NotCoexistenceA {
            constraint_id,
            event_id,
            b_peer: peer(*b),
        },
        Role::NotCoexistenceB { a } => ConstraintConfig::NotCoexistenceB {
            constraint_id,
            event_id,
            a_peer: peer(*a),
        },
        Role::OrderFirst => ConstraintConfig::OrderFirst {
            constraint_id,
            event_id,
        },
        Role::OrderSecond { first } => ConstraintConfig::OrderSecond {
            constraint_id,
            event_id,
            order_first_peers: vec![peer(*first)],
        },
        Role::PrecedenceConsumer => ConstraintConfig::PrecedenceConsumer {
            constraint_id,
            event_id,
        },
        Role::PrecedenceProducer {
            consumers: consumer,
        } => ConstraintConfig::PrecedenceProducer {
            constraint_id,
            event_id,
            consumer_peers: consumer.iter().map(|&i| peer(i)).collect(),
        },
        Role::ResponseProducer => ConstraintConfig::ResponseProducer {
            constraint_id,
            event_id,
        },
        Role::ResponseConsumer {
            producers: producer,
        } => ConstraintConfig::ResponseConsumer {
            constraint_id,
            event_id,
            producer_peers: producer.iter().map(|&i| peer(i)).collect(),
        },
        Role::SensorCollector {
            attribute_key,
            value,
            mode,
            max_value_age,
        } => ConstraintConfig::SensorCollector {
            constraint_id,
            event_id,
            attribute_key: attribute_key.clone(),
            value: *value,
            mode: mode.clone(),
            max_value_age: *max_value_age,
        },
        Role::SensorTrigger {
            collectors: collector,
        } => ConstraintConfig::SensorTrigger {
            constraint_id,
            event_id,
            collector_peers: collector.iter().map(|&i| peer(i)).collect(),
        },
        Role::TimeStart { duration, mode } => ConstraintConfig::TimeStart {
            constraint_id,
            event_id,
            duration: *duration,
            mode: mode.clone(),
        },
        Role::TimeStop { start } => ConstraintConfig::TimeStop {
            constraint_id,
            event_id,
            time_start_peers: vec![peer(*start)],
        },
        Role::EndActivity => ConstraintConfig::EndActivity {
            constraint_id,
            event_id,
        },
    }
}

// -------------------------------------------
// assignment of roles onto distributed nodes
// -------------------------------------------

// how slots are handed out depends on `RoleAssignment`
// slots map to node ids via `dist_node_of` so the forwarders mirror the distributed nodes in which events they accept/expect
type Layout = Vec<Vec<u64>>;

// how constraint roles are assigned to the constraint nodes
#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
enum RoleAssignment {
    /// hand out roles one at a time, round robin over the nodes (default)
    RoundRobin,
    /// give every node one event: all roles observing the same event id land on the same node
    ByEvent,
}
impl RoleAssignment {
    fn name(self) -> &'static str {
        match self {
            Self::RoundRobin => "round-robin",
            Self::ByEvent => "by-event",
        }
    }

    // slots needed so nothing has to share a node: one per role, or one per distinct event
    fn units(self, instances: &[Instance]) -> u64 {
        match self {
            Self::RoundRobin => instances.iter().map(|inst| inst.roles.len() as u64).sum(),
            Self::ByEvent => distinct_events(instances).len() as u64,
        }
    }

    fn unit_name(self) -> &'static str {
        match self {
            Self::RoundRobin => "roles",
            Self::ByEvent => "distinct events",
        }
    }

    fn layout(self, instances: &[Instance], role_node_count: u64) -> Layout {
        assert!(
            role_node_count > 0,
            "need at least one node to host constraints"
        );
        match self {
            Self::RoundRobin => round_robin_layout(instances, role_node_count),
            Self::ByEvent => event_layout(instances, role_node_count),
        }
    }
}

// layout slot -> distributed node id (node 0 is the sink)
fn dist_node_of(slot: u64) -> u64 {
    slot + 1
}

// layout slot -> forwarder id in the central system
// id mapping onto the distributed node ids when forwarders == role_nodes
// wraps around when there are fewer forwarders than constraint nodes
fn mirror_forwarder_of(slot: u64, forwarders: u64) -> u64 {
    (slot % forwarders) + 1
}

// roles are handed out round robin, so consecutive roles of one constraint land on different nodes
// note that this requires a catalog in which every event id belongs to exactly one role, otherwise the event uniqueness check in `main` fails
// use `--role-assignment by-event` for catalogs that reuse event ids
fn round_robin_layout(instances: &[Instance], role_node_count: u64) -> Layout {
    let mut next = 0u64;
    instances
        .iter()
        .map(|inst| {
            inst.roles
                .iter()
                .map(|_| {
                    let node = next % role_node_count;
                    next += 1;
                    node
                })
                .collect()
        })
        .collect()
}

// the distinct event ids of the catalog in first-appearance order
fn distinct_events(instances: &[Instance]) -> Vec<EventId> {
    let mut seen = BTreeSet::new();
    let mut events = Vec::new();
    for instance in instances {
        for role in &instance.roles {
            if seen.insert(role.event_id.clone()) {
                events.push(role.event_id.clone());
            }
        }
    }
    events
}

// event id -> slot, handed out round robin over the constraint nodes
// with `role_node_count` >= number of events every node monitors exactly one activity
// with fewer nodes the events wrap around and a node monitors several activities
fn event_slots(instances: &[Instance], role_node_count: u64) -> Vec<(EventId, u64)> {
    distinct_events(instances)
        .into_iter()
        .enumerate()
        .map(|(index, event)| (event, index as u64 % role_node_count))
        .collect()
}

// place the roles by event: a node receives one event and gets every constraint role configured for that event
fn event_layout(instances: &[Instance], role_node_count: u64) -> Layout {
    let slot_of: BTreeMap<EventId, u64> = event_slots(instances, role_node_count)
        .into_iter()
        .collect();
    instances
        .iter()
        .map(|instance| {
            instance
                .roles
                .iter()
                .map(|role| slot_of[&role.event_id])
                .collect()
        })
        .collect()
}

// ----------------
// config emitters
// ----------------

struct CommonParams {
    cyclon: CyclonParams,
    gossip: GossipParams,
    case_end_retention: Duration,
}

// constraints are placed per `layout`and peers resolve to the conformance address of the hosting node
fn emit_distributed(
    instances: &[Instance],
    layout: &Layout,
    role_nodes: u64,
    addressing: &Addressing,
    db_path: &Path,
    common: &CommonParams,
    out_dir: &Path,
) {
    let sink_id = 0u64;
    let total_nodes = role_nodes + 1; // total nodes incl. sink
    std::fs::create_dir_all(out_dir).unwrap();

    for node_id in 0..total_nodes {
        let peers: Vec<SocketAddr> = bootstrap_ids(node_id, total_nodes, common.cyclon.ring_degree)
            .into_iter()
            .map(|peer| addressing.reachable_overlay(peer))
            .collect();

        let mut builder = NodeConfigBuilder::distributed_base()
            .node_id(node_id)
            .tcp_listen(addressing.listen_conformance(node_id))
            .overlay_listen(addressing.listen_overlay(node_id))
            .initial_peers(peers)
            .case_end_retention_period(common.case_end_retention)
            .default_result_buffer(ResultBufferConfig::Revocable {
                duration: Duration::from_secs(10),
            })
            .unix_socket(format!("/tmp/event_listener_{node_id}.sock"))
            .log_file(format!("node-{node_id}.log"))
            .shuffle_interval(common.cyclon.shuffle_interval)
            .shuffle_length(common.cyclon.shuffle_length)
            .max_peer_cache_size(common.cyclon.cache_size)
            .idle_timeout(common.cyclon.idle_timeout)
            .anti_entropy_interval(common.gossip.anti_entropy_interval)
            .anti_entropy_fanout(common.gossip.anti_entropy_fanout)
            .push_fanout(common.gossip.push_fanout);

        if node_id == sink_id {
            // dedicated result sink: no constraints, owns the database
            builder = builder.result_sink(ResultSinkConfig {
                db_path: db_path.to_string_lossy().into_owned(),
                mpsc_channel_size: 1024,
                retry_interval: Duration::from_secs(15),
                retry_buffer_size: 4096,
            });
        } else {
            // gather the roles placed on this node (layout slots start at 0, node ids start at 1)
            let mut constraints = Vec::new();
            for (instance_index, instance) in instances.iter().enumerate() {
                let role_slots_here = &layout[instance_index];
                for (role_index, role) in instance.roles.iter().enumerate() {
                    if dist_node_of(role_slots_here[role_index]) != node_id {
                        continue;
                    }
                    let resolve = |sib: usize| {
                        addressing.reachable_conformance(dist_node_of(role_slots_here[sib]))
                    };
                    let config =
                        to_constraint(&role.kind, &role.constraint_id, &role.event_id, &resolve);
                    constraints.push((config, role.buffer.clone(), role.constraint_id.clone()));
                }
            }
            builder =
                builder.result_sink_addresses(vec![addressing.reachable_conformance(sink_id)]);
            for (config, result_buffer, constraint_id) in constraints {
                builder = builder.constraint(config);
                if let Some(policy) = result_buffer {
                    builder = builder.result_buffer(constraint_id, policy);
                }
            }
        }

        let config = builder.build();
        let path = out_dir.join(format!("node-{node_id}-config.toml"));
        std::fs::write(&path, toml::to_string(&config).unwrap()).unwrap();
    }
}

// emit the central system: node 0 is the central node holding every constraint
// all remaining nodes are forwarders whose filter lists (attributes, event ids) are derived from the central constraints
fn emit_central(
    instances: &[Instance],
    layout: &Layout,
    forwarders: u64,
    addressing: &Addressing,
    db_path: &Path,
    out_dir: &Path,
) {
    std::fs::create_dir_all(out_dir).unwrap();
    let central_id = 0u64;
    let central_address = addressing.reachable_conformance(central_id);

    // ---- central node ----
    let mut central = NodeConfigBuilder::central_base()
        .node_id(central_id)
        .tcp_listen(addressing.listen_conformance(central_id))
        .unix_socket("/tmp/event_listener_central.sock")
        .log_file("central-node.log")
        .default_result_buffer(ResultBufferConfig::Revocable {
            duration: Duration::from_secs(10),
        })
        .result_sink(ResultSinkConfig {
            db_path: db_path.to_string_lossy().into_owned(),
            mpsc_channel_size: 1024,
            retry_interval: Duration::from_secs(15),
            retry_buffer_size: 4096,
        })
        .result_sink_addresses(vec![central_address]);

    // per-forwarder filter inputs, mirroring the distributed role assignment:
    // forwarder f accepts the events of the roles that the `layout` puts on distributed node f
    // BTreeMap/BTreeSet keep the emitted configs deterministic
    let mut forwarder_event_ids: BTreeMap<u64, BTreeSet<EventId>> = BTreeMap::new();
    let mut forwarder_attributes: BTreeMap<u64, BTreeSet<String>> = BTreeMap::new();
    for forwarder_id in 1..=forwarders {
        forwarder_event_ids.insert(forwarder_id, BTreeSet::new());
        forwarder_attributes.insert(forwarder_id, BTreeSet::new());
    }

    for (instance_index, instance) in instances.iter().enumerate() {
        let resolve = |_sib: usize| central_address; // everything is local to central
        for (role_index, role) in instance.roles.iter().enumerate() {
            let config = to_constraint(&role.kind, &role.constraint_id, &role.event_id, &resolve);
            let forwarder_id = mirror_forwarder_of(layout[instance_index][role_index], forwarders);
            forwarder_event_ids
                .get_mut(&forwarder_id)
                .unwrap()
                .insert(role.event_id.clone());
            if let Role::SensorCollector { attribute_key, .. } = &role.kind {
                forwarder_attributes
                    .get_mut(&forwarder_id)
                    .unwrap()
                    .insert(attribute_key.clone());
            }
            central = central.constraint(config);
            if let Some(policy) = &role.buffer {
                central = central.result_buffer(role.constraint_id.clone(), policy.clone());
            }
        }
    }
    let central = central.build();
    std::fs::write(
        out_dir.join("node-0-central-config.toml"),
        toml::to_string(&central).unwrap(),
    )
    .unwrap();

    // ---- forwarders ----
    // a forwarder that mirrors a constraint-free distributed node gets an empty include list
    for forwarder_id in 1..=forwarders {
        let attributes = &forwarder_attributes[&forwarder_id];
        let include_event_attributes = if attributes.is_empty() {
            None
        } else {
            Some(attributes.iter().cloned().collect())
        };
        let include_event_ids: Vec<EventId> =
            forwarder_event_ids[&forwarder_id].iter().cloned().collect();

        let config = NodeConfigBuilder::central_base()
            .node_id(forwarder_id)
            .tcp_listen(addressing.listen_conformance(forwarder_id))
            .unix_socket(format!("/tmp/event_listener_forwarder{forwarder_id}.sock"))
            .log_file(format!("forwarder-{forwarder_id}.log"))
            .forwarder(ForwarderConfig {
                central_node_address: central_address,
                include_event_attributes,
                include_event_ids: Some(include_event_ids),
            })
            .build();
        std::fs::write(
            out_dir.join(format!("node-{forwarder_id}-forwarder-config.toml")),
            toml::to_string(&config).unwrap(),
        )
        .unwrap();
    }
}

// -----
// CLI
// -----

#[derive(Parser)]
#[command(about = "Generate comparable distributed + central conformance-checker configs")]
struct Args {
    /// total nodes in the distributed system (constraint nodes + 1 sink)
    /// default: one constraint node per role assignment unit + 1 sink
    /// i.e. one node per role for `round-robin` (default catalog: 22 roles -> 23 nodes)
    /// and one node per distinct event id for `by-event` (--catalog sepsis: 17 activities -> 18 nodes)
    #[arg(short, long)]
    nodes: Option<u64>,

    /// how constraint roles are assigned to nodes: `round-robin` one role at a time, or
    /// `by-event` to give each node one event with all the constraint roles configured for it
    #[arg(long, value_enum, default_value_t = RoleAssignment::RoundRobin)]
    role_assignment: RoleAssignment,

    /// which constraint catalog to instantiate: `default` for the built-in test catalog, or
    /// `sepsis` for the sepsis event log catalog (reuses event ids across constraints so it needs `--role-assignment by-event`)
    #[arg(long, value_enum, default_value_t = CatalogKind::Default)]
    catalog: CatalogKind,

    /// print which events and constraint roles each node ends up with
    #[arg(long, action = clap::ArgAction::SetTrue)]
    show_assignment: bool,

    // instantiate the whole constraint catalog this many times (spreads more constraints across the system)
    #[arg(long, default_value_t = 1)]
    repeat: u64,

    // forwarders in the central system
    // default: match the distributed constraint node count so both systems have the same node count and forwarder f mirrors distributed node f
    #[arg(long)]
    forwarders: Option<u64>,

    // node addressing: `local` (one host, port = base+id) or `remote` (one host per node from --hosts).
    #[arg(long, default_value = "local")]
    addressing: String,

    // remote: one ip per node (max(distributed_nodes, central_nodes) entries).
    #[arg(long, value_delimiter = ',')]
    hosts: Vec<IpAddr>,

    #[arg(long, default_value_t = 9000)]
    overlay_port: u16,
    // conformance tcp port base for the distributed system
    #[arg(long, default_value_t = 19000)]
    conformance_port: u16,
    // conformance tcp port base for the central system
    #[arg(long, default_value_t = 18000)]
    central_port: u16,
    // remote: bind each node to its routable host ip instead of 0.0.0.0
    #[arg(long, action = clap::ArgAction::SetTrue)]
    no_bind_all: bool,

    // base output dir for both systems
    #[arg(long, default_value = "run")]
    out_dir: PathBuf,

    // override for the result sqlite dbs (relative to the launch cwd)
    // default: <out-dir>/<mode>/results.db, with the override they become
    // <db-dir>/{distributed,central}-results.db
    #[arg(long)]
    db_dir: Option<PathBuf>,

    #[arg(long, default_value_t = 180)]
    case_end_retention_period_secs: u64,

    #[arg(long)]
    shuffle_secs: Option<u64>,
    #[arg(long)]
    shuffle_length: Option<usize>,
    #[arg(long)]
    cache_size: Option<usize>,
    #[arg(long)]
    ring_degree: Option<u64>,
    #[arg(long)]
    idle_timeout_secs: Option<u64>,
    #[arg(long)]
    anti_entropy_secs: Option<u64>,
    #[arg(long)]
    anti_entropy_fanout: Option<usize>,
    #[arg(long)]
    push_fanout: Option<usize>,
}

fn main() {
    let args = Args::parse();

    let mut cyclon = CyclonParams::default();
    if let Some(v) = args.shuffle_secs {
        cyclon.shuffle_interval = Duration::from_secs(v);
    }
    if let Some(v) = args.shuffle_length {
        cyclon.shuffle_length = v;
    }
    if let Some(v) = args.cache_size {
        cyclon.cache_size = v;
    }
    if let Some(v) = args.ring_degree {
        cyclon.ring_degree = v;
    }
    if let Some(v) = args.idle_timeout_secs {
        cyclon.idle_timeout = Some(Duration::from_secs(v));
    }

    let mut gossip = GossipParams::default();
    if let Some(v) = args.anti_entropy_secs {
        gossip.anti_entropy_interval = Duration::from_secs(v);
    }
    if let Some(v) = args.anti_entropy_fanout {
        gossip.anti_entropy_fanout = v;
    }
    if let Some(v) = args.push_fanout {
        gossip.push_fanout = v;
    }

    let common = CommonParams {
        cyclon,
        gossip,
        case_end_retention: Duration::from_secs(args.case_end_retention_period_secs),
    };

    let make_addressing = |conformance_base: u16| match args.addressing.as_str() {
        "local" => Addressing::Local {
            ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            overlay_base: args.overlay_port,
            conformance_base,
        },
        "remote" => Addressing::Remote {
            hosts: args.hosts.clone(),
            overlay_port: args.overlay_port,
            conformance_port: conformance_base,
            bind_all: !args.no_bind_all,
        },
        unknown => panic!("unknown addressing: {unknown} (expected `local` or `remote`)"),
    };
    let addressing = make_addressing(args.conformance_port);
    let central_addressing = make_addressing(args.central_port);

    // build the constraints first: the default node count follows from the catalog and the role assignment
    let catalog_kind = args.catalog;
    let role_assignment = args.role_assignment;
    let instances = build_instances(args.repeat, catalog_kind);
    let total_roles: usize = instances.iter().map(|instance| instance.roles.len()).sum();
    let units = role_assignment.units(&instances); // roles, or distinct events

    // node counts (default: one constraint node per role assignment unit + the result sink)
    let dist_nodes = match addressing.capacity() {
        Some(cap) => cap, // spread: derive from host count
        None => args.nodes.unwrap_or(units + 1),
    };
    assert!(
        dist_nodes >= 2,
        "need >=2 distributed nodes (>=1 constraint node + 1 sink)"
    );
    let role_nodes = dist_nodes - 1; // node 0 is the sink
    let forwarders = args.forwarders.unwrap_or(dist_nodes - 1);
    assert!(forwarders >= 1, "need at least one forwarder");

    let layout = role_assignment.layout(&instances, role_nodes);

    if role_nodes > units {
        eprintln!(
            "note: {role_nodes} constraint nodes but only {units} {} - {} nodes will have no constraints",
            role_assignment.unit_name(),
            role_nodes - units
        );
    } else if role_nodes < units {
        eprintln!(
            "note: {units} {} but only {role_nodes} constraint nodes - they wrap around, so some nodes \
             get more than one",
            role_assignment.unit_name()
        );
    }
    if forwarders != role_nodes {
        eprintln!(
            "note: {forwarders} forwarders != {role_nodes} constraint nodes - forwarders cannot mirror \
             the distributed nodes 1:1 (distributed node s+1 maps to forwarder (s % {forwarders}) + 1)"
        );
    }

    // check that every event_id is received by exactly one distributed node, so events can be routed by event_id alone and the central mirror is exact
    {
        let mut event_slot: BTreeMap<&str, u64> = BTreeMap::new();
        for (instance_index, instance) in instances.iter().enumerate() {
            for (role_index, role) in instance.roles.iter().enumerate() {
                let slot = layout[instance_index][role_index];
                if let Some(&previous) = event_slot.get(role.event_id.as_str()) {
                    assert_eq!(
                        previous,
                        slot,
                        "event_id '{}' is placed on two different nodes - with `--role-assignment {}` every \
                         event id in the catalog must belong to exactly one role \
                         use `--role-assignment by-event` for catalogs that reuse event ids across constraints",
                        role.event_id,
                        role_assignment.name()
                    );
                } else {
                    event_slot.insert(role.event_id.as_str(), slot);
                }
            }
        }
    }

    // check spread capacity covers the larger of the two systems
    if let Some(cap) = addressing.capacity() {
        let central_nodes = forwarders + 1;
        let need = dist_nodes.max(central_nodes);
        assert!(
            cap >= need,
            "--hosts has {cap} entries but {need} nodes are needed"
        );
    }

    let dist_root = args.out_dir.join("distributed");
    let central_root = args.out_dir.join("central");
    let dist_config_dir = dist_root.join("configs");
    let central_config_dir = central_root.join("configs");

    // create the log dirs up front so the node never has to
    for root in [&dist_root, &central_root] {
        std::fs::create_dir_all(root.join("logs")).unwrap();
    }

    let (dist_db, central_db) = match &args.db_dir {
        Some(dir) => {
            std::fs::create_dir_all(dir).unwrap();
            (
                dir.join("distributed-cc-results.db"),
                dir.join("central-cc-results.db"),
            )
        }
        None => (
            dist_root.join("cc-results.db"),
            central_root.join("cc-results.db"),
        ),
    };

    emit_distributed(
        &instances,
        &layout,
        role_nodes,
        &addressing,
        &dist_db,
        &common,
        &dist_config_dir,
    );
    emit_central(
        &instances,
        &layout,
        forwarders,
        &central_addressing,
        &central_db,
        &central_config_dir,
    );

    println!(
        "generated configs (in {:?}/<system>/configs/:",
        args.out_dir
    );
    println!(
        "  catalog:     {} x{} - {total_roles} roles over {} distinct events, assigned {}",
        catalog_kind.name(),
        args.repeat,
        distinct_events(&instances).len(),
        role_assignment.name()
    );
    println!(
        "  distributed: {dist_nodes} nodes (sink on node 0 + {role_nodes} constraint nodes), {total_roles} constraint roles"
    );
    println!(
        "  central:     {} nodes (central on node 0 + {forwarders} forwarders), {total_roles} constraint roles",
        forwarders + 1
    );

    if args.show_assignment {
        // node -> (accepted events, hosted constraint roles)
        let mut per_node: BTreeMap<u64, (BTreeSet<&str>, Vec<&str>)> = BTreeMap::new();
        for slot in 0..role_nodes {
            per_node.insert(dist_node_of(slot), (BTreeSet::new(), Vec::new()));
        }
        for (instance_index, instance) in instances.iter().enumerate() {
            for (role_index, role) in instance.roles.iter().enumerate() {
                let entry = per_node
                    .get_mut(&dist_node_of(layout[instance_index][role_index]))
                    .unwrap();
                entry.0.insert(role.event_id.as_str());
                entry.1.push(role.constraint_id.as_str());
            }
        }
        println!("  role assignment ({}):", role_assignment.name());
        for (node_id, (events, constraints)) in &per_node {
            let forwarder = mirror_forwarder_of(node_id - 1, forwarders);
            println!(
                "    node {node_id:>3} / forwarder {forwarder:>3}  events [{}]  {} roles [{}]",
                events.iter().copied().collect::<Vec<_>>().join(", "),
                constraints.len(),
                constraints.join(", ")
            );
        }
    }
}
