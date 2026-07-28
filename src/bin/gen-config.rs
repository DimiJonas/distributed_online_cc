//! gen-config — generate node configuration files for both distributed and central system
//!
//! Two orthogonal axes:
//!
//!   * Addressing (`--addressing`)
//!       - `local`  : every node on one host, ports offset by node id
//!       - `remote` : one host per node, fixed ports (one IP per node)
//!
//!   * MODE (how conformance is checked)
//!       - distributed : constraints are checked on the nodes that receive the events; nodes form a gossip/overlay ring
//!       - central     : forwarders receive events and forward them to a single central node that holds all constraints
//!
//! A single invocation always emits both a distributed system and a central system built from the same set of constraints
//! Each system is self contained under its own directory:
//!
//! <out-dir>/
//!   distributed/
//!     configs/   node-<id>-config.toml
//!     logs/      node-<id>.log
//!     results.db
//!   central/
//!     configs/   node-0-central-config.toml, node-<id>-forwarder-config.toml
//!     logs/      central-node.log, forwarder-<id>.log
//!     results.db
//!
//! The db paths written into the configs are relative to the launch cwd, so the nodes must be started from the same directory `gen-config` was run from

use clap::Parser;
use online_cc::config::config_builder::NodeConfigBuilder;
use online_cc::config::node_config::{
    AppearanceCounterReset, ConstraintConfig, DistributedAppearanceCounterReset, ForwarderConfig,
    MinMaxMode, ResultBufferConfig, ResultSinkConfig, SensorConstraintMode,
};
use online_cc::types::{ConstraintId, EventId, SensorAttributeKey, SensorAttributeThreshold};
use std::collections::HashSet;
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
            anti_entropy_interval: Duration::from_secs(4),
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
    use std::collections::BTreeSet;
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
        consumer: usize,
    },
    ResponseProducer,
    ResponseConsumer {
        producer: usize,
    },
    SensorCollector {
        attribute_key: SensorAttributeKey,
        value: SensorAttributeThreshold,
        mode: SensorConstraintMode,
        max_value_age: Duration,
    },
    SensorTrigger {
        collector: usize,
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

// the built-in constraint catalog - one entry per logical constraint
fn catalog() -> Vec<Template> {
    // small function to create sensor templates
    let sensor = |stem: &'static str,
                  threshold: SensorAttributeThreshold,
                  op: SensorConstraintMode| Template {
        constraint_stem: stem,
        roles: vec![
            (
                Role::SensorCollector {
                    attribute_key: "value".to_string(),
                    value: threshold,
                    mode: op,
                    max_value_age: Duration::from_hours(96),
                },
                "sensor_collector_event",
                None,
            ),
            (
                Role::SensorTrigger { collector: 0 },
                "sensor_trigger_event",
                None,
            ),
        ],
    };

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
                    Role::PrecedenceProducer { consumer: 0 },
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
                    Role::ResponseConsumer { producer: 0 },
                    "response_consumer_event",
                    None,
                ),
            ],
        },
        sensor("sensor_lt_test", 15.0, SensorConstraintMode::Lt),
        sensor("sensor_gt_test", 5.0, SensorConstraintMode::Gt),
        sensor("sensor_lte_test", 12.0, SensorConstraintMode::Lte),
        sensor("sensor_gte_test", 8.0, SensorConstraintMode::Gte),
        sensor("sensor_eq_test", 10.0, SensorConstraintMode::Eq),
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

// instantiate the catalog `repeat` times into concrete instances
fn build_instances(repeat: u64) -> Vec<Instance> {
    let templates = catalog();
    let mut repeated_catalog = Vec::new();
    for copy in 0..repeat {
        for template in &templates {
            let constraint_id = suffix(template.constraint_stem, copy);
            let roles = template
                .roles
                .iter()
                .map(|(kind, event, buf)| RoleInstance {
                    kind: kind.clone(),
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
        Role::PrecedenceProducer { consumer } => ConstraintConfig::PrecedenceProducer {
            constraint_id,
            event_id,
            consumer_peers: vec![peer(*consumer)],
        },
        Role::ResponseProducer => ConstraintConfig::ResponseProducer {
            constraint_id,
            event_id,
        },
        Role::ResponseConsumer { producer } => ConstraintConfig::ResponseConsumer {
            constraint_id,
            event_id,
            producer_peers: vec![peer(*producer)],
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
        Role::SensorTrigger { collector } => ConstraintConfig::SensorTrigger {
            constraint_id,
            event_id,
            collector_peers: vec![peer(*collector)],
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
// placement of roles onto distributed nodes
// -------------------------------------------

// for each instance, the node id that hosts each of its roles
// in distributed mode roles are round-robined across the non-sink nodes
type Layout = Vec<Vec<u64>>;

fn spread_layout(instances: &[Instance], role_node_count: u64) -> Layout {
    assert!(
        role_node_count > 0,
        "need at least one node to host constraints"
    );
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

// ----------------
// config emitters
// ----------------

struct CommonParams {
    cyclon: CyclonParams,
    gossip: GossipParams,
    case_end_retention: Duration,
}

// emit the distributed system: `role_nodes` constraint-carrying nodes plus one dedicated result-sink node (the last id)
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
    let sink_id = role_nodes; // sink is the extra node after the role nodes
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
            .default_result_buffer(ResultBufferConfig::GracePeriod {
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
            // gather the roles placed on this node
            let mut constraints = Vec::new();
            for (instance_index, instance) in instances.iter().enumerate() {
                let role_nodes_here = &layout[instance_index];
                for (role_index, role) in instance.roles.iter().enumerate() {
                    if role_nodes_here[role_index] != node_id {
                        continue;
                    }
                    let resolve =
                        |sib: usize| addressing.reachable_conformance(role_nodes_here[sib]);
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
        .default_result_buffer(ResultBufferConfig::GracePeriod {
            duration: Duration::from_secs(10),
        })
        .result_sink(ResultSinkConfig {
            db_path: db_path.to_string_lossy().into_owned(),
            mpsc_channel_size: 1024,
            retry_interval: Duration::from_secs(15),
            retry_buffer_size: 4096,
        })
        .result_sink_addresses(vec![central_address]);

    // collected filter inputs for the forwarders
    let mut event_ids: HashSet<EventId> = HashSet::new();
    let mut attributes: HashSet<String> = HashSet::new();

    for instance in instances {
        let resolve = |_sib: usize| central_address; // everything is local to central
        for role in &instance.roles {
            let config = to_constraint(&role.kind, &role.constraint_id, &role.event_id, &resolve);
            event_ids.insert(role.event_id.clone());
            if let Role::SensorCollector { attribute_key, .. } = &role.kind {
                attributes.insert(attribute_key.clone());
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
    let include_event_attributes = if attributes.is_empty() {
        None
    } else {
        Some(attributes.into_iter().collect())
    };
    let include_event_ids: Vec<String> = event_ids.into_iter().collect();

    for forwarder_id in 1..=forwarders {
        let config = NodeConfigBuilder::central_base()
            .node_id(forwarder_id)
            .tcp_listen(addressing.listen_conformance(forwarder_id))
            .unix_socket(format!("/tmp/event_listener_forwarder{forwarder_id}.sock"))
            .log_file(format!("forwarder-{forwarder_id}.log"))
            .forwarder(ForwarderConfig {
                central_node_address: central_address,
                include_event_attributes: include_event_attributes.clone(),
                include_event_ids: Some(include_event_ids.clone()),
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
    #[arg(short, long, default_value_t = 20)]
    nodes: u64,

    // instantiate the whole constraint catalog this many times (spreads more constraints across the system)
    #[arg(long, default_value_t = 1)]
    repeat: u64,

    // forwarders in the central system
    // default: match the distributed node count (nodes - 1) so both systems have the same number of nodes
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

    // node counts
    let dist_nodes = match addressing.capacity() {
        Some(cap) => cap, // spread: derive from host count
        None => args.nodes,
    };
    assert!(
        dist_nodes >= 2,
        "need >=2 distributed nodes (>=1 constraint node + 1 sink)"
    );
    let role_nodes = dist_nodes - 1; // last node is the sink
    let forwarders = args.forwarders.unwrap_or(dist_nodes - 1);
    assert!(forwarders >= 1, "need at least one forwarder");

    // spread constraints
    let instances = build_instances(args.repeat);
    let total_roles: usize = instances.iter().map(|instance| instance.roles.len()).sum();
    let layout = spread_layout(&instances, role_nodes);

    if (role_nodes as usize) > total_roles {
        eprintln!(
            "note: {role_nodes} constraint nodes but only {total_roles} roles - {} nodes will have no constraints",
            role_nodes as usize - total_roles
        );
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
        "  distributed: {dist_nodes} nodes ({role_nodes} constraint + 1 sink), {total_roles} constraints"
    );
    println!(
        "  central:     {} nodes (1 central + {forwarders} forwarders), {total_roles} constraints",
        forwarders + 1
    );
}
