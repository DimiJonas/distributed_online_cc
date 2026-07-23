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
//! A single invocation always emits BOTH a distributed system and a central
//! system built from the same set of constraints, into
//! `<out-dir>/distributed` and `<out-dir>/central`placement

use clap::Parser;
use online_cc::config::config_builder::NodeConfigBuilder;
use online_cc::config::node_config::{
    AppearanceCounterReset, ConstraintConfig, DistributedAppearanceCounterReset, ForwarderConfig,
    MinMaxMode, ResultBufferConfig, ResultSinkConfig, SensorConstraintMode,
};
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
fn bootstrap_ids(id: u64, n: u64, ring_degree: u64) -> Vec<u64> {
    use std::collections::BTreeSet;
    let mut peers = BTreeSet::new();
    let rd = ring_degree.min(n.saturating_sub(1)); // avoid u64 underflow when rd >= n
    for k in 1..=rd {
        peers.insert((id + k) % n); // successors
        peers.insert((id + n - k) % n); // predecessors
    }
    let mut jump = 1u64;
    while jump < n {
        peers.insert((id + jump) % n); // chord-style fingers
        jump = jump.saturating_mul(2);
    }
    peers.remove(&id);
    peers.into_iter().collect()
}

// --------------------
// constraint catalog
// --------------------
//
// A template is one constraint decomposed into multiple roles
// Instantiating a template produces concrete roles that share a `constraint_id`
// and each carry their own `event_id`. Roles reference sibling roles by their
// index within the instance (e.g. a trigger references its counter), so peer
// addresses can be filled in after the roles are placed on nodes.
//
// To spread more constraints across a system we simply instantiate the whole
// catalog `--repeat` times; copy `c > 0` gets a `_c` suffix on every
// constraint_id and event_id

/// A role within a constraint instance. `usize` fields are indices of sibling
/// roles within the same instance that are resolved to peer addresses later
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
        attribute_key: String,
        value: f64,
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
struct RoleInst {
    kind: Role,
    constraint_id: String,
    event_id: String,
    buffer: Option<ResultBufferConfig>,
}

// a constraint instance is a group of roles sharing one constraint_id
#[derive(Clone)]
struct Instance {
    roles: Vec<RoleInst>,
}

// template used to create instances with unique ids
struct Template {
    // e.g. "response_test"
    constraint_stem: &'static str,
    // (role, event_stem, buffer) per role
    roles: Vec<(Role, &'static str, Option<ResultBufferConfig>)>,
}

fn sfx(stem: &str, copy: u64) -> String {
    if copy == 0 {
        stem.to_string()
    } else {
        format!("{stem}_{copy}")
    }
}

// the built-in constraint catalog - one entry per logical constraint
fn catalog() -> Vec<Template> {
    // small function to create sensor templates
    let sensor = |stem: &'static str, attr: &'static str, op: SensorConstraintMode| Template {
        constraint_stem: stem,
        roles: vec![
            (
                Role::SensorCollector {
                    attribute_key: attr.to_string(),
                    value: 10.0,
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
        sensor("sensor_lt_test", "lt_value", SensorConstraintMode::Lt),
        sensor("sensor_gt_test", "gt_value", SensorConstraintMode::Gt),
        sensor("sensor_lte_test", "lte_value", SensorConstraintMode::Lte),
        sensor("sensor_gte_test", "gte_value", SensorConstraintMode::Gte),
        sensor("sensor_eq_test", "eq_value", SensorConstraintMode::Eq),
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
    let mut out = Vec::new();
    for copy in 0..repeat {
        for t in &templates {
            let cid = sfx(t.constraint_stem, copy);
            let roles = t
                .roles
                .iter()
                .map(|(kind, ev, buf)| RoleInst {
                    kind: kind.clone(),
                    constraint_id: cid.clone(),
                    event_id: sfx(ev, copy),
                    buffer: buf.clone(),
                })
                .collect();
            out.push(Instance { roles });
        }
    }
    out
}

// turn a role into a concrete ConstraintConfig
// `peer` resolves a sibling role index (within the same instance) to a reachable conformance address
fn to_constraint(
    role: &Role,
    constraint_id: &str,
    event_id: &str,
    peer: &dyn Fn(usize) -> SocketAddr,
) -> ConstraintConfig {
    let id = constraint_id.to_string();
    let ev = event_id.to_string();
    match role {
        Role::AppearanceCounter {
            mode,
            frequency,
            reset,
        } => ConstraintConfig::AppearanceCounter {
            constraint_id: id,
            event_id: ev,
            mode: mode.clone(),
            frequency: *frequency,
            reset: reset.clone(),
        },
        Role::AppearanceTrigger { counters } => ConstraintConfig::AppearanceTrigger {
            constraint_id: id,
            event_id: ev,
            counter_peers: counters.iter().map(|&i| peer(i)).collect(),
        },
        Role::DistAppearanceCounter { reset } => ConstraintConfig::DistributedAppearanceCounter {
            constraint_id: id,
            event_id: ev,
            reset: *reset,
        },
        Role::DistAppearanceTrigger {
            mode,
            frequency,
            counters,
        } => ConstraintConfig::DistributedAppearanceTrigger {
            constraint_id: id,
            event_id: ev,
            mode: mode.clone(),
            frequency: *frequency,
            counter_peers: counters.iter().map(|&i| peer(i)).collect(),
        },
        Role::NotCoexistenceA { b } => ConstraintConfig::NotCoexistenceA {
            constraint_id: id,
            event_id: ev,
            b_peer: peer(*b),
        },
        Role::NotCoexistenceB { a } => ConstraintConfig::NotCoexistenceB {
            constraint_id: id,
            event_id: ev,
            a_peer: peer(*a),
        },
        Role::OrderFirst => ConstraintConfig::OrderFirst {
            constraint_id: id,
            event_id: ev,
        },
        Role::OrderSecond { first } => ConstraintConfig::OrderSecond {
            constraint_id: id,
            event_id: ev,
            order_first_peers: vec![peer(*first)],
        },
        Role::PrecedenceConsumer => ConstraintConfig::PrecedenceConsumer {
            constraint_id: id,
            event_id: ev,
        },
        Role::PrecedenceProducer { consumer } => ConstraintConfig::PrecedenceProducer {
            constraint_id: id,
            event_id: ev,
            consumer_peers: vec![peer(*consumer)],
        },
        Role::ResponseProducer => ConstraintConfig::ResponseProducer {
            constraint_id: id,
            event_id: ev,
        },
        Role::ResponseConsumer { producer } => ConstraintConfig::ResponseConsumer {
            constraint_id: id,
            event_id: ev,
            producer_peers: vec![peer(*producer)],
        },
        Role::SensorCollector {
            attribute_key,
            value,
            mode,
            max_value_age,
        } => ConstraintConfig::SensorCollector {
            constraint_id: id,
            event_id: ev,
            attribute_key: attribute_key.clone(),
            value: *value,
            mode: mode.clone(),
            max_value_age: *max_value_age,
        },
        Role::SensorTrigger { collector } => ConstraintConfig::SensorTrigger {
            constraint_id: id,
            event_id: ev,
            collector_peers: vec![peer(*collector)],
        },
        Role::TimeStart { duration, mode } => ConstraintConfig::TimeStart {
            constraint_id: id,
            event_id: ev,
            duration: *duration,
            mode: mode.clone(),
        },
        Role::TimeStop { start } => ConstraintConfig::TimeStop {
            constraint_id: id,
            event_id: ev,
            time_start_peers: vec![peer(*start)],
        },
        Role::EndActivity => ConstraintConfig::EndActivity {
            constraint_id: id,
            event_id: ev,
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
    db_path: &str,
    common: &CommonParams,
    out_dir: &Path,
) {
    let sink_id = role_nodes; // sink is the extra node after the role nodes
    let n = role_nodes + 1; // total nodes incl. sink
    std::fs::create_dir_all(out_dir).unwrap();

    for id in 0..n {
        let peers: Vec<SocketAddr> = bootstrap_ids(id, n, common.cyclon.ring_degree)
            .into_iter()
            .map(|p| addressing.reachable_overlay(p))
            .collect();

        let mut b = NodeConfigBuilder::distributed_base()
            .node_id(id)
            .tcp_listen(addressing.listen_conformance(id))
            .overlay_listen(addressing.listen_overlay(id))
            .initial_peers(peers)
            .case_end_retention_period(common.case_end_retention)
            .default_result_buffer(ResultBufferConfig::GracePeriod {
                duration: Duration::from_secs(10),
            })
            .unix_socket(format!("/tmp/event_listener_{id}.sock"))
            .log_file(format!("node-{id}.log"))
            .shuffle_interval(common.cyclon.shuffle_interval)
            .shuffle_length(common.cyclon.shuffle_length)
            .max_peer_cache_size(common.cyclon.cache_size)
            .idle_timeout(common.cyclon.idle_timeout)
            .anti_entropy_interval(common.gossip.anti_entropy_interval)
            .anti_entropy_fanout(common.gossip.anti_entropy_fanout)
            .push_fanout(common.gossip.push_fanout);

        if id == sink_id {
            // dedicated result sink: no constraints, owns the database
            b = b.result_sink(ResultSinkConfig {
                db_path: db_path.to_string(),
                mpsc_channel_size: 1024,
                retry_interval: Duration::from_secs(15),
                retry_buffer_size: 4096,
            });
        } else {
            // gather the roles placed on this node
            let mut constraints = Vec::new();
            for (inst_idx, inst) in instances.iter().enumerate() {
                let role_nodes_here = &layout[inst_idx];
                for (role_idx, role) in inst.roles.iter().enumerate() {
                    if role_nodes_here[role_idx] != id {
                        continue;
                    }
                    let resolve =
                        |sib: usize| addressing.reachable_conformance(role_nodes_here[sib]);
                    let cfg =
                        to_constraint(&role.kind, &role.constraint_id, &role.event_id, &resolve);
                    constraints.push((cfg, role.buffer.clone(), role.constraint_id.clone()));
                }
            }
            b = b.result_sink_addresses(vec![addressing.reachable_conformance(sink_id)]);
            for (cfg, buf, cid) in constraints {
                b = b.constraint(cfg);
                if let Some(policy) = buf {
                    b = b.result_buffer(cid, policy);
                }
            }
        }

        let cfg = b.build();
        let path = out_dir.join(format!("node-{id}-config.toml"));
        std::fs::write(&path, toml::to_string(&cfg).unwrap()).unwrap();
    }
}

// emit the central system: node 0 is the central node holding every constraint
// all remaining nodes are forwarders whose filter lists (attributes, event ids) are derived from the central constraints
fn emit_central(
    instances: &[Instance],
    forwarders: u64,
    addressing: &Addressing,
    db_path: &str,
    out_dir: &Path,
) {
    std::fs::create_dir_all(out_dir).unwrap();
    let central_id = 0u64;
    let central_addr = addressing.reachable_conformance(central_id);

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
            db_path: db_path.to_string(),
            mpsc_channel_size: 1024,
            retry_interval: Duration::from_secs(15),
            retry_buffer_size: 4096,
        })
        .result_sink_addresses(vec![central_addr]);

    // collected filter inputs for the forwarders
    let mut event_ids: HashSet<String> = HashSet::new();
    let mut attributes: HashSet<String> = HashSet::new();

    for inst in instances {
        let resolve = |_sib: usize| central_addr; // everything is local to central
        for role in &inst.roles {
            let cfg = to_constraint(&role.kind, &role.constraint_id, &role.event_id, &resolve);
            event_ids.insert(role.event_id.clone());
            if let Role::SensorCollector { attribute_key, .. } = &role.kind {
                attributes.insert(attribute_key.clone());
            }
            central = central.constraint(cfg);
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

    for f in 1..=forwarders {
        let cfg = NodeConfigBuilder::central_base()
            .node_id(f)
            .tcp_listen(addressing.listen_conformance(f))
            .unix_socket(format!("/tmp/event_listener_forwarder{f}.sock"))
            .log_file(format!("forwarder-{f}.log"))
            .forwarder(ForwarderConfig {
                central_node_address: central_addr,
                include_event_attributes: include_event_attributes.clone(),
                include_event_ids: Some(include_event_ids.clone()),
            })
            .build();
        std::fs::write(
            out_dir.join(format!("node-{f}-forwarder-config.toml")),
            toml::to_string(&cfg).unwrap(),
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
    #[arg(short, long, default_value_t = 19)]
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

    // base output dir
    #[arg(long, default_value = "run")]
    out_dir: PathBuf,

    // directory (relative to the launch cwd) for the result sqlite dbs
    #[arg(long, default_value = "run")]
    db_dir: String,

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

    let make_placement = |conformance_base: u16| match args.addressing.as_str() {
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
        m => panic!("unknown placement: {m} (expected `local` or `remote`)"),
    };
    let placement = make_placement(args.conformance_port);
    let central_placement = make_placement(args.central_port);

    // node counts
    let dist_nodes = match placement.capacity() {
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
    let total_roles: usize = instances.iter().map(|i| i.roles.len()).sum();
    let layout = spread_layout(&instances, role_nodes);

    if (role_nodes as usize) > total_roles {
        eprintln!(
            "note: {role_nodes} constraint nodes but only {total_roles} roles - {} nodes will have no constraints",
            role_nodes as usize - total_roles
        );
    }

    // check spread capacity covers the larger of the two systems
    if let Some(cap) = placement.capacity() {
        let central_nodes = forwarders + 1;
        let need = dist_nodes.max(central_nodes);
        assert!(
            cap >= need,
            "--hosts has {cap} entries but {need} nodes are needed"
        );
    }

    let dist_dir = args.out_dir.join("distributed");
    let central_dir = args.out_dir.join("central");
    let dist_db = format!(
        "{}/distributed-results.db",
        args.db_dir.trim_end_matches('/')
    );
    let central_db = format!("{}/central-results.db", args.db_dir.trim_end_matches('/'));

    emit_distributed(
        &instances, &layout, role_nodes, &placement, &dist_db, &common, &dist_dir,
    );
    emit_central(
        &instances,
        forwarders,
        &central_placement,
        &central_db,
        &central_dir,
    );

    println!(
        "generated:\n  distributed: {} nodes ({} constraint + 1 sink), {} constraints -> {}\n  central:     {} nodes (1 central + {} forwarders), {} constraints -> {}",
        dist_nodes,
        role_nodes,
        total_roles,
        dist_dir.display(),
        forwarders + 1,
        forwarders,
        total_roles,
        central_dir.display(),
    );
}
