use clap::Parser;
use distributed_online_cc::config::config_builder::NodeConfigBuilder;
use distributed_online_cc::config::node_config::NodeConfig;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::time::Duration;

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

// string config skleton
// loading the config from a string utilizes the default values defined for the deserializer in the node config module
fn app_default_base() -> NodeConfig {
    let skeleton = "\
        node_id = 0\n\
        tcp_listen_address = \"127.0.0.1:9000\"\n\
        constraints = []\n\
        [overlay]\n\
        listen_address = \"127.0.0.1:19000\"\n\
        initial_peers = []\n";
    toml::from_str(skeleton).expect("skeleton must match NodeConfig schema")
}

enum Addressing {
    Local {
        ip: IpAddr,
        overlay_base: u16,
        conformance_base: u16,
    },
    Distributed {
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
            Self::Distributed {
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
            Self::Distributed {
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
            Self::Distributed {
                hosts,
                overlay_port,
                ..
            } => SocketAddr::new(hosts[id as usize], *overlay_port),
        }
    }

    // for local the count is specified as an argument
    // for distributed the count equals the number of hosts specified
    fn node_count(&self, requested: u64) -> u64 {
        match self {
            Self::Distributed { hosts, .. } => hosts.len() as u64,
            _ => requested,
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

// creates the final config and writes it to disk
fn generate(
    n: u64,
    cyclon_params: &CyclonParams,
    gossip_params: &GossipParams,
    addr: &Addressing,
    out_dir: &Path,
    case_end_retention_period_secs: &u64,
) {
    let base = app_default_base();
    std::fs::create_dir_all(out_dir).unwrap();
    for id in 0..n {
        let peers = bootstrap_ids(id, n, cyclon_params.ring_degree)
            .into_iter()
            .map(|p| addr.reachable_overlay(p))
            .collect();

        let cfg = NodeConfigBuilder::from_base(base.clone())
            .node_id(id)
            .tcp_listen(addr.listen_conformance(id))
            .overlay_listen(addr.listen_overlay(id))
            .initial_peers(peers)
            .case_end_retention_period(Duration::from_secs(*case_end_retention_period_secs))
            .unix_socket(format!("/tmp/event_listener_{id}.sock"))
            .log_file(format!("node-{id}.log"))
            .shuffle_interval(cyclon_params.shuffle_interval)
            .shuffle_length(cyclon_params.shuffle_length)
            .max_peer_cache_size(cyclon_params.cache_size)
            .idle_timeout(cyclon_params.idle_timeout)
            .anti_entropy_interval(gossip_params.anti_entropy_interval)
            .anti_entropy_fanout(gossip_params.anti_entropy_fanout)
            .push_fanout(gossip_params.push_fanout)
            .build();

        let path = out_dir.join(format!("node-{id}-config.toml"));
        std::fs::write(&path, toml::to_string(&cfg).unwrap()).unwrap();
    }
}

#[derive(Parser)]
struct Args {
    #[arg(short, long, default_value_t = 5)]
    nodes: u64,
    #[arg(long, default_value = "local")]
    mode: String, // "local" or "distributed"
    #[arg(long, value_delimiter = ',')]
    hosts: Vec<IpAddr>, // distributed: one IP per node
    #[arg(long, default_value_t = 9000)]
    overlay_port: u16,
    #[arg(long, default_value_t = 19000)]
    conformance_port: u16,
    #[arg(long, default_value_t = true)]
    bind_all: bool,
    #[arg(long, default_value = "run/configs")]
    out_dir: std::path::PathBuf,
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
    let args: Args = Args::parse();
    let mut cyclon_params = CyclonParams::default();
    if let Some(ss) = args.shuffle_secs {
        cyclon_params.shuffle_interval = Duration::from_secs(ss);
    }
    if let Some(sl) = args.shuffle_length {
        cyclon_params.shuffle_length = sl;
    }
    if let Some(cs) = args.cache_size {
        cyclon_params.cache_size = cs;
    }
    if let Some(rd) = args.ring_degree {
        cyclon_params.ring_degree = rd;
    }
    if let Some(its) = args.idle_timeout_secs {
        cyclon_params.idle_timeout = Some(Duration::from_secs(its));
    }

    let mut gossip_params = GossipParams::default();
    if let Some(aes) = args.anti_entropy_secs {
        gossip_params.anti_entropy_interval = Duration::from_secs(aes);
    }
    if let Some(aef) = args.anti_entropy_fanout {
        gossip_params.anti_entropy_fanout = aef;
    }
    if let Some(pf) = args.push_fanout {
        gossip_params.push_fanout = pf;
    }

    let addressing = match args.mode.as_str() {
        "local" => Addressing::Local {
            ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            overlay_base: args.overlay_port,
            conformance_base: args.conformance_port,
        },
        "distributed" => Addressing::Distributed {
            hosts: args.hosts,
            overlay_port: args.overlay_port,
            conformance_port: args.conformance_port,
            bind_all: args.bind_all,
        },
        m => panic!("unknown mode: {m}"),
    };
    let n = addressing.node_count(args.nodes); // distributed derives n from host count
    generate(
        n,
        &cyclon_params,
        &gossip_params,
        &addressing,
        &args.out_dir,
        &args.case_end_retention_period_secs,
    );
}
