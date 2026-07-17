use clap::Parser;
use distributed_online_cc::config::node_config::NodeConfig;
use distributed_online_cc::network::tcp_router::connect_peer;
use distributed_online_cc::state::app_state::AppState;
use distributed_online_cc::types::PeerAddress;
use distributed_online_cc::util::logging::setup_logging;
use std::net::SocketAddr;
use tokio::sync::mpsc;

// parse command line arguments
#[derive(Parser)]
struct Input {
    #[arg(short, long, default_value = "config.toml")]
    pub config_path: String,
    #[arg(short, long)]
    pub log_dir: Option<String>,
}

#[tokio::main]
async fn main() {
    let args = Input::parse();
    let config: NodeConfig = NodeConfig::load_config(&args.config_path);

    setup_logging(&args.log_dir, &config).expect("failed to initialize logging");
    log::info!("starting node");
    log::debug!("configuration: {:?}", config);

    // create event queue
    let (event_q_tx, _event_q_rx) = mpsc::channel(config.event_queue_size);
    // create loopback message queue - it decouples tcp router from tcp channel for messages that are meant for the own host that skip the tcp communication
    let (loopback_q_tx, loopback_q_rx) = mpsc::channel(config.loopback_queue_size);
    // create result sync channel - decouples the asynchronous system from the synchronous/blocking database connection
    let sink_channel_size = {
        match config.result_sink.as_ref() {
            Some(sink_config) => sink_config.mpsc_channel_size,
            None => 16,
        }
    };
    let (sink_tx, sink_rx) = mpsc::channel(sink_channel_size);
    // create local end signal channel
    let (outbound_gossip_tx, outbound_gossip_rx) =
        mpsc::channel(config.gossip().gossip_outbound_mpsc_channel_size);
    let (inbound_gossip_tx, inbound_gossip_rx) =
        mpsc::channel(config.gossip().gossip_inbound_mpsc_channel_size);

    let state = AppState::new_distributed(
        config,
        event_q_tx,
        loopback_q_tx,
        sink_tx,
        outbound_gossip_tx,
        inbound_gossip_tx,
    );

    setup_tcp_channels(state.clone()).await;

    distributed_online_cc::app::tcp_listener::spawn_conformance_network_listener(state.clone());
    distributed_online_cc::app::tcp_listener::spawn_overlay_network_listener(state.clone());
    distributed_online_cc::app::cli::spawn_cli(state.clone());
    distributed_online_cc::app::event_listener::spawn_event_listener(state.clone());
    distributed_online_cc::app::event_q_processor::spawn_event_processor(
        state.clone(),
        _event_q_rx,
    );
    distributed_online_cc::app::loopback_q_processor::spawn_loopback_q_processor(
        state.clone(),
        loopback_q_rx,
    );

    // spawn result sink if configured
    if let Some(result_sink_config) = state.config.result_sink.as_ref() {
        distributed_online_cc::app::result_sink::spawn_result_sink(
            result_sink_config.clone(),
            sink_rx,
        );
    }

    distributed_online_cc::app::overlay_manager::spawn_overlay_task(state.clone());
    distributed_online_cc::app::gossip_manager::spawn_case_end_gossip_manager(
        state.clone(),
        outbound_gossip_rx,
        inbound_gossip_rx,
    );

    // keeps main alive until i shut it down
    tokio::signal::ctrl_c().await.unwrap();
    log::warn!("shutting down main");
}

async fn setup_tcp_channels(state: AppState) {
    // create tcp channels from config if there are any

    let constraint_peers: Vec<SocketAddr> = state
        .handlers
        .all()
        .flat_map(|h| h.configured_peers())
        .collect();

    if constraint_peers.is_empty() {
        return;
    }

    log::info!("creating tcp channels to configured constraint check peers");
    log::debug!("configured constraint check peers: {:?}", constraint_peers);

    for address in constraint_peers {
        connect_peer(PeerAddress::new_conformance_address(address), state.clone()).await;
    }
}
