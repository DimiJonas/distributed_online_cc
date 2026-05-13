use std::net::SocketAddr;

use clap::Parser;
use distributed_online_cc::config::node_config::NodeConfig;
use distributed_online_cc::network::tcp_channel::connect_peer;
use distributed_online_cc::state::app_state::AppState;

// parse command line arguments
#[derive(Parser)]
struct Input {
    #[arg(short, long, default_value = "config.toml")]
    pub config_path: String,
}

#[tokio::main]
async fn main() {
    let args = Input::parse();
    let config: NodeConfig = NodeConfig::load_config(&args.config_path);
    let (event_q_tx, _event_q_rx) = tokio::sync::mpsc::channel(config.event_queue_size);

    let state = AppState::new(config, event_q_tx);

    initialize_node(state.clone()).await;

    distributed_online_cc::app::tcp_listener::spawn_listener(
        state.clone(),
        state.config.listen_address,
    );
    distributed_online_cc::app::cli::spawn_cli(state.clone());
    distributed_online_cc::app::event_listener::spawn_event_listener(state.clone());
    distributed_online_cc::app::event_q_processor::spawn_event_processor(
        state.clone(),
        _event_q_rx,
    );

    // keeps main alive until i shut it down
    tokio::signal::ctrl_c().await.unwrap();
    println!("shutting down main");
}

async fn initialize_node(state: AppState) {
    // create tcp channels from config
    let constraint_peers: Vec<SocketAddr> = state
        .handlers
        .all()
        .flat_map(|h| h.configured_peers())
        .collect();
    for peer_address in constraint_peers {
        connect_peer(peer_address, state.clone()).await;
    }
}
