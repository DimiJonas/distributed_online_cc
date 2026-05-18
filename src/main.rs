use clap::Parser;
use distributed_online_cc::config::node_config::NodeConfig;
use distributed_online_cc::network::tcp_router::connect_peer;
use distributed_online_cc::state::app_state::AppState;
use log::LevelFilter;
use std::net::SocketAddr;
use tokio::sync::mpsc;

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

    setup_logging(&config.log_file).expect("failed to initialize logging");
    log::info!("starting node");

    // create event queue
    let (event_q_tx, _event_q_rx) = mpsc::channel(config.event_queue_size);
    // create outbound router message queue
    let (outbound_router_q_tx, outbound_router_q_rx) = mpsc::channel(1024);

    let state = AppState::new(config, event_q_tx, outbound_router_q_tx);

    setup_tcp_channels(state.clone()).await;

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
    distributed_online_cc::app::outbound_router_q_processor::spawn_outbound_router_processor(
        outbound_router_q_rx,
        state.clone(),
    );

    // keeps main alive until i shut it down
    tokio::signal::ctrl_c().await.unwrap();
    log::warn!("shutting down main");
}

async fn setup_tcp_channels(state: AppState) {
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

fn setup_logging(log_file: &str) -> Result<(), fern::InitError> {
    let format =
        |out: fern::FormatCallback, message: &std::fmt::Arguments, record: &log::Record| {
            out.finish(format_args!(
                "[{} {} {}] {}",
                chrono::Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.level(),  // error, warn, info, debug
                record.target(), // module path, e.g. "distributed_online_cc::network::tcp_channel"
                message
            ))
        };

    // read desired level from env var, default to Info
    // e.g. RUST_LOG=info cargo run
    let level = std::env::var("RUST_LOG")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(LevelFilter::Info);

    fern::Dispatch::new()
        .format(format)
        .level(level)
        // stdout for Info and above
        .chain(std::io::stdout())
        // file gets everything
        .chain(fern::log_file(log_file)?)
        .apply()?;

    Ok(())
}
