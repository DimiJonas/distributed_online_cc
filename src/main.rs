use clap::Parser;
use distributed_online_cc::config::node_config::NodeConfig;
use distributed_online_cc::network::tcp_router::connect_peer;
use distributed_online_cc::state::app_state::AppState;
use distributed_online_cc::types::PeerAddress;
use log::LevelFilter;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
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

    // config owns the filename; --log-dir (if it exists) owns the directory
    let log_path: PathBuf = match &args.log_dir {
        Some(dir) => {
            let file_name = Path::new(&config.log_file)
                .file_name()
                .unwrap_or_else(|| std::ffi::OsStr::new("app.log"));
            Path::new(dir).join(file_name)
        }
        None => PathBuf::from(&config.log_file),
    };

    // ensure the directory exists so logging init can't fail on a missing path
    if let Some(parent) = log_path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent).expect("failed to create log directory");
    }

    let log_path_str = log_path.to_string_lossy().into_owned();
    setup_logging(&log_path_str).expect("failed to initialize logging");
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
    let (local_end_signal_tx, local_end_signal_rx) =
        mpsc::channel(config.case_end_gossip.local_end_signal_mpsc_channel_size);
    let (gossip_msg_q_tx, gossip_msg_q_rx) =
        mpsc::channel(config.case_end_gossip.gossip_message_queue_size);

    let state = AppState::new(
        config,
        event_q_tx,
        loopback_q_tx,
        sink_tx,
        local_end_signal_tx,
        gossip_msg_q_tx,
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
    distributed_online_cc::app::case_end_gossip::spawn_case_end_gossip_manager(
        state.clone(),
        local_end_signal_rx,
        gossip_msg_q_rx,
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
