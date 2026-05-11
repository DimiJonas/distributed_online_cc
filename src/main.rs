use clap::Parser;
use distributed_online_cc::config::config_loader::load_config;
use distributed_online_cc::state::app_state::AppState;

// parse command line arguments
#[derive(Parser)]
struct Input {
    #[arg(short, long, default_value = "config.toml")]
    pub config: String,
}

#[tokio::main]
async fn main() {
    let args = Input::parse();
    let config = load_config(&args.config);
    let host_id = config.node_id;

    let (event_q_tx, _event_q_rx) = tokio::sync::mpsc::channel(100);

    let state = AppState::new(config, host_id as u64 * 1000, event_q_tx);

    initialize_node(state.clone());

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

fn initialize_node(state: AppState) {
    // TODO: create tcp channels from config
}
