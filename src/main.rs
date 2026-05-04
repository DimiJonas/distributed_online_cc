use clap::Parser;
use distributed_online_cc::state::app_state::AppState;
use distributed_online_cc::types::PeerId;

// parse command line arguments
#[derive(Parser)]
struct Input {
    id: PeerId,
}

#[tokio::main]
async fn main() {
    let args = Input::parse();
    let host_id = args.id;
    let host_address =
        distributed_online_cc::util::address_id_conversion::get_address_from_id(host_id);
    let (event_q_tx, _event_q_rx) = tokio::sync::mpsc::channel(100);

    let state = AppState::new(host_id as u64 * 100, event_q_tx);

    distributed_online_cc::app::tcp_listener::spawn_listener(state.clone(), host_address);
    distributed_online_cc::app::cli::spawn_cli(state.clone(), host_id);
    distributed_online_cc::app::event_listener::spawn_event_listener(state.clone());
    distributed_online_cc::app::event_q_processor::spawn_event_processor(
        state.clone(),
        _event_q_rx,
    );

    // keeps main alive until i shut it down
    tokio::signal::ctrl_c().await.unwrap();
    println!("shutting down main");
}
