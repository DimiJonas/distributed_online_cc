use clap::Parser;
use distributed_online_cc::state::app_state::AppState;
use distributed_online_cc::types::PeerId;

// parse command line arguments
#[derive(Parser)]
struct Input {
    id: PeerId,
}

fn get_address_from_id(id: PeerId) -> String {
    format!("127.0.0.1:{}", 7000 + id as u16)
}

#[tokio::main]
async fn main() {
    let args = Input::parse();
    let host_id = args.id;
    let host_address = get_address_from_id(host_id);

    let state = AppState::new(host_id as u64 * 100);

    distributed_online_cc::app::tcp_listener::spawn_listener(state.clone(), host_address);
    distributed_online_cc::app::cli::spawn_cli(state.clone(), host_id);

    // keeps main alive until i shut it down
    tokio::signal::ctrl_c().await.unwrap();
    println!("shutting down main");
}
