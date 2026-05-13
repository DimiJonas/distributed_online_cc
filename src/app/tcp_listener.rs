use std::net::SocketAddr;

use crate::network::tcp_channel::peer_task_accepted;
use crate::state::app_state::AppState;
use tokio::net::TcpListener;

pub fn spawn_listener(state: AppState, host_address: SocketAddr) {
    tokio::spawn(async move {
        let listener = TcpListener::bind(&host_address).await.unwrap();
        println!("listening for peer messages on {}", host_address);

        loop {
            let (stream, _) = listener.accept().await.unwrap();

            tokio::spawn(peer_task_accepted(stream, state.clone()));
        }
    });
}
