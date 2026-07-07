use std::net::SocketAddr;

use crate::network::tcp_channel::accept_incoming_tcp_connection;
use crate::state::app_state::AppState;
use crate::types::NetworkType;
use tokio::net::TcpListener;

pub fn spawn_conformance_network_listener(state: AppState, host_address: SocketAddr) {
    tokio::spawn(async move {
        let listener = TcpListener::bind(&host_address).await.unwrap();
        log::info!(
            "listening in conformance network for peer messages on {}",
            host_address
        );

        loop {
            let (stream, _) = listener.accept().await.unwrap();

            tokio::spawn(accept_incoming_tcp_connection(
                stream,
                NetworkType::Conformance,
                state.clone(),
            ));
        }
    });
}

pub fn spawn_overlay_network_listener(state: AppState, host_address: SocketAddr) {
    tokio::spawn(async move {
        let listener = TcpListener::bind(&host_address).await.unwrap();
        log::info!(
            "listening in overlay network for peer messages on {}",
            host_address
        );

        loop {
            let (stream, _) = listener.accept().await.unwrap();

            tokio::spawn(accept_incoming_tcp_connection(
                stream,
                NetworkType::Overlay,
                state.clone(),
            ));
        }
    });
}
