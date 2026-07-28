use crate::network::tcp_channel::accept_incoming_tcp_connection;
use crate::state::app_state::AppState;
use crate::types::NetworkType;
use tokio::net::TcpListener;

pub fn spawn_conformance_network_listener(state: AppState) {
    tokio::spawn(async move {
        let listener = TcpListener::bind(&state.config.tcp_listen_address)
            .await
            .unwrap();
        log::info!(
            "listening in conformance network for peer messages on {}",
            state.config.tcp_listen_address
        );

        loop {
            let (stream, _) = listener.accept().await.unwrap();
            // turn of the tcp nagle algorithm to be able to send small messages immediately
            if let Err(e) = stream.set_nodelay(true) {
                log::warn!("could not set TCP_NODELAY on incoming connection: {}", e);
            }
            tokio::spawn(accept_incoming_tcp_connection(
                stream,
                NetworkType::Conformance,
                state.clone(),
            ));
        }
    });
}

pub fn spawn_overlay_network_listener(state: AppState) {
    tokio::spawn(async move {
        let listener = TcpListener::bind(&state.config.overlay().listen_address)
            .await
            .unwrap();
        log::info!(
            "listening in overlay network for peer messages on {}",
            state.config.overlay().listen_address
        );

        loop {
            let (stream, _) = listener.accept().await.unwrap();
            // turn of the tcp nagle algorithm to be able to send small messages immediately
            if let Err(e) = stream.set_nodelay(true) {
                log::warn!("could not set TCP_NODELAY on incoming connection: {}", e);
            }
            tokio::spawn(accept_incoming_tcp_connection(
                stream,
                NetworkType::Overlay,
                state.clone(),
            ));
        }
    });
}
