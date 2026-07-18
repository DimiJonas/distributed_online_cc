use std::net::SocketAddr;

use tokio::sync::mpsc;

use crate::{
    config::node_config::{ConfigMode, NodeConfig},
    network::tcp_router::connect_peer,
    state::app_state::AppState,
    types::PeerAddress,
};

pub async fn run(config: NodeConfig) {
    log::info!(
        "starting node {} (mode: {})",
        config.node_id,
        match config.mode {
            ConfigMode::Distributed => "distributed",
            ConfigMode::Central => "central",
        }
    );
    log::debug!("configuration: {:?}", config);

    let (event_q_tx, event_q_rx) = mpsc::channel(config.event_queue_size);
    let (loopback_q_tx, loopback_q_rx) = mpsc::channel(config.loopback_queue_size);
    let sink_channel_size = {
        match config.result_sink.as_ref() {
            Some(sink_config) => sink_config.mpsc_channel_size,
            None => 16,
        }
    };
    let (sink_tx, sink_rx) = mpsc::channel(sink_channel_size);

    let (state, outbound_gossip_rx, inbound_gossip_rx) = match config.mode {
        ConfigMode::Distributed => {
            // gossip channels (only exist for distributed mode)
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
            (state, Some(outbound_gossip_rx), Some(inbound_gossip_rx))
        }
        ConfigMode::Central => (
            AppState::new_central(config, event_q_tx, loopback_q_tx, sink_tx),
            None,
            None,
        ),
    };

    setup_tcp_channels(state.clone()).await;

    crate::app::tcp_listener::spawn_conformance_network_listener(state.clone());
    crate::app::cli::spawn_cli(state.clone());
    crate::app::event_listener::spawn_event_listener(state.clone());
    crate::app::event_q_processor::spawn_event_processor(state.clone(), event_q_rx);
    crate::app::loopback_q_processor::spawn_loopback_q_processor(state.clone(), loopback_q_rx);

    // spawn result sink if configured
    if let Some(result_sink_config) = state.config.result_sink.as_ref() {
        crate::app::result_sink::spawn_result_sink(result_sink_config.clone(), sink_rx);
    }

    // mode specific tasks
    if state.config.mode == ConfigMode::Distributed {
        crate::app::tcp_listener::spawn_overlay_network_listener(state.clone());
        crate::app::overlay_manager::spawn_cyclon_overlay_task(state.clone());
        crate::app::gossip_manager::spawn_case_end_gossip_manager(
            state.clone(),
            outbound_gossip_rx.unwrap(),
            inbound_gossip_rx.unwrap(),
        );
    }

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
