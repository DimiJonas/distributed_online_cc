use std::{
    io::{IsTerminal, stdin},
    net::SocketAddr,
    time::Duration,
};

use tokio::{sync::mpsc, time::sleep};

use crate::{
    config::node_config::{ConfigMode, NodeConfig},
    measurement::api::meta,
    network::tcp_router::connect_peer,
    state::app_state::AppState,
    types::PeerAddress,
};

pub async fn run(config: NodeConfig) {
    log::info!(
        "starting node {} (mode: {})",
        config.node_id,
        match config.mode {
            ConfigMode::Decentralized => "distributed",
            ConfigMode::Central => "central",
        }
    );
    log::debug!("configuration: {:?}", config);

    // initialize measurement and spawn the record writer (if enabled)
    crate::measurement::record_writer::spawn_record_writer(config.node_id);

    let (event_q_tx, event_q_rx) = mpsc::channel(config.event_queue_size);
    let (loopback_q_tx, loopback_q_rx) = mpsc::channel(config.loopback_queue_size);
    let sink_channel_size = {
        match config.result_sink.as_ref() {
            Some(sink_config) => sink_config.mpsc_channel_size,
            None => 16,
        }
    };
    let (sink_tx, sink_rx) = mpsc::channel(sink_channel_size);

    let (state, outbound_gossip_rx, inbound_gossip_rx) = match config.participates_in_overlay() {
        true => {
            // gossip channels (only exist for distributed mode)
            let (outbound_gossip_tx, outbound_gossip_rx) =
                mpsc::channel(config.gossip().gossip_outbound_mpsc_channel_size);
            let (inbound_gossip_tx, inbound_gossip_rx) =
                mpsc::channel(config.gossip().gossip_inbound_mpsc_channel_size);
            let state = AppState::new_decentralized(
                config,
                event_q_tx,
                loopback_q_tx,
                sink_tx,
                outbound_gossip_tx,
                inbound_gossip_tx,
            );
            (state, Some(outbound_gossip_rx), Some(inbound_gossip_rx))
        }
        false => (
            AppState::new_central(config, event_q_tx, loopback_q_tx, sink_tx),
            None,
            None,
        ),
    };

    setup_tcp_channels(state.clone()).await;

    // record some meta info
    meta(
        match state.config.mode {
            ConfigMode::Central => "central",
            ConfigMode::Decentralized => "distributed",
        },
        state.config.constraints.len(),
        state.config.tcp_listen_address,
        state
            .config
            .overlay
            .as_ref()
            .map(|overlay_config| overlay_config.listen_address),
    );

    if stdin().is_terminal() {
        crate::app::cli::spawn_cli(state.clone());
    }

    crate::app::tcp_listener::spawn_conformance_network_listener(state.clone());
    crate::app::event_listener::spawn_event_listener(state.clone());
    crate::app::event_q_processor::spawn_event_processor(state.clone(), event_q_rx);
    crate::app::loopback_q_processor::spawn_loopback_q_processor(state.clone(), loopback_q_rx);

    // spawn result sink if configured
    if let Some(result_sink_config) = state.config.result_sink.as_ref() {
        crate::app::result_sink::spawn_result_sink(result_sink_config.clone(), sink_rx);
    }

    // mode specific tasks
    if state.dist.is_some() {
        crate::app::tcp_listener::spawn_overlay_network_listener(state.clone());
        crate::app::overlay_manager::spawn_cyclon_overlay_task(state.clone());
        crate::app::gossip_manager::spawn_case_end_gossip_manager(
            state.clone(),
            outbound_gossip_rx.unwrap(),
            inbound_gossip_rx.unwrap(),
        );
    }

    crate::app::case_janitor::spawn_case_janitor(
        state.clone(),
        state.config.case_janitor_cleanup_interval,
    );

    // periodically record queue depth (if enabled)
    crate::measurement::queue_depth_sampler::spawn_queue_depth_sampler(state.clone());

    // keeps main alive until i shut it down
    let signal = wait_for_shutdown().await;
    log::warn!("received {}, shutting down main", signal);

    // let results in transit reach the sink and then flush the metrics file
    sleep(Duration::from_millis(500)).await;
    crate::measurement::record_writer::shutdown_record_writer();
}

async fn setup_tcp_channels(state: AppState) {
    // create tcp channels from config if there are any

    let mut constraint_peers: Vec<SocketAddr> = state
        .handlers
        .all()
        .flat_map(|h| h.configured_peers())
        .collect();

    if let Some(forwarder_config) = &state.config.forwarder {
        constraint_peers.push(forwarder_config.central_node_address);
    }

    if constraint_peers.is_empty() {
        return;
    }

    log::info!("creating tcp channels to configured peers");
    log::debug!("configured peers: {:?}", constraint_peers);

    for address in constraint_peers {
        connect_peer(PeerAddress::new_conformance_address(address), state.clone()).await;
    }
}

// ctrl_c sends SIGINT on unix
// a standard 'kill' signal is SIGTERM
#[cfg(unix)]
async fn wait_for_shutdown() -> &'static str {
    use tokio::signal::unix::{SignalKind, signal};

    let mut sigterm = signal(SignalKind::terminate()).expect("cannot install SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("cannot install SIGINT handler");

    tokio::select! {
        _ = sigterm.recv() => "SIGTERM",
        _ = sigint.recv() => "SIGINT",
    }
}

// windows
#[cfg(not(unix))]
async fn wait_for_shutdown() -> &'static str {
    tokio::signal::ctrl_c()
        .await
        .expect("cannot listen for ctrl_c");
    "ctrl_c"
}
