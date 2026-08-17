use chrono::Utc;
use tokio::sync::mpsc;

use crate::{
    conformance_checking::case_end_signal_handling::handle_case_end_info,
    measurement::record::CaseEndSource,
    network::{
        message_types::{CEGossipMessage, CEGossipPayload, PeerMessage},
        tcp_router::send_to_overlay_peer,
    },
    state::app_state::AppState,
    types::CaseEnd,
};

pub fn spawn_case_end_gossip_manager(
    state: AppState,
    mut outbound_gossip_q: mpsc::Receiver<Vec<CaseEnd>>,
    mut inbound_gossip_q: mpsc::Receiver<CEGossipMessage>,
) {
    let mut anti_entropy_interval =
        tokio::time::interval(state.config.gossip().anti_entropy_interval);

    tokio::spawn(async move {
        loop {
            tokio::select! {
                // anti entropy ticks
                _ = anti_entropy_interval.tick() => {
                    let now = Utc::now();
                    log::debug!("anti entropy: running anti entropy mechanism");
                    let digest = {
                        let mut ce_cache = state.dist().ce_gossip_cache.lock().unwrap();
                        // prune expired records
                        ce_cache.prune(now);
                        ce_cache.digest(now)
                    };
                    let addresses = {
                        let peer_cache = state.dist().peer_cache.lock().unwrap();
                        peer_cache.sample_addresses(state.config.gossip().anti_entropy_fanout, &mut rand::rng())
                    };
                    if addresses.is_empty() {
                        log::warn!("anti_entropy: cannot run anti entropy mechanism because of no known peers");
                        continue;
                    }
                    let digest_message = PeerMessage::ce_gossip(CEGossipMessage::new_ae_digest(state.config.overlay().listen_address, digest));
                    for address in addresses {
                        log::debug!("anti entropy: sending digest {} to {}", digest, address);
                        if let Err(e) = send_to_overlay_peer(address, digest_message.clone(), state.clone()).await {
                            log::error!(
                                "case_end_gossip: error sending cache digest to {}: {}",
                                address,
                                e
                            );

                        }
                    }
                }

                // the local node wants to spread gossip
                Some(data) = outbound_gossip_q.recv() => {
                    initiate_push_gossip(data, state.clone()).await;
                }

                // receive gossip from another peer
                Some(msg) = inbound_gossip_q.recv() => {
                    handle_inbound_message(msg, state.clone()).await;
                }

            }
        }
    });
}

pub async fn initiate_push_gossip(data: Vec<CaseEnd>, state: AppState) {
    if data.is_empty() {
        return;
    }

    let addresses = {
        let peer_cache = state.dist().peer_cache.lock().unwrap();
        peer_cache.sample_addresses(state.config.gossip().push_fanout, &mut rand::rng())
    };

    let push_gossip_message = PeerMessage::ce_gossip(CEGossipMessage::new_push(
        state.config.overlay().listen_address,
        data,
    ));

    for address in addresses {
        if let Err(e) =
            send_to_overlay_peer(address, push_gossip_message.clone(), state.clone()).await
        {
            log::error!(
                "case_end_gossip: error sending push gossip to {}: {}",
                address,
                e
            );
        }
    }
}

async fn handle_inbound_message(message: CEGossipMessage, state: AppState) {
    let payload_name = message.payload.name();
    match message.payload {
        CEGossipPayload::Push(data) => {
            log::debug!(
                "handle_inbound_message: received {} data of length {}",
                payload_name,
                data.len()
            );
            log::debug!("received data: {:?}", data);
            handle_case_end_info(
                state.clone(),
                data,
                CaseEndSource::PushGossip,
                Some(message.sender_address),
            )
            .await;
        }
        CEGossipPayload::AeSet(data) => {
            log::debug!(
                "handle_inbound_message: received {} data of length {}",
                payload_name,
                data.len()
            );
            log::debug!("received data: {:?}", data);
            handle_case_end_info(
                state.clone(),
                data,
                CaseEndSource::AntiEntropy,
                Some(message.sender_address),
            )
            .await;
        }
        CEGossipPayload::AeDigest(their_digest) => {
            log::debug!(
                "handle_inbound_message: received anti entropy digest: {}",
                their_digest
            );
            let now = Utc::now();
            let (my_digest, live) = {
                let ce_cache = state.dist().ce_gossip_cache.lock().unwrap();
                (ce_cache.digest(now), ce_cache.live_entries(now))
            };
            log::debug!("my digest: {}; their digest: {}", my_digest, their_digest);
            if my_digest != their_digest {
                // reply with live cache entries
                log::debug!(
                    "digests differ - sending {} live cache entries to {}",
                    live.len(),
                    message.sender_address
                );
                let ae_reply_message = PeerMessage::ce_gossip(CEGossipMessage::new_ae_set(
                    state.config.overlay().listen_address,
                    live,
                ));
                if let Err(e) =
                    send_to_overlay_peer(message.sender_address, ae_reply_message, state.clone())
                        .await
                {
                    log::error!(
                        "handle_inbound_message: error replying with anti entropy set to {}: {}",
                        message.sender_address,
                        e
                    );
                }
            }
        }
    }
}
