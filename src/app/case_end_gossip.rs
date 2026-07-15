use std::iter::once;

use chrono::Utc;
use tokio::sync::mpsc;

use crate::{
    conformance_checking::case_closing_task::case_closing_task,
    network::{
        message_types::{CEGossipMessage, CEGossipPayload, PeerMessage},
        tcp_router::send_to_overlay_peer,
    },
    state::{app_state::AppState, case_end_gossip_cache::MergeResult},
    types::{CaseId, EventTime},
};

pub fn spawn_case_end_gossip_manager(
    state: AppState,
    mut local_end_receiver: mpsc::Receiver<(CaseId, EventTime)>, // 256
    mut gossip_message_receiver: mpsc::Receiver<CEGossipMessage>, // 1024
) {
    let mut anti_entropy_interval =
        tokio::time::interval(state.config.case_end_gossip.anti_entropy_interval);

    tokio::spawn(async move {
        loop {
            tokio::select! {
                // anti entropy ticks
                _ = anti_entropy_interval.tick() => {
                    let now = Utc::now();
                    log::info!("anti entropy: running anti entropy mechanism");
                    let digest = {
                        let mut ce_cache = state.ce_gossip_cache.lock().unwrap();
                        // prune expired records
                        ce_cache.prune(now);
                        ce_cache.digest(now)
                    };
                    let addresses = {
                        let peer_cache = state.peer_cache.lock().unwrap();
                        peer_cache.sample_addresses(state.config.case_end_gossip.anti_entropy_fanout, &mut rand::rng())
                    };
                    let digest_message = PeerMessage::ce_gossip(CEGossipMessage::new_ae_digest(state.config.overlay.listen_address, digest));
                    for address in addresses {
                        log::info!("anti entropy: sending digest {} to {}", digest, address);
                        if let Err(e) = send_to_overlay_peer(address, digest_message.clone(), state.clone()).await {
                            log::error!(
                                "case_end_gossip: error sending cache digest to {}: {}",
                                address,
                                e
                            );

                        }
                    }
                }

                // end signal from a local end activity
                Some((case_id, event_time)) = local_end_receiver.recv() => {
                    let now = Utc::now();
                    log::info!("received case end signal for case {} at {} from local end activity", case_id, event_time);
                    let merge_result = {
                        // update cache if needed
                        let mut ce_cache = state.ce_gossip_cache.lock().unwrap();
                        ce_cache.merge(once((case_id, event_time)), now)
                    };
                    apply_merge(merge_result, state.clone()).await;
                }

                // message from another peer
                Some(msg) = gossip_message_receiver.recv() => {
                    handle_inbound_message(msg, state.clone()).await;
                }

            }
        }
    });
}

async fn initiate_push_gossip(data: Vec<(CaseId, EventTime)>, state: AppState) {
    if data.is_empty() {
        return;
    }

    let addresses = {
        let peer_cache = state.peer_cache.lock().unwrap();
        peer_cache.sample_addresses(state.config.case_end_gossip.push_fanout, &mut rand::rng())
    };

    let push_gossip_message = PeerMessage::ce_gossip(CEGossipMessage::new_push(
        state.config.overlay.listen_address,
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
    let now = Utc::now();
    let payload_name = message.payload.name();
    match message.payload {
        CEGossipPayload::Push(data) | CEGossipPayload::AeSet(data) => {
            log::info!(
                "handle_inbound_message: received {} data of length {}",
                payload_name,
                data.len()
            );
            log::debug!("received gossip: {:?}", data);
            let merge_result = {
                let mut ce_cache = state.ce_gossip_cache.lock().unwrap();
                ce_cache.merge(data, now)
            };
            log::debug!("merge result: {:?}", merge_result);
            apply_merge(merge_result, state.clone()).await;
        }
        CEGossipPayload::AeDigest(their_digest) => {
            log::info!(
                "handle_inbound_message: received anti entropy digest: {}",
                their_digest
            );
            let (my_digest, live) = {
                let ce_cache = state.ce_gossip_cache.lock().unwrap();
                (ce_cache.digest(now), ce_cache.live_entries(now))
            };
            log::debug!("my digest: {}; their digest: {}", my_digest, their_digest);
            if my_digest != their_digest {
                // reply with live cache entries
                log::info!(
                    "digests differ - sending {} live cache entries to {}",
                    live.len(),
                    message.sender_address
                );
                let ae_reply_message = PeerMessage::ce_gossip(CEGossipMessage::new_ae_set(
                    state.config.overlay.listen_address,
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

async fn apply_merge(res: MergeResult, state: AppState) {
    // spawn a case closing task for newly observed case ends
    for (case_id, _event_time) in &res.new {
        spawn_case_closing_task(state.clone(), case_id.clone());
    }
    // spread gossip of new and updated case ends
    let mut to_push = res.new;
    to_push.extend(res.updated);
    initiate_push_gossip(to_push, state).await;
}

fn spawn_case_closing_task(state: AppState, case_id: CaseId) {
    // spawn case closing task
    let handle = tokio::spawn(case_closing_task(state.clone(), case_id.clone()));
    // spawn a task that logs the error if the case closing task panics
    tokio::spawn(async move {
        if let Err(e) = handle.await {
            log::error!("case closing task for case {} failed: {}", case_id, e);
        }
    });
}
