use std::net::SocketAddr;

use tokio::time::sleep;

use crate::{
    network::{
        message_types::{OverlayMessage, PeerMessage, PeerMessagePayload},
        tcp_router::send_and_receive_overlay,
    },
    state::app_state::AppState,
    types::RequestId,
};

/// creates and manages a cyclon overlay
///
/// P: initiating peer
/// Q: random peer of P
/// l: shuffling subset size ("shuffle length")
/// c: peer cache size (how many peers are kept at max)
///
/// enhanced shuffling (cyclon):
///     P chooses Q by age (oldest) and initiates the enhanced shuffle:
///     1. increase all neighbor ages by 1
///     2. select oldest neighbor as Q and additionally l-1 other random neighbors
///     3. replace Q's entry with P's own address and age 0
///     4. send updated subset to Q
///     5. receive subset from Q with i entries
///     6. discard entries of P and entries already known to P
///     7. update P's cache with the remaining entries by first using empty cache slots and then replacing entries that were sent to Q
///     when Q receives the subset from P it replies with a random subset of its own and updates entries using the received subset BUT it does not increase age!
///     age is only increased when shuffling is initiated on its own
pub async fn cyclon_shuffle_initiation_task(state: AppState) {
    loop {
        sleep(state.config.overlay().shuffle_interval).await;
        initiate_shuffle(state.clone()).await;
    }
}

async fn initiate_shuffle(state: AppState) {
    log::debug!("cyclon overlay: initiating shuffle");

    let (subset, q_address) = {
        let mut peer_cache = state.dist().peer_cache.lock().unwrap();
        // first let all entries age
        peer_cache.advance_age();
        // then remove oldest entry
        let q = match peer_cache.remove_oldest() {
            Some((address, _)) => address,
            None => {
                log::error!(
                    "cyclon overlay: peer cache does not contain any addresses; cannot send shuffle request"
                );
                return;
            }
        };
        // now select batch of l-1 entries
        let mut subset: std::collections::HashMap<SocketAddr, u64> = peer_cache
            .select_random_subset(state.config.overlay().shuffle_length - 1, &mut rand::rng());
        if subset.is_empty() {
            log::warn!("peer cache does not contain any more addresses than Q");
        }
        // and add own address to batch with age 0
        subset.insert(state.config.overlay().listen_address, 0);

        (subset, q)
    };

    log::debug!("cyclon overlay: subset to be sent to Q: {:?}", subset);

    // this is used to later replace the peer cache entries that have been sent to Q during the cache update
    let sent: Vec<SocketAddr> = subset.keys().copied().collect();

    let shuffle_req = PeerMessage::overlay_req(
        RequestId::new_v4(),
        OverlayMessage::new(state.config.overlay().listen_address, subset),
    );

    // send peer subset to Q and receive another subset from Q
    log::debug!(
        "cyclon overlay: sending subset of size {} to {}",
        sent.len(),
        q_address,
    );
    let received_subset = match send_and_receive_overlay(q_address, shuffle_req, state.clone())
        .await
    {
        Ok(response) => match response.payload {
            PeerMessagePayload::Overlay(msg) => msg.peer_batch,
            _ => {
                log::error!(
                    "cyclon overlay: received wrong message type: \"{}\"; expected type: \"overlay\"",
                    response.payload
                );
                return;
            }
        },
        Err(e) => {
            log::error!(
                "cyclone overlay: error receiving subset from {} during shuffle: {}",
                q_address,
                e
            );
            return;
        }
    };
    log::debug!(
        "cyclone overlay: received subset of size {} from {}: {:?}",
        received_subset.len(),
        q_address,
        received_subset,
    );

    {
        let mut peer_cache = state.dist().peer_cache.lock().unwrap();
        peer_cache.update(
            state.config.overlay().listen_address,
            &sent,
            received_subset,
            state.config.overlay().max_peer_cache_size,
        );
    }
}
