use std::{collections::HashMap, net::SocketAddr};

use rand::seq::IteratorRandom;

use crate::types::{Age, PeerBatch};

pub struct PeerCache {
    // address -> age
    peers: HashMap<SocketAddr, Age>,
}

impl PeerCache {
    pub fn new() -> Self {
        Self {
            peers: HashMap::new(),
        }
    }

    pub fn populate(&mut self, initial_peers: Vec<SocketAddr>, max_cache_size: usize) {
        if initial_peers.len() > max_cache_size {
            log::warn!(
                "{} initial peers were configured, but max cache size is only {}. using only the first {} peers",
                initial_peers.len(),
                max_cache_size,
                max_cache_size
            );
        }
        self.peers.extend(
            initial_peers
                .into_iter()
                .take(max_cache_size)
                .map(|p| (p, 0)),
        );
    }

    pub fn advance_age(&mut self) {
        for (_, age) in self.peers.iter_mut() {
            *age += 1
        }
    }

    pub fn remove_oldest(&mut self) -> Option<(SocketAddr, Age)> {
        let oldest_key = *self
            .peers
            .iter()
            .max_by_key(|(_, age)| **age)
            .map(|(k, _)| k)?;
        self.peers.remove(&oldest_key).map(|age| (oldest_key, age))
    }

    pub fn select_random_subset(&self, subset_size: usize, rng: &mut impl rand::Rng) -> PeerBatch {
        self.peers
            .iter()
            .map(|(&k, &v)| (k, v))
            .sample(rng, subset_size) // rand::seq::IteratorRandom
            .into_iter()
            .collect()
    }

    /// updates the peer cache as described in CYCLON:
    /// 1. discard entries pointing at self
    /// 2. discard entries already contained in the cache
    /// 3. fill empty slots
    /// 4. replace entries that have been sent to peer that the update entries were received from
    pub fn update(
        &mut self,
        own_address: SocketAddr,
        sent: &[SocketAddr],
        received: PeerBatch,
        max_cache_size: usize,
    ) {
        let mut sent_iter = sent.iter();

        for (received_addr, received_age) in received {
            // discard entries pointing at self
            if received_addr == own_address {
                continue;
            }
            // discard entries already contained in the cache
            if self.peers.contains_key(&received_addr) {
                continue;
            }

            // fill empty slots
            if self.peers.len() < max_cache_size {
                self.peers.insert(received_addr, received_age);
                continue;
            }

            // replace entries that have been sent to peer that the update entries were received from
            // it might happen that some entries from the sent subset are not present anymore (especially in the case of initiating a shuffle)
            let sent_entry = loop {
                match sent_iter.next() {
                    Some(candidate) if self.peers.contains_key(candidate) => {
                        break Some(*candidate);
                    }
                    Some(_) => continue, // entry is not present anymore
                    None => break None,  // no more sent entries
                }
            };

            match sent_entry {
                Some(sent_addr) => {
                    self.peers.remove(&sent_addr);
                    self.peers.insert(received_addr, received_age);
                }
                None => {
                    // no capacity left - silently drop excess received entries
                }
            }
        }
    }
}

impl Default for PeerCache {
    fn default() -> Self {
        Self::new()
    }
}
