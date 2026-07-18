use crate::{
    overlay::cyclon_shuffle_initiation_task::cyclon_shuffle_initiation_task,
    state::app_state::AppState,
};

pub fn spawn_cyclon_overlay_task(state: AppState) {
    // populate peer cache
    log::info!("initializing overlay peer cache");
    {
        let mut peer_cache = state.dist().peer_cache.lock().unwrap();
        peer_cache.populate(
            state.config.overlay().initial_peers.clone(),
            state.config.overlay().max_peer_cache_size,
        );
    }

    tokio::spawn(cyclon_shuffle_initiation_task(state.clone()));
}
