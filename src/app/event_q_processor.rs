use crate::{
    config::node_config::NodeConfig,
    state::app_state::AppState,
    types::{ConstraintId, ConstraintType, TracedEvent},
};

use std::sync::Arc;
use tokio::sync::mpsc;

/*
   useful for
       - serialization
       - no race conditions
       - easy stateful checks
       - events are in order and processed in order

   it might be better to not spawn new tasks to handle each event and just use the event q processor as a dispatcher because
       - handling is probably fast anyway (lock, mutate, unlock, ...)
       - ordering matters for correctness
       - bottleneck is probably network IO anyway
*/
pub fn spawn_event_processor(state: AppState, mut event_q: mpsc::Receiver<TracedEvent>) {
    tokio::spawn(async move {
        loop {
            if let Some(event) = event_q.recv().await {
                // is it a new case? -> register it in result store
                {
                    let mut result_store = state.cc_results.lock().unwrap();
                    if !result_store.has_case(&event.case_id) {
                        log::info!("identified new case id={}", &event.case_id);
                        let constraints = build_constraint_list(state.config.clone());
                        result_store.register_case(event.case_id.clone(), &constraints);
                    }
                }

                log::info!("processing event: {}", event);
                log::debug!("processing event: {:?}", event);

                // dispatch relevant handlers
                let event_id = event.name().unwrap_or("");
                for handler in state.handlers.get_by_event_id(event_id) {
                    handler.handle_event(&event, state.clone()).await;
                }
            }
        }
    });
}

fn build_constraint_list(config: Arc<NodeConfig>) -> Vec<(ConstraintId, ConstraintType)> {
    config
        .constraints
        .iter()
        .map(|c| (c.constraint_id().to_string(), c.constraint_type()))
        .collect()
}
