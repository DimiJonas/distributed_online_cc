use crate::{
    state::app_state::AppState,
    types::{ConstraintId, ConstraintType, TracedEvent},
};
use tokio::sync::mpsc;

// useful for serialization, no race conditions, events are in order and processed in order, easy stateful checks
pub fn spawn_event_processor(state: AppState, mut event_q: mpsc::Receiver<TracedEvent>) {
    tokio::spawn(async move {
        loop {
            if let Some(event) = event_q.recv().await {
                // is it a new case?
                {
                    let mut store = state.cc_results.lock().unwrap();
                    if !store.has_case(&event.case_id) {
                        let constraints: Vec<(ConstraintId, ConstraintType)> = state
                            .config
                            .constraints
                            .iter()
                            .map(|c| (c.constraint_id().to_string(), c.constraint_type()))
                            .collect();
                        store.register_case(event.case_id.clone(), &constraints);
                    }
                }

                println!(
                    "processing event: {}",
                    serde_json::to_string_pretty(&event).unwrap()
                );
            }
        }
    });
}
