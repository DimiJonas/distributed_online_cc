use crate::state::app_state::AppState;
use crate::types::XesEvent;
use tokio::sync::mpsc;

pub fn spawn_event_processor(state: AppState, mut event_q: mpsc::Receiver<XesEvent>) {
    tokio::spawn(async move {
        loop {
            if let Some(event) = event_q.recv().await {
                println!(
                    "processing event: {}",
                    serde_json::to_string_pretty(&event).unwrap()
                );
            }
        }
    });
}
