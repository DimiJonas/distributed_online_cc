use std::sync::Arc;
use std::time::Duration;

use crate::conformance_checking::event_input_handler::handle_event;
use crate::state::app_state::AppState;
use tokio::net::UnixListener;
use tokio::time::sleep;

pub fn spawn_event_listener(state: AppState) {
    let socket_path = state.config.unix_socket.clone();
    // remove existing file, possibly from previous run
    let _ = std::fs::remove_file(&socket_path);

    tokio::spawn(async move {
        let listener = UnixListener::bind(&socket_path).unwrap();
        log::info!("event listener started on {}", socket_path);

        let configured_attributes = Arc::new(state.config.build_event_attribute_list());
        let configured_events = Arc::new(state.config.build_event_list());

        loop {
            let (stream, _) = match listener.accept().await {
                Ok(pair) => pair,
                Err(e) => {
                    log::error!("event listener: accept failed: {}", e);
                    sleep(Duration::from_millis(5)).await;
                    continue;
                }
            };
            log::debug!("received input on event listener unix socket");

            tokio::spawn(handle_event(
                stream,
                state.event_q.clone(),
                configured_attributes.clone(),
                configured_events.clone(),
                state.clone(),
            ));
        }
    });
}
