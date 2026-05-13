use crate::protocol::event_input_handler::handle_event;
use crate::state::app_state::AppState;
use tokio::net::UnixListener;

pub fn spawn_event_listener(state: AppState) {
    let socket_path = state.config.unix_socket.clone();
    // remove existing file, possibly from previous run
    let _ = std::fs::remove_file(&socket_path);

    tokio::spawn(async move {
        let listener = UnixListener::bind(&socket_path).unwrap();
        println!("event listener started on {}", socket_path);

        loop {
            let (stream, _) = listener.accept().await.unwrap();
            println!("received input on event listener unix socket");

            tokio::spawn(handle_event(stream, state.event_q.clone()));
        }
    });
}
