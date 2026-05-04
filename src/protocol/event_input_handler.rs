use crate::types::XesEvent;
use crate::util::xes_parsing::parse_xes;
use bytes::BytesMut;
use tokio::io::AsyncReadExt;
use tokio::net::UnixStream;
use tokio::sync::mpsc;

pub async fn handle_event(mut stream: UnixStream, event_q: mpsc::Sender<XesEvent>) {
    let mut read_buf = BytesMut::with_capacity(8 * 1024);

    loop {
        let n = stream.read_buf(&mut read_buf).await.unwrap();
        if n == 0 {
            // full document received, or connection closed
            break;
        }

        for event in parse_xes(&read_buf) {
            if event_q.send(event).await.is_err() {
                // error, shut down event handler worker task
                eprintln!("event input handler dropped, unable to send event");
                break;
            }
        }
    }
}
