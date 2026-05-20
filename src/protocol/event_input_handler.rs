use crate::types::{CaseId, TracedEvent};
use bytes::BytesMut;
use process_mining::core::event_data::case_centric::xes::build_ignore_attributes;
use process_mining::core::event_data::case_centric::xes::{XESImportOptions, import_xes_slice};
use process_mining::core::event_data::case_centric::{Trace, XESEditableAttribute};
use tokio::io::AsyncReadExt;
use tokio::net::UnixStream;
use tokio::sync::mpsc;

pub async fn handle_event(mut stream: UnixStream, event_q: mpsc::Sender<TracedEvent>) {
    let mut read_buf = BytesMut::with_capacity(8 * 1024);

    loop {
        let n = stream.read_buf(&mut read_buf).await.unwrap();
        if n == 0 {
            // full document received, or connection closed
            break;
        }

        let import_options: XESImportOptions = XESImportOptions {
            // TODO:
            // only parse the attributes actually needed
            ignore_event_attributes_except: Some(build_ignore_attributes(vec![
                "concept:name",
                "time:timestamp",
                "sensor_id",
                "median_vout",
                "some:count",
            ])),
            // auto-sort events within each trace by timestamp
            sort_events_with_timestamp_key: Some("time:timestamp".to_string()),
            // silence parse warnings in production
            verbose: false,
            ..XESImportOptions::default()
        };

        let log = import_xes_slice(&read_buf, false, import_options).unwrap();
        for trace in log.traces {
            let trace_id: CaseId = get_trace_id(&trace);

            for event in trace.events {
                let traced_event = TracedEvent {
                    case_id: trace_id.clone(),
                    event,
                };

                // TODO: filter events to only put events that are configured to be checked (are part of a constraint for this node) into the queue

                if event_q.send(traced_event).await.is_err() {
                    // error, shut down event handler worker task
                    log::error!("event input handler dropped, unable to send event");
                    break;
                }
            }
        }
    }
}

fn get_trace_id(trace: &Trace) -> String {
    // TODO: handle events from Isa if the case id is an event attribute named 'case:concept:name'
    trace
        .attributes
        .get_by_key("concept:name")
        .and_then(|a| a.value.try_as_string())
        .cloned()
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use tokio::{io::AsyncWriteExt, net::UnixListener, sync::mpsc};

    use crate::protocol::event_input_handler::handle_event;
    use crate::util::test_tools::minimal_xes;

    /// A valid XES document produces a TracedEvent on the queue.
    #[tokio::test]
    async fn valid_xes_produces_event_on_queue() {
        let socket_path = "/tmp/test_eih_valid.sock";
        let _ = std::fs::remove_file(socket_path);

        let listener = UnixListener::bind(socket_path).unwrap();
        let (event_tx, mut event_rx) = mpsc::channel(16);

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_event(stream, event_tx).await;
        });

        let mut client = tokio::net::UnixStream::connect(socket_path).await.unwrap();
        client
            .write_all(minimal_xes("case-xes", "event-a").as_bytes())
            .await
            .unwrap();
        drop(client);

        let event = tokio::time::timeout(std::time::Duration::from_millis(500), event_rx.recv())
            .await
            .expect("timed out")
            .expect("channel closed");

        assert_eq!(event.case_id, "case-xes");
        assert_eq!(event.name().unwrap_or(""), "event-a");

        let _ = std::fs::remove_file(socket_path);
    }

    /// Closing the connection immediately (zero bytes) must not panic.
    #[tokio::test]
    async fn empty_connection_does_not_panic() {
        let socket_path = "/tmp/test_eih_empty.sock";
        let _ = std::fs::remove_file(socket_path);

        let listener = UnixListener::bind(socket_path).unwrap();
        let (event_tx, _rx) = mpsc::channel(16);

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_event(stream, event_tx).await;
        });

        let client = tokio::net::UnixStream::connect(socket_path).await.unwrap();
        drop(client);

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let _ = std::fs::remove_file(socket_path);
    }

    /// Multiple traces in a single XES document each yield one event.
    #[tokio::test]
    async fn multiple_traces_produce_multiple_events() {
        let socket_path = "/tmp/test_eih_multi.sock";
        let _ = std::fs::remove_file(socket_path);

        let listener = UnixListener::bind(socket_path).unwrap();
        let (event_tx, mut event_rx) = mpsc::channel(32);

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_event(stream, event_tx).await;
        });

        let xes = r#"<?xml version="1.0" encoding="UTF-8" ?>
<log>
  <trace>
    <string key="concept:name" value="case-A"/>
    <event>
      <string key="concept:name" value="e1"/>
      <date key="time:timestamp" value="2024-01-01T00:00:00+00:00"/>
    </event>
  </trace>
  <trace>
    <string key="concept:name" value="case-B"/>
    <event>
      <string key="concept:name" value="e2"/>
      <date key="time:timestamp" value="2024-01-01T01:00:00+00:00"/>
    </event>
  </trace>
</log>"#;

        let mut client = tokio::net::UnixStream::connect(socket_path).await.unwrap();
        client.write_all(xes.as_bytes()).await.unwrap();
        drop(client);

        let mut events = Vec::new();
        for _ in 0..2 {
            let ev = tokio::time::timeout(std::time::Duration::from_millis(500), event_rx.recv())
                .await
                .expect("timed out")
                .expect("closed");
            events.push(ev);
        }

        let case_ids: Vec<&str> = events.iter().map(|e| e.case_id.as_str()).collect();
        assert!(case_ids.contains(&"case-A"));
        assert!(case_ids.contains(&"case-B"));

        let _ = std::fs::remove_file(socket_path);
    }

    /// Events within a trace are sorted by timestamp before being enqueued.
    #[tokio::test]
    async fn events_are_sorted_by_timestamp() {
        let socket_path = "/tmp/test_eih_sort.sock";
        let _ = std::fs::remove_file(socket_path);

        let listener = UnixListener::bind(socket_path).unwrap();
        let (event_tx, mut event_rx) = mpsc::channel(32);

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_event(stream, event_tx).await;
        });

        // Two events in reverse timestamp order.
        let xes = r#"<?xml version="1.0" encoding="UTF-8" ?>
<log>
  <trace>
    <string key="concept:name" value="case-sort"/>
    <event>
      <string key="concept:name" value="second"/>
      <date key="time:timestamp" value="2024-01-01T02:00:00+00:00"/>
    </event>
    <event>
      <string key="concept:name" value="first"/>
      <date key="time:timestamp" value="2024-01-01T01:00:00+00:00"/>
    </event>
  </trace>
</log>"#;

        let mut client = tokio::net::UnixStream::connect(socket_path).await.unwrap();
        client.write_all(xes.as_bytes()).await.unwrap();
        drop(client);

        let first = tokio::time::timeout(std::time::Duration::from_millis(500), event_rx.recv())
            .await
            .expect("timed out")
            .expect("closed");

        let second = tokio::time::timeout(std::time::Duration::from_millis(500), event_rx.recv())
            .await
            .expect("timed out")
            .expect("closed");

        assert_eq!(first.name().unwrap_or(""), "first");
        assert_eq!(second.name().unwrap_or(""), "second");

        let _ = std::fs::remove_file(socket_path);
    }
}
