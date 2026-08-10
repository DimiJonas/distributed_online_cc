use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use crate::measurement::api::{event_input, timed};
use crate::network::message_types::PeerMessage;
use crate::network::tcp_router::send_to_conformance_peer;
use crate::state::app_state::AppState;
use crate::types::{AttributeKey, CaseId, EventId, TracedEvent};
use bytes::BytesMut;
use chrono::{DateTime, Utc};
use process_mining::core::event_data::case_centric::xes::{XESImportOptions, import_xes_slice};
use process_mining::core::event_data::case_centric::{
    AttributeValue, Event, Trace, XESEditableAttribute,
};
use tokio::io::AsyncReadExt;
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio::time::sleep;

pub async fn handle_event(
    mut stream: UnixStream,
    event_q: mpsc::Sender<TracedEvent>,
    configured_attributes: Arc<HashSet<String>>,
    configured_events: Arc<HashSet<EventId>>,
    state: AppState,
) {
    let mut read_buf = BytesMut::with_capacity(8 * 1024);

    loop {
        let bytes_read = match stream.read_buf(&mut read_buf).await {
            Ok(bytes_read) => bytes_read,
            Err(e) => {
                log::error!("event input handler: error reading from unix socket: {}", e);
                sleep(Duration::from_millis(5)).await;
                continue;
            }
        };
        if bytes_read == 0 {
            // full document received, or connection closed
            break;
        }

        let import_options: XESImportOptions = XESImportOptions {
            // list contains event name, timestamp (and the sensor constraint attributes if configured)
            ignore_event_attributes_except: Some((*configured_attributes).clone()),
            // auto-sort events within each trace by timestamp
            sort_events_with_timestamp_key: Some("time:timestamp".to_string()),
            // silence parse warnings in production
            verbose: false,
            ..XESImportOptions::default()
        };

        let (parsed, parse_us) = timed(|| import_xes_slice(&read_buf, false, import_options));
        let event_log = match parsed {
            Ok(event_log) => event_log,
            Err(e) => {
                log::error!("handle_event: error importing xes slice: {}", e);
                break;
            }
        };
        for trace in event_log.traces {
            let trace_id: CaseId = match get_trace_id(&trace) {
                Some(id) => id,
                None => {
                    log::error!(
                        "received xes event input, but trace/case id is missing. ignoring trace ({:?})",
                        trace
                    );
                    continue;
                }
            };

            for event in trace.events {
                if let Some(event_id) = get_event_id(&event) {
                    if configured_events.contains(&event_id) {
                        // build traced event
                        let timestamp = match get_timestamp_utc(&event) {
                            Some(event_time) => event_time,
                            None => {
                                log::warn!(
                                    "XES event did not contain a timestamp - using current time as event time"
                                );
                                Utc::now()
                            }
                        };
                        let attributes = get_attributes(&event);

                        let traced_event = TracedEvent::new(
                            trace_id.clone(),
                            event_id.clone(),
                            timestamp,
                            attributes,
                        );
                        log::debug!("handle_event: received event {:?}", traced_event);

                        // record event
                        event_input(
                            &traced_event.case_id,
                            &traced_event.event_id,
                            traced_event.instance_id,
                            parse_us,
                            state.config.forwarder.is_some(),
                        );

                        if let Some(forwarder_config) = &state.config.forwarder {
                            // forward the event to the central node
                            let message = PeerMessage::event(traced_event);
                            match send_to_conformance_peer(
                                forwarder_config.central_node_address,
                                message,
                                state.clone(),
                            )
                            .await
                            {
                                Err(e) => {
                                    log::error!(
                                        "handle_event: error sending the event to the central node: {}",
                                        e
                                    );
                                    continue;
                                }
                                Ok(_) => {
                                    log::info!(
                                        "handle_event: forwarded event '{}' to central node",
                                        event_id
                                    );
                                }
                            }
                        } else {
                            // put the event directly in the event q
                            if let Err(e) = event_q.send(traced_event).await {
                                log::error!(
                                    "handle_event: unable to send event into event queue: {}",
                                    e
                                );
                                continue;
                            }
                        }
                    } else {
                        log::warn!(
                            "no constraint was configured for '{}'. ignoring event",
                            event_id
                        );
                    }
                } else {
                    log::warn!(
                        "received event does not contain an event_id (concept:name). ignoring event {:?}",
                        event
                    );
                }
            }
        }
    }
}

fn get_trace_id(trace: &Trace) -> Option<String> {
    trace
        .attributes
        .get_by_key("concept:name")
        .and_then(|a| a.value.try_as_string())
        .cloned()
}

fn get_timestamp_utc(event: &Event) -> Option<DateTime<Utc>> {
    event
        .attributes
        .get_by_key("time:timestamp")
        .and_then(|a| a.value.try_as_date())
        .map(|dt| dt.with_timezone(&Utc))
}

fn get_event_id(event: &Event) -> Option<EventId> {
    event
        .attributes
        .get_by_key("concept:name")
        .and_then(|a| a.value.try_as_string())
        .cloned()
}

fn get_attributes(event: &Event) -> HashMap<AttributeKey, AttributeValue> {
    event
        .attributes
        .iter()
        .map(|a| (a.key.clone(), a.value.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use tokio::{io::AsyncWriteExt, net::UnixListener, sync::mpsc};

    use crate::conformance_checking::event_input_handler::handle_event;
    use crate::types::EventId;
    use crate::util::test_tools::{make_state, minimal_xes};

    fn no_filters() -> (Arc<HashSet<String>>, Arc<HashSet<EventId>>) {
        (Arc::new(HashSet::new()), Arc::new(HashSet::new()))
    }

    fn with_event_filter(events: &[&str]) -> (Arc<HashSet<String>>, Arc<HashSet<EventId>>) {
        let event_set = events.iter().map(|s| s.to_string()).collect();
        (default_attributes(), Arc::new(event_set))
    }

    fn default_attributes() -> Arc<HashSet<String>> {
        Arc::new(HashSet::from([
            "concept:name".to_string(),
            "time:timestamp".to_string(),
        ]))
    }

    /// A valid XES document produces a TracedEvent on the queue.
    #[tokio::test]
    async fn valid_xes_produces_event_on_queue() {
        let state = make_state(11000);
        let socket_path = "/tmp/test_eih_valid.sock";
        let _ = std::fs::remove_file(socket_path);

        let listener = UnixListener::bind(socket_path).unwrap();
        let (event_tx, mut event_rx) = mpsc::channel(16);
        let (filter_attributes, filter_events) = with_event_filter(&["event-a"]);

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_event(stream, event_tx, filter_attributes, filter_events, state).await;
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
        assert_eq!(event.event_id, "event-a");

        let _ = std::fs::remove_file(socket_path);
    }

    /// Closing the connection immediately (zero bytes) must not panic.
    #[tokio::test]
    async fn empty_connection_does_not_panic() {
        let state = make_state(11001);
        let socket_path = "/tmp/test_eih_empty.sock";
        let _ = std::fs::remove_file(socket_path);

        let listener = UnixListener::bind(socket_path).unwrap();
        let (event_tx, _rx) = mpsc::channel(16);
        let (filter_attributes, filter_events) = no_filters();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_event(stream, event_tx, filter_attributes, filter_events, state).await;
        });

        let client = tokio::net::UnixStream::connect(socket_path).await.unwrap();
        drop(client);

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let _ = std::fs::remove_file(socket_path);
    }

    /// Multiple traces in a single XES document each yield one event.
    #[tokio::test]
    async fn multiple_traces_produce_multiple_events() {
        let state = make_state(11001);
        let socket_path = "/tmp/test_eih_multi.sock";
        let _ = std::fs::remove_file(socket_path);

        let listener = UnixListener::bind(socket_path).unwrap();
        let (event_tx, mut event_rx) = mpsc::channel(32);
        let (filter_attributes, filter_events) = with_event_filter(&["e1", "e2"]);

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_event(stream, event_tx, filter_attributes, filter_events, state).await;
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
        let state = make_state(11001);
        let socket_path = "/tmp/test_eih_sort.sock";
        let _ = std::fs::remove_file(socket_path);

        let listener = UnixListener::bind(socket_path).unwrap();
        let (event_tx, mut event_rx) = mpsc::channel(32);
        let (filter_attributes, filter_events) = with_event_filter(&["first", "second"]);

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_event(stream, event_tx, filter_attributes, filter_events, state).await;
        });

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

        assert_eq!(first.event_id, "first");
        assert_eq!(second.event_id, "second");

        let _ = std::fs::remove_file(socket_path);
    }

    /// Events not in the filter are not enqueued.
    #[tokio::test]
    async fn filtered_out_events_are_not_enqueued() {
        let state = make_state(11001);
        let socket_path = "/tmp/test_eih_filter.sock";
        let _ = std::fs::remove_file(socket_path);

        let listener = UnixListener::bind(socket_path).unwrap();
        let (event_tx, mut event_rx) = mpsc::channel(16);
        // only "allowed-event" passes the filter
        let (filter_attributes, filter_events) = with_event_filter(&["allowed-event"]);

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_event(stream, event_tx, filter_attributes, filter_events, state).await;
        });

        let mut client = tokio::net::UnixStream::connect(socket_path).await.unwrap();
        client
            .write_all(minimal_xes("case-xes", "blocked-event").as_bytes())
            .await
            .unwrap();

        let result =
            tokio::time::timeout(std::time::Duration::from_millis(200), event_rx.recv()).await;

        assert!(result.is_err(), "expected timeout but got an event");

        drop(client);
        let _ = std::fs::remove_file(socket_path);
    }
}
