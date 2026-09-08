use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::sync::Arc;

use crate::measurement;
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

/// framed mode uses a length prefix of this size (bytes)
const LENGTH_PREFIX: usize = 4;

/// cap max accepted fram at 16 MiB
const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

/// set once per connection if the input is framed (multiple XES files per connection) or of its a oneshot connection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WireMode {
    /// one xes document per connection - raw XES
    Raw,
    /// many xes documents per connection, each preceded by a 4 byte (big endian) length
    Framed,
}

impl Display for WireMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WireMode::Raw => write!(f, "Raw"),
            WireMode::Framed => write!(f, "Framed"),
        }
    }
}

/// decide the wire mode from the first bytes of a connection
///
/// returns 'None' while there are not enough bytes to decide yet
fn detect_wire_mode(buffer: &[u8]) -> Option<WireMode> {
    if buffer.len() < LENGTH_PREFIX {
        return None;
    }
    if buffer[0] == 0 {
        let length = u32::from_be_bytes(
            buffer[..LENGTH_PREFIX]
                .try_into()
                .expect("slice is LENGTH_PREFIX long"),
        ) as usize;
        if length > 0 && length <= MAX_FRAME_BYTES {
            return Some(WireMode::Framed);
        }
    }
    Some(WireMode::Raw)
}

pub async fn handle_event(
    mut stream: UnixStream,
    event_q: mpsc::Sender<TracedEvent>,
    configured_attributes: Arc<HashSet<String>>,
    configured_events: Arc<HashSet<EventId>>,
    state: AppState,
) {
    let mut read_buf = BytesMut::with_capacity(64 * 1024);

    // read until the wire mode can be decided
    let mode = loop {
        if let Some(mode) = detect_wire_mode(&read_buf) {
            break mode;
        }
        match stream.read_buf(&mut read_buf).await {
            Ok(0) => {
                // closed before four bytes arrived
                if !read_buf.is_empty() {
                    process_document(
                        &read_buf,
                        &event_q,
                        &configured_attributes,
                        &configured_events,
                        state.clone(),
                    )
                    .await;
                }
                return;
            }
            Ok(_) => continue,
            Err(e) => {
                log::error!("event input handler: error reading from unix socket: {}", e);
                return;
            }
        }
    };
    log::debug!("event input handler: connection using {} mode", mode);

    match mode {
        WireMode::Raw => {
            // one document per connection: read to EOF, then parse once
            loop {
                match stream.read_buf(&mut read_buf).await {
                    Ok(0) => break,
                    Ok(_) => continue,
                    Err(e) => {
                        log::error!("event input handler: error reading from unix socket: {}", e);
                        return;
                    }
                }
            }
            process_document(
                &read_buf,
                &event_q,
                &configured_attributes,
                &configured_events,
                state.clone(),
            )
            .await;
        }

        WireMode::Framed => loop {
            // pull exactly one complete frame out of the buffer
            let document = loop {
                if read_buf.len() >= LENGTH_PREFIX {
                    let length = u32::from_be_bytes(
                        read_buf[..LENGTH_PREFIX]
                            .try_into()
                            .expect("slice is LENGTH_PREFIX long"),
                    ) as usize;

                    if length > MAX_FRAME_BYTES {
                        log::error!(
                            "event input handler: frame of {} bytes exceeds the {} byte limit. closing connection",
                            length,
                            MAX_FRAME_BYTES
                        );
                        return;
                    }
                    if read_buf.len() >= LENGTH_PREFIX + length {
                        let _ = read_buf.split_to(LENGTH_PREFIX);
                        break read_buf.split_to(length);
                    }
                }
                match stream.read_buf(&mut read_buf).await {
                    Ok(0) => {
                        if !read_buf.is_empty() {
                            log::warn!(
                                "event input handler: connection closed mid frame, discarding {} buffered bytes",
                                read_buf.len()
                            );
                        }
                        return;
                    }
                    Ok(_) => continue,
                    Err(e) => {
                        log::error!("event input handler: error reading from unix socket: {}", e);
                        return;
                    }
                }
            };

            process_document(
                &document,
                &event_q,
                &configured_attributes,
                &configured_events,
                state.clone(),
            )
            .await;
        },
    }
}

/// parse one complete xes document and process it
async fn process_document(
    document: &[u8],
    event_q: &mpsc::Sender<TracedEvent>,
    configured_attributes: &HashSet<String>,
    configured_events: &HashSet<EventId>,
    state: AppState,
) {
    if document.is_empty() {
        return;
    }

    let import_options: XESImportOptions = XESImportOptions {
        // list contains event name, timestamp (and the sensor constraint attributes if configured)
        ignore_event_attributes_except: Some(configured_attributes.clone()),
        // auto-sort events within each trace by timestamp
        sort_events_with_timestamp_key: Some("time:timestamp".to_string()),
        // silence parse warnings in production
        verbose: false,
        ..XESImportOptions::default()
    };

    let (parsed, parse_us) =
        measurement::api::timed(|| import_xes_slice(document, false, import_options));
    let event_log = match parsed {
        Ok(event_log) => event_log,
        Err(e) => {
            log::error!("process_document: error importing xes slice: {}", e);
            return;
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
            let Some(event_id) = get_event_id(&event) else {
                log::warn!(
                    "received event does not contain an event_id (concept:name). ignoring event {:?}",
                    event
                );
                continue;
            };

            if !configured_events.contains(&event_id) {
                log::warn!(
                    "no constraint was configured for '{}'. ignoring event",
                    event_id
                );
                continue;
            }

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

            let traced_event =
                TracedEvent::new(trace_id.clone(), event_id.clone(), timestamp, attributes);
            log::debug!("process_document: received event {}", traced_event);

            // record event
            measurement::api::event_input(
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
                            "process_document: error sending the event to the central node: {}",
                            e
                        );
                        continue;
                    }
                    Ok(_) => {
                        log::info!(
                            "process_document: forwarded event '{}' to central node",
                            event_id
                        );
                    }
                }
            } else {
                // put the event directly in the event q
                if let Err(e) = event_q.send(traced_event).await {
                    log::error!(
                        "process_document: unable to send event into event queue: {}",
                        e
                    );
                    continue;
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

    use crate::conformance_checking::event_input_handler::{
        WireMode, detect_wire_mode, handle_event,
    };
    use crate::types::EventId;
    use crate::util::test_tools::{make_state, minimal_xes};

    /// prefix a document the way a framed sender does
    fn frame(document: &str) -> Vec<u8> {
        let bytes = document.as_bytes();
        let mut out = (bytes.len() as u32).to_be_bytes().to_vec();
        out.extend_from_slice(bytes);
        out
    }

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

    #[test]
    fn sniff_needs_four_bytes() {
        assert_eq!(detect_wire_mode(b"<?x"), None);
    }

    #[test]
    fn sniff_detects_raw_xes() {
        assert_eq!(detect_wire_mode(b"<?xml version"), Some(WireMode::Raw));
        assert_eq!(detect_wire_mode(b"<log></log>"), Some(WireMode::Raw));
    }

    #[test]
    fn sniff_detects_a_length_prefix() {
        assert_eq!(detect_wire_mode(&frame("<log/>")), Some(WireMode::Framed));
    }

    #[test]
    fn sniff_rejects_an_implausible_length() {
        // zero high byte but an absurd length: not a frame we would ever send
        let bogus = [0x0Fu8, 0xFF, 0xFF, 0xFF];
        assert_eq!(detect_wire_mode(&bogus), Some(WireMode::Raw));
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

    /// A raw document that arrives in several chunks is still parsed once, as a whole.
    #[tokio::test]
    async fn split_raw_document_is_reassembled() {
        let state = make_state(11002);
        let socket_path = "/tmp/test_eih_split.sock";
        let _ = std::fs::remove_file(socket_path);

        let listener = UnixListener::bind(socket_path).unwrap();
        let (event_tx, mut event_rx) = mpsc::channel(16);
        let (filter_attributes, filter_events) = with_event_filter(&["event-a"]);

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_event(stream, event_tx, filter_attributes, filter_events, state).await;
        });

        let document = minimal_xes("case-split", "event-a");
        let (head, tail) = document.split_at(document.len() / 2);

        let mut client = tokio::net::UnixStream::connect(socket_path).await.unwrap();
        client.write_all(head.as_bytes()).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        client.write_all(tail.as_bytes()).await.unwrap();
        drop(client);

        let event = tokio::time::timeout(std::time::Duration::from_millis(500), event_rx.recv())
            .await
            .expect("timed out")
            .expect("channel closed");
        assert_eq!(event.case_id, "case-split");

        let _ = std::fs::remove_file(socket_path);
    }

    /// Several framed documents on ONE connection produce one event each, in order.
    #[tokio::test]
    async fn framed_documents_are_read_in_order() {
        let state = make_state(11003);
        let socket_path = "/tmp/test_eih_framed.sock";
        let _ = std::fs::remove_file(socket_path);

        let listener = UnixListener::bind(socket_path).unwrap();
        let (event_tx, mut event_rx) = mpsc::channel(16);
        let (filter_attributes, filter_events) = with_event_filter(&["event-a"]);

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_event(stream, event_tx, filter_attributes, filter_events, state).await;
        });

        let mut client = tokio::net::UnixStream::connect(socket_path).await.unwrap();
        for case in ["case-1", "case-2", "case-3"] {
            client
                .write_all(&frame(&minimal_xes(case, "event-a")))
                .await
                .unwrap();
        }

        for expected in ["case-1", "case-2", "case-3"] {
            let event =
                tokio::time::timeout(std::time::Duration::from_millis(500), event_rx.recv())
                    .await
                    .expect("timed out")
                    .expect("channel closed");
            assert_eq!(event.case_id, expected);
        }

        drop(client);
        let _ = std::fs::remove_file(socket_path);
    }

    /// A frame split across two writes is reassembled.
    #[tokio::test]
    async fn split_frame_is_reassembled() {
        let state = make_state(11004);
        let socket_path = "/tmp/test_eih_split_frame.sock";
        let _ = std::fs::remove_file(socket_path);

        let listener = UnixListener::bind(socket_path).unwrap();
        let (event_tx, mut event_rx) = mpsc::channel(16);
        let (filter_attributes, filter_events) = with_event_filter(&["event-a"]);

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_event(stream, event_tx, filter_attributes, filter_events, state).await;
        });

        let framed = frame(&minimal_xes("case-partial", "event-a"));
        let (head, tail) = framed.split_at(6);

        let mut client = tokio::net::UnixStream::connect(socket_path).await.unwrap();
        client.write_all(head).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        client.write_all(tail).await.unwrap();

        let event = tokio::time::timeout(std::time::Duration::from_millis(500), event_rx.recv())
            .await
            .expect("timed out")
            .expect("channel closed");
        assert_eq!(event.case_id, "case-partial");

        drop(client);
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
