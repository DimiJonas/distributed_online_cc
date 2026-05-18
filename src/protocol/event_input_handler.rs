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
