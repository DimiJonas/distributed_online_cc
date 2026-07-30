//! set `CC_METRICS_DIR=<dir>` before starting the node
//!
//! one file per node is written to `<dir>/node-<node_id>.jsonl`
//!   
//! optionally set `CC_METRICS_MSGTRACE=0` to disable the per-message trace (the highest volume record type) while keeping everything else

use crate::{measurement::record::Record, types::PeerId};
use chrono::Utc;
use serde::Serialize;
use std::fs::{File, create_dir_all};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::OnceLock;
use std::sync::mpsc::{Sender, channel};
use std::time::Instant;

#[derive(Serialize)]
struct Line {
    node_id: PeerId,
    timestamp_monotonic_us: u64,
    timestamp_wallclock_us: i64,
    #[serde(flatten)]
    record: Record,
}

struct Recorder {
    node_id: PeerId,
    start_time: Instant,
    record_queue: Sender<Line>,
    message_trace: bool,
}

static RECORDER: OnceLock<Recorder> = OnceLock::new();

/// initialize measurement from the environment and spawn record writing os thread
pub fn spawn_record_writer(node_id: PeerId) {
    let dir = match std::env::var("CC_METRICS_DIR") {
        Ok(d) if !d.is_empty() => PathBuf::from(d),
        _ => {
            log::info!("measurement: CC_METRICS_DIR unset. metric recording disabled");
            return;
        }
    };

    if let Err(e) = create_dir_all(&dir) {
        log::error!("measurement: cannot create {}: {}", dir.display(), e);
        return;
    }

    let path = dir.join(format!("node-{node_id}.jsonl"));
    let file = match File::create(&path) {
        Ok(f) => f,
        Err(e) => {
            log::error!("measurement: cannot create {}: {}", path.display(), e);
            return;
        }
    };

    let message_trace = std::env::var("CC_METRICS_MSGTRACE")
        .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
        .unwrap_or(true);

    let (record_queue_sender, record_queue_receiver) = channel::<Line>();

    // os thread (not a tokio task) so file writes dont occupy a runtime worker and a slow disk does not stall the application
    // also writing still continues even if the application is overloaded
    std::thread::Builder::new()
        .name(format!("metrics-writer-{node_id}"))
        .spawn(move || {
            let mut writer = BufWriter::with_capacity(256 * 1024, file);
            let mut since_flush = 0usize;
            while let Ok(line) = record_queue_receiver.recv() {
                match serde_json::to_writer(&mut writer, &line) {
                    Ok(()) => {
                        let _ = writer.write_all(b"\n");
                    }
                    Err(e) => {
                        log::error!("measurement: serialize failed: {e}");
                        continue;
                    }
                }
                since_flush += 1;
                // buffering 512 lines until flush
                if since_flush >= 512 {
                    let _ = writer.flush();
                    since_flush = 0;
                }
            }
            // process is shutting down so write everything to disk
            let _ = writer.flush();
        })
        .expect("measurement: failed to spawn writer thread");

    let recorder = Recorder {
        node_id,
        start_time: Instant::now(),
        record_queue: record_queue_sender,
        message_trace,
    };

    if RECORDER.set(recorder).is_err() {
        log::warn!("measurement: already initialized");
        return;
    }
    log::info!(
        "measurement: recording to {} (msgtrace={})",
        path.display(),
        message_trace
    );
}

#[inline]
pub fn message_trace_enabled() -> bool {
    RECORDER.get().map(|r| r.message_trace).unwrap_or(false)
}

#[inline]
pub fn enabled() -> bool {
    RECORDER.get().is_some()
}

/// never blocks because its an unbounded channel
#[inline]
pub fn record(record: Record) {
    let Some(recorder) = RECORDER.get() else {
        return;
    };
    let line = Line {
        node_id: recorder.node_id,
        timestamp_monotonic_us: recorder.start_time.elapsed().as_micros() as u64,
        timestamp_wallclock_us: Utc::now().timestamp_micros(),
        record,
    };
    let _ = recorder.record_queue.send(line);
}
