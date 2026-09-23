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
use std::sync::mpsc::{RecvTimeoutError, Sender, SyncSender, channel, sync_channel};
use std::time::{Duration, Instant};

/// how long the writer may sit on buffered bytes before flushing them to disk
const FLUSH_INTERVAL: Duration = Duration::from_millis(500);

/// how long 'shutdown_record_writer' signal waits for the writer to confirm
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Serialize)]
struct Line {
    node_id: PeerId,
    /// microseconds since node start
    timestamp_monotonic_us: u64,
    /// current wallclock time in microseconds
    timestamp_wallclock_us: i64,
    #[serde(flatten)]
    record: Record,
}

enum WriterMessage {
    Line(Box<Line>),
    Shutdown(SyncSender<()>),
}

struct Recorder {
    node_id: PeerId,
    start_time: Instant,
    record_queue: Sender<WriterMessage>,
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

    let (record_queue_sender, record_queue_receiver) = channel::<WriterMessage>();

    // os thread (not a tokio task) so file writes dont occupy a runtime worker and a slow disk does not stall the application
    // also writing still continues even if the application is overloaded
    std::thread::Builder::new()
        .name(format!("metrics-writer-{node_id}"))
        .spawn(move || {
            let mut writer = BufWriter::with_capacity(256 * 1024, file);
            // true when there are buffered bytes that have not been written to file yet
            let mut dirty = false;

            loop {
                match record_queue_receiver.recv_timeout(FLUSH_INTERVAL) {
                    Ok(WriterMessage::Line(line)) => {
                        // serialize into memory first to handle lines with serialization errors (they will not be written)
                        match serde_json::to_vec(&line) {
                            Ok(mut bytes) => {
                                bytes.push(b'\n');
                                let _ = writer.write_all(&bytes);
                                dirty = true;
                            }
                            Err(e) => {
                                log::error!("measurement: serialize failed: {e}");
                                continue;
                            }
                        }
                    }
                    Ok(WriterMessage::Shutdown(ack)) => {
                        if let Err(e) = writer.flush() {
                            log::error!("measurement: final flush failed: {e}");
                        }
                        // fsync so the file survives a container teardown
                        let _ = writer.get_ref().sync_all();
                        let _ = ack.send(());
                        return;
                    }
                    Err(RecvTimeoutError::Timeout) => {
                        if dirty {
                            let _ = writer.flush();
                            dirty = false;
                        }
                    }
                    Err(RecvTimeoutError::Disconnected) => break,
                }
            }

            let _ = writer.flush();
            let _ = writer.get_ref().sync_all();
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

/// flush every buffered record and stop the writer thread
pub fn shutdown_record_writer() {
    let Some(recorder) = RECORDER.get() else {
        return;
    };

    // record the shutdown so the analysis can verify the file is complete
    record(Record::WriterShutdown);

    let (ack_sender, ack_receiver) = sync_channel::<()>(1);
    if recorder
        .record_queue
        .send(WriterMessage::Shutdown(ack_sender))
        .is_err()
    {
        log::warn!("measurement: writer thread already gone, records may be incomplete");
        return;
    }

    match ack_receiver.recv_timeout(SHUTDOWN_TIMEOUT) {
        Ok(()) => log::info!("measurement: records flushed"),
        Err(e) => log::error!("measurement: writer did not confirm the final flush: {e}"),
    }
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
    let _ = recorder
        .record_queue
        .send(WriterMessage::Line(Box::new(line)));
}
