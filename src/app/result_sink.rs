use std::{collections::VecDeque, mem::take, sync::Arc, time::Instant};

use crate::{
    config::node_config::ResultSinkConfig, measurement::api::result_persisted,
    network::message_types::CCResultMessage, state::result_revision_store::RevisionNumber,
};
use chrono::Utc;
use rusqlite::Connection;
use thiserror::Error;
use tokio::{sync::mpsc, time::MissedTickBehavior};

#[derive(Debug, Error)]
pub enum SinkError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("failed to open/create database at {path}: {source}")]
    Open {
        path: String,
        #[source]
        source: rusqlite::Error,
    },
}

impl SinkError {
    /// used to decide whether a result gets put in the retry buffer or is logged and dropped
    ///
    /// a transient error is supposed to be a temporary problem
    ///
    /// malformed data that goes against the db schema can never be written
    /// its a permanent problem and thus the data should be logged and dropped
    fn is_transient(&self) -> bool {
        match self {
            SinkError::Open { .. } => true,
            SinkError::Sqlite(rusqlite::Error::SqliteFailure(err, _)) => matches!(
                err.code,
                rusqlite::ErrorCode::DatabaseBusy
                    | rusqlite::ErrorCode::DatabaseLocked
                    | rusqlite::ErrorCode::CannotOpen
                    | rusqlite::ErrorCode::SystemIoFailure
            ),
            SinkError::Sqlite(_) => false,
        }
    }
}

pub fn init_db(config: Arc<ResultSinkConfig>) -> Result<Connection, SinkError> {
    let connection = Connection::open(&config.db_path).map_err(|e| SinkError::Open {
        path: config.db_path.clone(),
        source: e,
    })?;

    connection.execute_batch(include_str!("db_schema.sql"))?;
    Ok(connection)
}

/// internal signal to wake the main loop on a fixed interval when the database connection was lost
/// makes it possible to still receive results in the meantime and buffer them
enum SinkEvent {
    Batch,
    FlushTick,
    RetryTick,
    ChannelClosed,
}

/// spawn the result sink
///
/// a batch is written when either
///   * `max_batch_size` results have accumulated, or
///   * `flush_interval` elapsed since the batch was opened
pub fn spawn_result_sink(config: ResultSinkConfig, mut sink_rx: mpsc::Receiver<CCResultMessage>) {
    let config = Arc::new(config);
    let db_path = config.db_path.clone();
    let retry_buffer_size = config.retry_buffer_size;
    let max_batch_size = config.max_batch_size.max(1);

    tokio::spawn(async move {
        log::info!("result sink configured. writing results to {}", db_path,);

        let mut batch: Vec<CCResultMessage> = Vec::with_capacity(max_batch_size);
        // results that could not be written because the db was unavailable
        let mut buffer: VecDeque<CCResultMessage> = VecDeque::new();

        let mut retry_interval = tokio::time::interval(config.retry_interval);
        retry_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut flush_interval = tokio::time::interval(config.flush_interval);
        flush_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        // inital database connect
        log::info!("connecting to database");
        let mut connection_container: Option<Connection> = try_connect(config.clone()).await;

        loop {
            let event = tokio::select! {
                received = sink_rx.recv_many(&mut batch, max_batch_size) => {
                    if received == 0 { SinkEvent::ChannelClosed } else { SinkEvent::Batch }
                },
                _ = flush_interval.tick() => SinkEvent::FlushTick,
                _ = retry_interval.tick() => SinkEvent::RetryTick,
            };

            match event {
                SinkEvent::ChannelClosed => {
                    log::warn!(
                        "constraint result channel closed. sink is flushing and shutting down"
                    );
                    if let Some(connection) = connection_container.take() {
                        let (_, returned_connection) =
                            write_batch(connection, &mut batch, &mut buffer, retry_buffer_size)
                                .await;
                        let (_, _) = flush_buffer(returned_connection, &mut buffer).await;
                    }
                    if !batch.is_empty() || !buffer.is_empty() {
                        log::error!(
                            "sink shutting down with {} unwritten result(s)",
                            batch.len() + buffer.len()
                        );
                    }
                    break;
                }

                SinkEvent::Batch => {
                    log::debug!("result sink: {} result(s) in the open batch", batch.len());
                    // only write when the batch is full - partial batches are written through the flush interval
                    if batch.len() < max_batch_size {
                        continue;
                    }
                    match connection_container.take() {
                        Some(connection) => {
                            let (_, returned_connection) =
                                write_batch(connection, &mut batch, &mut buffer, retry_buffer_size)
                                    .await;
                            connection_container = Some(returned_connection);
                        }
                        None => {
                            log::debug!("no db connection, buffering {} result(s)", batch.len());
                            for msg in batch.drain(..) {
                                enqueue_buffered(&mut buffer, msg, retry_buffer_size);
                            }
                        }
                    }
                }

                SinkEvent::FlushTick => {
                    if batch.is_empty() {
                        continue;
                    }
                    match connection_container.take() {
                        Some(c) => {
                            let (_, returned_connection) =
                                write_batch(c, &mut batch, &mut buffer, retry_buffer_size).await;
                            connection_container = Some(returned_connection);
                        }
                        None => {
                            for msg in batch.drain(..) {
                                enqueue_buffered(&mut buffer, msg, retry_buffer_size);
                            }
                        }
                    }
                }

                SinkEvent::RetryTick => {
                    if connection_container.is_none() {
                        log::info!("retrying to connect to database");
                        // sql connection is blocking IO
                        connection_container = try_connect(config.clone()).await;
                        if connection_container.is_none() {
                            continue;
                        }
                    }

                    if let Some(c) = connection_container.take() {
                        let (flush_result, returned_connection) =
                            flush_buffer(c, &mut buffer).await;
                        connection_container = Some(returned_connection);
                        if let Err(e) = flush_result {
                            log::warn!("buffer flush interrupted by db error: {}", e);
                            connection_container = None;
                        }
                    }
                }
            }
        }
    });
}

async fn try_connect(config: Arc<ResultSinkConfig>) -> Option<Connection> {
    let mut connection_container = None;

    // sql connection is blocking IO
    match tokio::task::spawn_blocking({
        let config = config.clone();
        move || init_db(config.clone())
    })
    .await
    {
        Ok(Ok(new_connection)) => {
            log::info!("db connection (re)established");
            connection_container = Some(new_connection);
        }
        Ok(Err(e)) => {
            log::warn!("db still unavailable, will retry: {}", e);
        }
        Err(join_err) => {
            log::error!("db init task panicked: {}", join_err);
        }
    }
    connection_container
}

/// write the open batch in one transaction on a blocking thread and hand the Connection back
/// on a transient error the whole batch goes into the retry buffer and the
async fn write_batch(
    connection: Connection,
    batch: &mut Vec<CCResultMessage>,
    buffer: &mut VecDeque<CCResultMessage>,
    retry_buffer_size: usize,
) -> (Result<(), SinkError>, Connection) {
    if batch.is_empty() {
        return (Ok(()), connection);
    }
    let pending: Vec<CCResultMessage> = take(batch);
    let size = pending.len();
    let started = Instant::now();

    let (task_result, unwritten, connection) = tokio::task::spawn_blocking(move || {
        let insert_result = insert_results(&connection, &pending);
        match insert_result {
            Ok(()) => (Ok(()), Vec::new(), connection),
            Err(e) => (Err(e), pending, connection),
        }
    })
    .await
    .expect("sink db task panicked");

    match &task_result {
        Ok(()) => log::debug!(
            "result sink: wrote batch of {} result(s) in {:?}",
            size,
            started.elapsed()
        ),
        Err(e) if e.is_transient() => {
            log::warn!(
                "db write failed ({}), buffering {} result(s) and dropping connection",
                e,
                size
            );
            for msg in unwritten {
                enqueue_buffered(buffer, msg, retry_buffer_size);
            }
        }
        Err(e) => {
            log::error!(
                "permanent error writing a batch of {} result(s), dropping it: {}",
                size,
                e
            );
        }
    }

    (task_result, connection)
}

/// drains the retry buffer into the database in batches and returns the Connection
async fn flush_buffer(
    connection: Connection,
    buffer: &mut VecDeque<CCResultMessage>,
) -> (Result<(), SinkError>, Connection) {
    if buffer.is_empty() {
        return (Ok(()), connection);
    }

    log::info!("flushing {} buffered results to db", buffer.len());
    let pending: Vec<CCResultMessage> = buffer.drain(..).collect();

    let (result, remaining, connection) = tokio::task::spawn_blocking(move || {
        match insert_results(&connection, &pending) {
            Ok(()) => (None, VecDeque::new(), connection),
            Err(e) => {
                log::error!("failed to flush buffered results: {e}");
                if e.is_transient() {
                    // put the results back since the write failed
                    (Some(e), pending.into_iter().collect(), connection)
                } else {
                    // schema rejects them and always will - drop
                    (None, VecDeque::new(), connection)
                }
            }
        }
    })
    .await
    .expect("flush task panicked");

    // put left over results back into the buffer
    for msg in remaining.into_iter().rev() {
        buffer.push_front(msg);
    }

    (result.map_or(Ok(()), Err), connection)
}

/// buffers a result
/// drops the oldest buffered result if the buffer is full
fn enqueue_buffered(buffer: &mut VecDeque<CCResultMessage>, msg: CCResultMessage, max_size: usize) {
    if buffer.len() >= max_size {
        log::warn!("result buffer full ({max_size}), dropping oldest buffered result");
        buffer.pop_front();
    }
    buffer.push_back(msg);
    log::debug!("{} results buffered, awaiting db connection", buffer.len());
}

/// inserts a whole batch of results and updates case and constraint tables in one transaction
fn insert_results(connection: &Connection, results: &[CCResultMessage]) -> Result<(), SinkError> {
    if results.is_empty() {
        return Ok(());
    }

    let tx = connection.unchecked_transaction()?;
    {
        let mut insert_case = tx.prepare_cached(
            "INSERT OR IGNORE INTO cases (case_id, first_seen_at) VALUES (?1, ?2)",
        )?;
        let mut insert_constraint = tx.prepare_cached(
            "INSERT OR IGNORE INTO constraints (constraint_id, constraint_type, first_seen_at) VALUES (?1, ?2, ?3)",
        )?;
        let mut insert_result = tx.prepare_cached(
            "INSERT INTO results (case_id, constraint_id, result, info, revision, created_at, inserted_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        )?;

        let inserted_at = Utc::now().to_rfc3339();

        for result_message in results {
            let created_at = result_message.created_at.to_rfc3339();

            insert_case.execute(rusqlite::params![result_message.case_id, created_at])?;
            insert_constraint.execute(rusqlite::params![
                result_message.constraint_id,
                result_message.constraint_type.as_db_str(),
                created_at
            ])?;
            insert_result.execute(rusqlite::params![
                result_message.case_id,
                result_message.constraint_id,
                result_message.result.as_db_str(),
                result_message.info,
                result_message.revision,
                created_at,
                inserted_at,
            ])?;
        }
    }
    tx.commit()?;

    // record persisted results
    for result_message in results {
        result_persisted(
            &result_message.case_id,
            &result_message.constraint_id,
            &result_message.created_at.to_rfc3339(),
        );
    }
    Ok(())
}

// --------------------------------------------
// Retrieval methods for testing purposes only
// --------------------------------------------

#[derive(Debug, Clone)]
pub struct ResultRow {
    pub case_id: String,
    pub constraint_id: String,
    pub constraint_type: String,
    pub result: String,
    pub info: String,
    pub revision: RevisionNumber,
    pub created_at: String,
}

/// retrieve all results for a case
/// results are ordered by timestamp
pub fn get_results_for_case(
    connection: &Connection,
    case_id: &str,
) -> Result<Vec<ResultRow>, SinkError> {
    let mut statement = connection.prepare(
        "SELECT case_id, constraint_id, constraint_type, result, info, revision, created_at
         FROM results
         WHERE case_id = ?1
         ORDER BY constraint_id, revision",
    )?;

    let rows = statement
        .query_map(rusqlite::params![case_id], |row| {
            Ok(ResultRow {
                case_id: row.get(0)?,
                constraint_id: row.get(1)?,
                constraint_type: row.get(2)?,
                result: row.get(3)?,
                info: row.get(4)?,
                revision: row.get(5)?,
                created_at: row.get(6)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

    Ok(rows)
}

/// retrieve all results for one specific case + constraint pair ordered by timestamp
pub fn get_results_for_case_and_constraint(
    connction: &Connection,
    case_id: &str,
    constraint_id: &str,
) -> Result<Vec<ResultRow>, SinkError> {
    let mut statement = connction.prepare(
        "SELECT case_id, constraint_id, constraint_type, result, info, revision, created_at
         FROM results
         WHERE case_id = ?1 AND constraint_id = ?2
         ORDER BY revision",
    )?;

    let rows = statement
        .query_map(rusqlite::params![case_id, constraint_id], |row| {
            Ok(ResultRow {
                case_id: row.get(0)?,
                constraint_id: row.get(1)?,
                constraint_type: row.get(2)?,
                result: row.get(3)?,
                info: row.get(4)?,
                revision: row.get(5)?,
                created_at: row.get(6)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

    Ok(rows)
}

/// retrieve the final revision of every (case, constraint) pair
///
/// this is the query the comparison tooling should use: ranking by `revision`
/// is the only ordering that reflects the order in which the emitting node
/// decided the revisions
pub fn get_final_results(connection: &Connection) -> Result<Vec<ResultRow>, SinkError> {
    let mut statement = connection.prepare(
        "SELECT r.case_id, r.constraint_id, r.constraint_type, r.result, r.info, r.revision, r.created_at
         FROM results r
         JOIN (
             SELECT case_id, constraint_id, MAX(revision) AS revision
             FROM results
             GROUP BY case_id, constraint_id
         ) final
           ON final.case_id = r.case_id
          AND final.constraint_id = r.constraint_id
          AND final.revision = r.revision
         ORDER BY r.case_id, r.constraint_id",
    )?;

    let rows = statement
        .query_map([], |row| {
            Ok(ResultRow {
                case_id: row.get(0)?,
                constraint_id: row.get(1)?,
                constraint_type: row.get(2)?,
                result: row.get(3)?,
                info: row.get(4)?,
                revision: row.get(5)?,
                created_at: row.get(6)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

    Ok(rows)
}
