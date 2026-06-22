use std::{collections::VecDeque, sync::Arc};

use crate::{config::node_config::ResultSinkConfig, network::message_types::CCResultMessage};
use rusqlite::Connection;
use thiserror::Error;
use tokio::{sync::mpsc, time::sleep};

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
    let conn = Connection::open(&config.db_path).map_err(|e| SinkError::Open {
        path: config.db_path.clone(),
        source: e,
    })?;

    conn.execute_batch(include_str!("db_schema.sql"))?;
    Ok(conn)
}

/// internal signal to wake the main loop on a fixed interval when the database connection was lost
/// makes it possible to still receive results in the meantime and buffer them
enum SinkEvent {
    Result(CCResultMessage),
    RetryTick,
    ChannelClosed,
}

pub fn spawn_result_sink(config: ResultSinkConfig, mut sink_rx: mpsc::Receiver<CCResultMessage>) {
    let config = Arc::new(config);
    let db_path = config.db_path.clone();
    let retry_interval = config.retry_interval;
    let retry_buffer_size = config.retry_buffer_size;

    tokio::spawn(async move {
        log::info!("result sink configured. writing results to {}", db_path);

        let mut buffer: VecDeque<CCResultMessage> = VecDeque::new();
        // inital database connect
        log::info!("connecting to database");
        let mut conn: Option<Connection> = try_connect(config.clone()).await;

        loop {
            let event = tokio::select! {
                msg = sink_rx.recv() => match msg {
                    Some(m) => SinkEvent::Result(m),
                    None => SinkEvent::ChannelClosed,
                },
                _ = sleep(retry_interval) => SinkEvent::RetryTick,
            };

            match event {
                SinkEvent::ChannelClosed => {
                    log::warn!("constraint result channel closed, sink shutting down");
                    break;
                }
                SinkEvent::Result(result_message) => {
                    log::info!(
                        "received result message for {}/{}",
                        result_message.case_id,
                        result_message.constraint_id
                    );
                    log::debug!("result message ({:?})", result_message);

                    match conn.take() {
                        Some(c) => {
                            let (result, returned_conn) =
                                write_one(c, result_message.clone()).await;
                            match result {
                                Ok(()) => conn = Some(returned_conn),
                                Err(e) if e.is_transient() => {
                                    log::warn!(
                                        "db write failed ({e}), buffering result and dropping connection"
                                    );
                                    enqueue_buffered(
                                        &mut buffer,
                                        result_message,
                                        retry_buffer_size,
                                    );
                                }
                                Err(e) => {
                                    log::error!(
                                        "permanent error writing result {:?}, dropping it: {e}",
                                        result_message
                                    );
                                    conn = Some(returned_conn);
                                }
                            }
                        }
                        None => {
                            log::debug!("no db connection, buffering result");
                            enqueue_buffered(&mut buffer, result_message, retry_buffer_size);
                        }
                    }
                }
                SinkEvent::RetryTick => {
                    if conn.is_none() {
                        log::info!("retrying to connect to database");
                        // sql connection is blocking IO
                        conn = try_connect(config.clone()).await;
                        if conn.is_none() {
                            continue;
                        }
                    }

                    if let Some(c) = conn.take() {
                        let (flush_result, returned_conn) = flush_buffer(c, &mut buffer).await;
                        conn = Some(returned_conn);
                        if let Err(e) = flush_result {
                            log::warn!("buffer flush interrupted by db error: {e}");
                            conn = None;
                        }
                    }
                }
            }
        }
    });
}

async fn try_connect(config: Arc<ResultSinkConfig>) -> Option<Connection> {
    let mut conn = None;

    // sql connection is blocking IO
    match tokio::task::spawn_blocking({
        let config = config.clone();
        move || init_db(config.clone())
    })
    .await
    {
        Ok(Ok(new_conn)) => {
            log::info!("db connection (re)established");
            conn = Some(new_conn);
        }
        Ok(Err(e)) => {
            log::warn!("db still unavailable, will retry: {e}");
        }
        Err(join_err) => {
            log::error!("db init task panicked: {join_err}");
        }
    }
    conn
}

/// runs one insert on a blocking thread and hands the Connection back
async fn write_one(
    conn: Connection,
    result_message: CCResultMessage,
) -> (Result<(), SinkError>, Connection) {
    log::info!(
        "writing result for {}/{} to database",
        result_message.case_id,
        result_message.constraint_id
    );
    tokio::task::spawn_blocking(move || {
        let res = insert_result(&conn, &result_message);
        (res, conn)
    })
    .await
    .expect("sink db task panicked")
}

/// drains as much of the buffer as possible into the database until the first error and returns the Connection
async fn flush_buffer(
    conn: Connection,
    buffer: &mut VecDeque<CCResultMessage>,
) -> (Result<(), SinkError>, Connection) {
    if buffer.is_empty() {
        return (Ok(()), conn);
    }

    log::info!("flushing {} buffered results to db", buffer.len());
    let pending: Vec<CCResultMessage> = buffer.drain(..).collect();

    let (result, remaining, conn) = tokio::task::spawn_blocking(move || {
        let mut remaining = VecDeque::new();
        let mut first_err: Option<SinkError> = None;

        let mut iter = pending.into_iter();
        for msg in &mut iter {
            if first_err.is_some() {
                remaining.push_back(msg);
                continue;
            }
            if let Err(e) = insert_result(&conn, &msg) {
                log::error!("failed to flush buffered result: {e}");
                let is_transient = e.is_transient();
                first_err = Some(e);
                if is_transient {
                    // put result back into buffer since it the write failed
                    remaining.push_back(msg);
                }
                if !is_transient {
                    first_err = None;
                }
            }
        }

        (first_err, remaining, conn)
    })
    .await
    .expect("flush task panicked");

    // put left over results back into the buffer
    for msg in remaining.into_iter().rev() {
        buffer.push_front(msg);
    }

    (result.map_or(Ok(()), Err), conn)
}

/// buffers a result
/// drops the oldest buffered result if the buffer is full
fn enqueue_buffered(buffer: &mut VecDeque<CCResultMessage>, msg: CCResultMessage, max_size: usize) {
    log::info!("enqueueing result in result buffer");
    if buffer.len() >= max_size {
        log::warn!("result buffer full ({max_size}), dropping oldest buffered result");
        buffer.pop_front();
    }
    buffer.push_back(msg);
    log::debug!("{} results buffered, awaiting db connection", buffer.len());
}

/// inserts one result and updates case and constraint tables in one transaction
fn insert_result(conn: &Connection, result_message: &CCResultMessage) -> Result<(), SinkError> {
    let created_at = result_message.created_at.to_rfc3339();

    let tx = conn.unchecked_transaction()?;

    // insert case into db
    tx.execute(
        "INSERT OR IGNORE INTO cases (case_id, first_seen_at) VALUES (?1, ?2)",
        rusqlite::params![result_message.case_id, created_at],
    )?;

    // insert constraint into db
    tx.execute(
        "INSERT OR IGNORE INTO constraints (constraint_id, constraint_type, first_seen_at) VALUES (?1, ?2, ?3)",
        rusqlite::params![
            result_message.constraint_id,
            result_message.constraint_type.to_string(),
            created_at
        ],
    )?;

    // insert result with info and timestamp into db
    tx.execute(
        "INSERT INTO results (case_id, constraint_id, constraint_type, result, info, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        rusqlite::params![
            result_message.case_id,
            result_message.constraint_id,
            result_message.constraint_type.to_string(),
            result_message.result.to_string(),
            result_message.info,
            created_at,
        ],
    )?;

    tx.commit()?;
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
    pub created_at: String,
}

/// retrieve all results for a case
/// results are ordered by timestamp
pub fn get_results_for_case(conn: &Connection, case_id: &str) -> Result<Vec<ResultRow>, SinkError> {
    let mut stmt = conn.prepare(
        "SELECT case_id, constraint_id, constraint_type, result, info, created_at
         FROM results
         WHERE case_id = ?1
         ORDER BY constraint_id, created_at",
    )?;

    let rows = stmt
        .query_map(rusqlite::params![case_id], |row| {
            Ok(ResultRow {
                case_id: row.get(0)?,
                constraint_id: row.get(1)?,
                constraint_type: row.get(2)?,
                result: row.get(3)?,
                info: row.get(4)?,
                created_at: row.get(5)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

    Ok(rows)
}

/// retrieve all results for one specific case + constraint pair ordered by timestamp
pub fn get_results_for_case_and_constraint(
    conn: &Connection,
    case_id: &str,
    constraint_id: &str,
) -> Result<Vec<ResultRow>, SinkError> {
    let mut stmt = conn.prepare(
        "SELECT case_id, constraint_id, constraint_type, result, info, created_at
         FROM results
         WHERE case_id = ?1 AND constraint_id = ?2
         ORDER BY created_at",
    )?;

    let rows = stmt
        .query_map(rusqlite::params![case_id, constraint_id], |row| {
            Ok(ResultRow {
                case_id: row.get(0)?,
                constraint_id: row.get(1)?,
                constraint_type: row.get(2)?,
                result: row.get(3)?,
                info: row.get(4)?,
                created_at: row.get(5)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

    Ok(rows)
}
