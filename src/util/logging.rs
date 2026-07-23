use std::path::{Path, PathBuf};

use log::LevelFilter;

use crate::config::node_config::NodeConfig;

fn build_log_path(log_dir_arg: &Option<String>, config: &NodeConfig) -> PathBuf {
    // config owns the filename; --log-dir (if it exists) owns the directory
    match &log_dir_arg {
        Some(dir) => {
            let file_name = Path::new(&config.log_file)
                .file_name()
                .unwrap_or_else(|| std::ffi::OsStr::new("app.log"));
            Path::new(dir).join(file_name)
        }
        None => PathBuf::from(&config.log_file),
    }
}

fn ensure_log_dir_exists(log_path: &Path) {
    // ensure the directory exists so logging init can't fail on a missing path
    if let Some(parent) = log_path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent).expect("failed to create log directory");
    }
}

pub fn setup_logging(
    log_dir_arg: &Option<String>,
    config: &NodeConfig,
) -> Result<(), fern::InitError> {
    let log_path = build_log_path(log_dir_arg, config);
    ensure_log_dir_exists(&log_path);

    let log_file_str = log_path.to_string_lossy().into_owned();

    let format =
        |out: fern::FormatCallback, message: &std::fmt::Arguments, record: &log::Record| {
            out.finish(format_args!(
                "[{} {} {}] {}",
                chrono::Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.level(),  // error, warn, info, debug (trace)
                record.target(), // module path, e.g. "distributed_online_cc::network::tcp_channel"
                message
            ))
        };

    // read desired level from env var, default to Info
    // e.g. RUST_LOG=info cargo run
    // log levels, configurable independently:
    //   RUST_LOG      -> stdout level (default: Info)
    //   RUST_LOG_FILE -> log file level (default: same as RUST_LOG)
    let stdout_level = std::env::var("RUST_LOG")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(LevelFilter::Info);
    let file_level = std::env::var("RUST_LOG_FILE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(stdout_level);

    fern::Dispatch::new()
        .format(format)
        .chain(
            fern::Dispatch::new()
                .level(stdout_level)
                .chain(std::io::stdout()),
        )
        .chain(
            fern::Dispatch::new()
                .level(file_level)
                .chain(fern::log_file(log_file_str)?),
        )
        .apply()?;

    Ok(())
}
