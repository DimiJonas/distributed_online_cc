// parse command line arguments
use clap::Parser;
use online_cc::config::node_config::NodeConfig;
use online_cc::util::logging::setup_logging;
use std::fs::create_dir_all;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

/// env var fallback for '--worker-threads' (set by run_simulation.py)
const ENV_WORKER_THREADS: &str = "CC_WORKER_THREADS";
/// env var fallback for '--max-blocking-threads'
const ENV_MAX_BLOCKING_THREADS: &str = "CC_MAX_BLOCKING_THREADS";

#[derive(Parser)]
struct Input {
    #[arg(short, long, default_value = "config.toml")]
    pub config_path: String,
    #[arg(short, long)]
    pub log_dir: Option<String>,

    /// tokio worker threads for this node (overrules 'CC_WORKER_THREADS')
    #[arg(short = 'w', long)]
    pub worker_threads: Option<usize>,

    /// upper bound for the blocking pool (used by the sqlite result sink)
    #[arg(long)]
    pub max_blocking_threads: Option<usize>,
}

fn log_dir_from_config_path(config_path: &str) -> Option<String> {
    let config_dir = Path::new(config_path).parent()?;
    if config_dir.file_name()?.to_str()? != "configs" {
        return None;
    }
    Some(
        config_dir
            .parent()?
            .join("logs")
            .to_string_lossy()
            .into_owned(),
    )
}

fn thread_count(flag: Option<usize>, env_var: &str) -> Option<usize> {
    if let Some(value) = flag {
        return Some(value.max(1));
    }
    match std::env::var(env_var) {
        Ok(raw) if !raw.trim().is_empty() => match raw.trim().parse::<usize>() {
            Ok(value) => Some(value.max(1)),
            Err(error) => {
                eprintln!("ignoring {env_var}={raw:?}: not a positive integer ({error})");
                None
            }
        },
        _ => None,
    }
}

fn default_worker_threads() -> usize {
    std::thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(1)
}

fn main() {
    let args = Input::parse();

    // the config is read before the runtime is built so the worker threads can carry the node id, which makes the per thread /proc samples attributable to a node
    let config: NodeConfig = NodeConfig::load_config(&args.config_path);
    let node_id = config.node_id;

    let worker_threads = thread_count(args.worker_threads, ENV_WORKER_THREADS);
    let max_blocking_threads = thread_count(args.max_blocking_threads, ENV_MAX_BLOCKING_THREADS);
    let effective_workers = worker_threads.unwrap_or_else(default_worker_threads);

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    builder.thread_name_fn(move || {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let index = NEXT.fetch_add(1, Ordering::Relaxed);
        format!("cc-n{node_id}-w{index}")
    });
    if let Some(count) = worker_threads {
        builder.worker_threads(count);
    }
    if let Some(count) = max_blocking_threads {
        builder.max_blocking_threads(count);
    }

    let runtime = builder.build().expect("failed to build the tokio runtime");

    runtime.block_on(async move {
        let log_dir = args
            .log_dir
            .clone()
            .or_else(|| log_dir_from_config_path(&args.config_path));
        if let Some(dir) = log_dir.as_ref() {
            create_dir_all(dir).expect("failed to create log directory");
        }

        setup_logging(&log_dir, &config).expect("failed to initialize logging");
        log::info!("starting node");
        log::info!(
            "tokio runtime: {} worker threads ({}), {} usable cpus{}",
            effective_workers,
            if worker_threads.is_some() {
                "configured"
            } else {
                "default: one per usable cpu"
            },
            default_worker_threads(),
            match max_blocking_threads {
                Some(count) => format!(", max {} blocking threads", count),
                None => String::new(),
            }
        );
        log::debug!("configuration: {:?}", config);
        online_cc::app::run::run(config).await;
    });
}
