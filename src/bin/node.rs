// parse command line arguments
use clap::Parser;
use online_cc::config::node_config::NodeConfig;
use online_cc::util::logging::setup_logging;
use std::path::Path;

#[derive(Parser)]
struct Input {
    #[arg(short, long, default_value = "config.toml")]
    pub config_path: String,
    #[arg(short, long)]
    pub log_dir: Option<String>,
}

fn log_dir_from_config_path(config_path: &str) -> Option<String> {
    let dir = Path::new(config_path).parent()?;
    if dir.file_name()?.to_str()? != "configs" {
        return None;
    }
    Some(dir.parent()?.join("logs").to_string_lossy().into_owned())
}

#[tokio::main]
async fn main() {
    let args = Input::parse();
    let config: NodeConfig = NodeConfig::load_config(&args.config_path);

    let log_dir = args
        .log_dir
        .clone()
        .or_else(|| log_dir_from_config_path(&args.config_path));
    if let Some(dir) = log_dir.as_ref() {
        std::fs::create_dir_all(dir).expect("failed to create log directory");
    }

    setup_logging(&log_dir, &config).expect("failed to initialize logging");
    log::info!("starting node");
    log::debug!("configuration: {:?}", config);
    online_cc::app::run::run(config).await;
}
