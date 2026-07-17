// parse command line arguments
use clap::Parser;
use online_cc::config::node_config::NodeConfig;
use online_cc::util::logging::setup_logging;

#[derive(Parser)]
struct Input {
    #[arg(short, long, default_value = "config.toml")]
    pub config_path: String,
    #[arg(short, long)]
    pub log_dir: Option<String>,
}

#[tokio::main]
async fn main() {
    let args = Input::parse();
    let config: NodeConfig = NodeConfig::load_config(&args.config_path);

    setup_logging(&args.log_dir, &config).expect("failed to initialize logging");
    log::info!("starting node");
    log::debug!("configuration: {:?}", config);
    online_cc::app::run::run(config).await;
}
