use crate::config::types::NodeConfig;
use std::fs;

pub fn load_config(path: &str) -> NodeConfig {
    let contents = fs::read_to_string(path).expect("could not read config file");
    toml::from_str(&contents).expect("could not parse config file")
}
