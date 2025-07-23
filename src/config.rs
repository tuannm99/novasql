use serde::Deserialize;
use std::fs::File;
use std::io::Read;

#[derive(Debug, Deserialize)]
pub struct StorageConfig {
    pub mode: Option<String>,
    pub workdir: Option<String>,
    pub page_size: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    pub port: Option<u16>,
    pub debug: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub app_name: Option<String>,
    pub storage: Option<StorageConfig>,
    pub server: Option<ServerConfig>,
}

impl Config {
    pub fn from_yaml_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let config: Config = serde_yaml::from_str(&contents)?;
        Ok(config)
    }
}

