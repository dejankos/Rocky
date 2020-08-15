use std::path::Path;

use confy::ConfyError;
use serde::{Deserialize, Serialize};


#[derive(Serialize, Deserialize, Debug)]
pub struct ServiceConfig {
    ip: String,
    port: u16,
    workers: u8,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DbConfig {
    pub path: String,
}

impl Default for DbConfig {
    fn default() -> Self {
        DbConfig {
            path: "./db".into(),
        }
    }
}

impl Default for ServiceConfig {
    fn default() -> Self {
        ServiceConfig {
            ip: "localhost".to_string(),
            port: 8080,
            workers: num_cpus::get() as u8,
        }
    }
}

pub fn load_db_config() -> Result<DbConfig, ConfyError> {
    confy::load_path(Path::new("./db_config.toml"))
}

pub fn load_service_config() -> Result<ServiceConfig, ConfyError> {
    confy::load_path(Path::new("./service_config.toml"))
}

