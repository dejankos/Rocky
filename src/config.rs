use confy::ConfyError;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Serialize, Deserialize)]
pub struct ServiceConfig {}

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

pub fn load_db_config() -> Result<DbConfig, ConfyError> {
    confy::load_path(Path::new("./db_config.toml"))
}
