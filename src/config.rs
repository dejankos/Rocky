use confy::ConfyError;
use rocksdb::{DBCompactionStyle, Options};
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
    max_open_files: i32,
    fsync: bool,
    bytes_per_sync: u64,
    disable_data_sync: bool,
    optimize_for_point_lookup: u64,
    num_shard_bits: i32,
    max_write_buffer_number: i32,
    write_buffer_size: usize,
    target_file_size_base: usize,
    min_write_buffer_number_to_merge: i32,
    level_zero_stop_writes_trigger: i32,
    level_zero_slowdown_writes_trigger: i32,
    compaction_style: String,
    max_background_compactions: i32,
    max_background_flushes: i32,
}

impl Default for DbConfig {
    fn default() -> Self {
        DbConfig {
            path: "./db".into(),
            max_open_files: -1,
            fsync: false,
            bytes_per_sync: 0,
            num_shard_bits: 6,
            optimize_for_point_lookup: 1024,
            disable_data_sync: false,
            max_write_buffer_number: 2,
            write_buffer_size: 0x4000000,
            target_file_size_base: 0x4000000,
            min_write_buffer_number_to_merge: 1,
            level_zero_stop_writes_trigger: 24,
            level_zero_slowdown_writes_trigger: 24,
            compaction_style: "Level".to_string(),
            max_background_compactions: 2,
            max_background_flushes: 2,
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

impl DbConfig {
    pub fn options(&self) -> Options {
        let mut opts = Options::default();
        opts.set_max_open_files(self.max_open_files);
        opts.set_use_fsync(self.fsync);
        opts.set_bytes_per_sync(self.bytes_per_sync);
        opts.optimize_for_point_lookup(self.optimize_for_point_lookup);
        opts.set_table_cache_num_shard_bits(self.num_shard_bits);
        opts.set_max_write_buffer_number(self.max_write_buffer_number);
        opts.set_write_buffer_size(self.write_buffer_size);
        opts.set_min_write_buffer_number_to_merge(self.min_write_buffer_number_to_merge);
        opts.set_level_zero_stop_writes_trigger(self.level_zero_stop_writes_trigger);
        opts.set_level_zero_slowdown_writes_trigger(self.level_zero_slowdown_writes_trigger);
        opts.set_compaction_style(get_compaction_style(&self.compaction_style));
        opts.set_max_background_compactions(self.max_background_compactions);
        opts.set_max_background_flushes(self.max_background_flushes);

        opts
    }
}

pub fn load_db_config() -> Result<DbConfig, ConfyError> {
    confy::load_path("./db_config.toml")
}

pub fn load_service_config() -> Result<ServiceConfig, ConfyError> {
    confy::load_path("./service_config.toml")
}

fn get_compaction_style(s: &str) -> DBCompactionStyle {
    match s.to_lowercase().as_str() {
        "level" => DBCompactionStyle::Level,
        "universal" => DBCompactionStyle::Universal,
        "fifo" => DBCompactionStyle::Fifo,
        _ => {
            error!(
                "Unknown compaction style {} - fallback to default {:?}",
                &s,
                DBCompactionStyle::Level
            );
            DBCompactionStyle::Level
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn load() {}
}
