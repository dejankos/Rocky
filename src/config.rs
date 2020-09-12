use confy::ConfyError;
use rocksdb::{BlockBasedIndexType, BlockBasedOptions, Cache, DBCompactionStyle, Options};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct ServiceConfig {
    ip: String,
    port: u16,
    workers: u8,
    dev_mode: bool,
}

#[derive(Debug)]
pub struct DbConfig(pub RocksDbConfig);

impl DbConfig {
    fn new(rocks_cfg: RocksDbConfig) -> Self {
        DbConfig(rocks_cfg)
    }

    pub fn rocks_options(&self) -> Options {
        self.0.options()
    }

    pub fn root_db_options(&self) -> Options {
        let mut opts = Options::default();
        opts.create_if_missing(true);

        opts
    }

    pub fn path(&self) -> &str {
        self.0.path.as_ref()
    }

    pub fn db_path(&self, db_name: &str) -> String {
        format!("{}/{}", self.path(), db_name)
    }
}

impl ServiceConfig {
    pub fn bind_address(&self) -> String {
        format!("{}:{}", self.ip, self.port)
    }

    pub fn dev_mode(&self) -> bool {
        self.dev_mode
    }

    pub fn workers(&self) -> usize {
        self.workers as usize
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RocksDbConfig {
    path: String,
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
    use_cache: bool,
    cache_defaults: bool,
    cache_size: usize,
    block_cache: usize,
    bloom_filter_bits_per_key: i32,
    bloom_filter_block_based: bool,
    index_type: String,
}

impl Default for RocksDbConfig {
    fn default() -> Self {
        RocksDbConfig {
            path: "./db".to_string(),
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
            use_cache: true,
            cache_defaults: true,
            cache_size: 100_000,
            block_cache: 10_000,
            bloom_filter_bits_per_key: 8,
            bloom_filter_block_based: true,
            index_type: "HashSearch".to_string(),
        }
    }
}

impl Default for ServiceConfig {
    fn default() -> Self {
        ServiceConfig {
            ip: "localhost".to_string(),
            port: 8080,
            workers: num_cpus::get() as u8,
            dev_mode: true,
        }
    }
}

impl RocksDbConfig {
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
        opts.create_if_missing(true);
        opts.set_block_based_table_factory(&block_based_opts(self));

        opts
    }
}

fn block_based_opts(cfg: &RocksDbConfig) -> BlockBasedOptions {
    let mut opts = BlockBasedOptions::default();
    if cfg.cache_defaults {
        opts
    } else if !cfg.use_cache {
        opts.disable_cache();
        opts
    } else {
        opts.set_block_cache(
            &Cache::new_lru_cache(cfg.cache_size).expect("Could not create Cache"),
        );
        opts.set_block_size(cfg.block_cache);
        opts.set_bloom_filter(cfg.bloom_filter_bits_per_key, cfg.bloom_filter_block_based);
        opts.set_index_type(get_index_type(&cfg.index_type));
        opts
    }
}

pub fn load_db_config(cfg_path: &str) -> Result<DbConfig, ConfyError> {
    let rocks_cfg = confy::load_path(format!("{}/db_config.toml", cfg_path))?;
    Ok(DbConfig::new(rocks_cfg))
}

pub fn load_service_config(cfg_path: &str) -> Result<ServiceConfig, ConfyError> {
    Ok(confy::load_path(format!(
        "{}/service_config.toml",
        cfg_path
    ))?)
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

fn get_index_type(s: &str) -> BlockBasedIndexType {
    match s.to_lowercase().as_str() {
        "hashsearch" => BlockBasedIndexType::HashSearch,
        "binarysearch" => BlockBasedIndexType::BinarySearch,
        "twolevelindexsearch" => BlockBasedIndexType::TwoLevelIndexSearch,
        _ => {
            error!(
                "Unknown block based index type {} - fallback to default {}",
                &s, "HashSearch"
            );
            BlockBasedIndexType::HashSearch
        }
    }
}
