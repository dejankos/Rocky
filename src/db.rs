use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;

use actix_web::web::Bytes;
use crossbeam::sync::{ShardedLock, ShardedLockReadGuard, ShardedLockWriteGuard};
use executors::Executor;
use executors::threadpool_executor::ThreadPoolExecutor;
use rocksdb::{CompactionDecision, DB, IteratorMode, Options};
use serde::{Deserialize, Serialize};

use crate::{Conversion, current_time_ms, PathCfg};
use crate::config::{DbConfig, load_db_config};
use crate::errors::DbError;

const ROOT_DB_NAME: &str = "root";

type SafeRW<T> = Arc<ShardedLock<T>>;
type DbResult<T> = Result<T, DbError>;

trait RWLock {
    type Item;

    fn r_lock(&self) -> ShardedLockReadGuard<'_, Self::Item>;
    fn w_lock(&self) -> ShardedLockWriteGuard<'_, Self::Item>;
}

#[derive(Clone)]
struct Db {
    rock: SafeRW<DB>,
}

#[derive(Serialize, Deserialize)]
struct Data {
    ttl: u128,
    data: Vec<u8>,
}

pub struct DbManager {
    config: DbConfig,
    root_db: Db,
    dbs: SafeRW<HashMap<String, Db>>,
    executor: Mutex<ThreadPoolExecutor>,
}

impl RWLock for Db {
    type Item = DB;

    fn r_lock(&self) -> ShardedLockReadGuard<'_, Self::Item> {
        self.rock.read().expect("Can't acquire read lock")
    }

    fn w_lock(&self) -> ShardedLockWriteGuard<'_, Self::Item> {
        self.rock.write().expect("Can't acquire write lock")
    }
}

impl Db {
    fn new<P>(path: P, opts: &Options) -> DbResult<Self>
        where
            P: AsRef<Path>,
    {
        let rock = DB::open(&opts, path)?;
        Ok(Db {
            rock: Arc::new(ShardedLock::new(rock)),
        })
    }

    fn put<V>(&self, key: &str, val: V) -> DbResult<()>
        where
            V: AsRef<[u8]>,
    {
        Ok(self.w_lock().put(key, val)?)
    }

    fn get(&self, key: &str) -> DbResult<Option<Vec<u8>>> {
        Ok(self.r_lock().get(key)?)
    }

    fn remove(&self, key: &str) -> DbResult<()> {
        self.w_lock().delete(key).map_err(DbError::from)
    }

    fn close<P>(&self, path: P) -> DbResult<()>
        where
            P: AsRef<Path>,
    {
        Ok(DB::destroy(&Options::default(), path)?)
    }
}

impl RWLock for DbManager {
    type Item = HashMap<String, Db>;

    fn r_lock(&self) -> ShardedLockReadGuard<'_, Self::Item> {
        self.dbs.read().expect("Can't acquire read lock")
    }

    fn w_lock(&self) -> ShardedLockWriteGuard<'_, Self::Item> {
        self.dbs.write().expect("Can't acquire write lock")
    }
}

impl DbManager {
    pub fn new(path_cfg: PathCfg) -> DbResult<Self> {
        let config = load_db_config(path_cfg.config_path.as_ref())?;
        config
            .rocks_options()
            .set_compaction_filter("expiration-filter", compaction_filter);
        info!("Using db config {:#?}", &config);
        info!("Opening root db...");
        let root_db = Db::new(
            format!("{}/{}", config.path(), ROOT_DB_NAME),
            &config.root_db_options(),
        )?;
        Ok(DbManager {
            config,
            root_db,
            dbs: Arc::new(ShardedLock::new(HashMap::new())),
            executor: Mutex::new(ThreadPoolExecutor::new(1)),
        })
    }

    pub fn init(&self) -> DbResult<()> {
        info!("Initializing dbs ...");
        //TODO db iterator
        self.root_db
            .r_lock()
            .iterator(IteratorMode::Start)
            .map(|(k, v)| {
                (
                    String::from_utf8(k.to_vec()).expect("Failed to read from db"),
                    String::from_utf8(v.to_vec()).expect("Failed to read from db"),
                )
            })
            .for_each(|(name, path)| {
                info!("Initializing db = {} on path = {}", &name, &path);
                self.open_on_path(name, path).expect("Failed to open db");
            });

        Ok(())
    }

    pub async fn open(&self, db_name: String) -> DbResult<()> {
        if self.is_present(&db_name) {
            debug!("Db {} already exists", &db_name);
            Err(DbError::Validation(format!(
                "Database {} already exists",
                db_name
            )))
        } else {
            let path = format!("{}/{}", self.config.path(), db_name);
            info!("Opening Db = {} on path = {}", &db_name, &path);
            self.root_db.put(&db_name, &path)?;
            self.open_on_path(db_name, path)?;
            Ok(())
        }
    }

    fn open_on_path(&self, db_name: String, path: String) -> DbResult<()> {
        let db = Db::new(&path, &self.config.rocks_options())?;
        self.w_lock().insert(db_name, db);
        Ok(())
    }

    pub async fn close(&self, db_name: String) -> DbResult<()> {
        if self.not_present(&db_name) {
            Err(DbError::Validation(format!(
                "Can't close {} db - doesn't exist",
                &db_name
            )))
        } else {
            if let Some(db) = self.w_lock().remove(&db_name) {
                info!("Closing db = {} ...", &db_name);
                self.tp_mutex().execute(move || match db.close(&db_name) {
                    Ok(_) => info!("Db = {} closed", &db_name),
                    Err(e) => error!("Error closing db = {}, e = {}", &db_name, e),
                });
            }

            Ok(())
        }
    }

    pub async fn store(&self, db_name: &str, key: &str, val: Bytes, ttl: u128) -> DbResult<()> {
        let bytes = serialize(val, ttl)?;
        match self.w_lock().get(db_name) {
            Some(db) => db.put(&key, bytes),
            None => Err(not_exists(db_name)),
        }
    }

    pub async fn read(&self, db_name: &str, key: &str) -> DbResult<Option<Vec<u8>>> {
        match self.r_lock().get(db_name) {
            Some(db) => {
                if let Some(bytes) = db.get(&key)? {
                    let data = deserialize(bytes)?;
                    if is_expired(data.ttl)? {
                        self.expire(db, key);
                        Ok(None)
                    } else {
                        Ok(Some(data.data))
                    }
                } else {
                    Ok(None)
                }
            }
            None => Err(not_exists(db_name)),
        }
    }

    fn expire(&self, db: &Db, key: &str) {
        let db = db.clone();
        let key = key.to_string();
        self.tp_mutex().execute(move || {
            if let Err(e) = db.w_lock().delete(&key) {
                error!("Failed to expire key = {}, e = {}", key, e);
            }
        });
    }

    pub async fn remove(&self, db_name: &str, key: &str) -> DbResult<()> {
        match self.w_lock().get(db_name) {
            Some(db) => db.remove(&key),
            None => Err(not_exists(db_name)),
        }
    }

    fn is_present(&self, db_name: &str) -> bool {
        self.r_lock().contains_key(db_name)
    }

    fn not_present(&self, db_name: &str) -> bool {
        !self.is_present(db_name)
    }

    fn tp_mutex(&self) -> MutexGuard<'_, ThreadPoolExecutor> {
        self.executor
            .lock()
            .expect("Failed to acquire executor lock")
    }
}

fn not_exists(db_name: &str) -> DbError {
    DbError::Validation(format!("Db {} - doesn't exist", &db_name))
}

fn serialize(data: Bytes, ttl: u128) -> DbResult<Vec<u8>> {
    Ok(bincode::serialize(&Data {
        ttl,
        data: data.to_vec(),
    })?)
}

fn deserialize(data: Vec<u8>) -> DbResult<Data> {
    Ok(bincode::deserialize(&data)?)
}

fn is_expired(ttl: u128) -> Conversion<bool> {
    if ttl == 0 {
        Ok(false)
    } else {
        Ok(ttl < current_time_ms()?)
    }
}

fn compaction_filter(_level: u32, _key: &[u8], value: &[u8]) -> CompactionDecision {
    info!(
        "Running compaction filter in thread {:?}",
        thread::current()
    );
    if let Ok(data) = deserialize(value.to_vec()) {
        if let Ok(expired) = is_expired(data.ttl) {
            if expired {
                CompactionDecision::Remove
            } else {
                CompactionDecision::Keep
            }
        } else {
            CompactionDecision::Remove
        }
    } else {
        error!("Compaction job:: Can't deserialize record - will be discarded.");
        CompactionDecision::Remove
    }
}
