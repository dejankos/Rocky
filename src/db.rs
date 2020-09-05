use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::{fs, thread};

use actix_web::web::Bytes;
use crossbeam::sync::{ShardedLock, ShardedLockReadGuard, ShardedLockWriteGuard};
use executors::threadpool_executor::ThreadPoolExecutor;
use executors::Executor;
use rocksdb::{CompactionDecision, IteratorMode, Options, DB};
use serde::{Deserialize, Serialize};

use crate::config::DbConfig;
use crate::conversion::{bytes_to_str, current_ms, Conversion, FromBytes, IntoBytes};
use crate::errors::DbError;
use std::fmt::Debug;

const ROOT_DB_NAME: &str = "root";

type SafeRW<T> = Arc<ShardedLock<T>>;
type DbResult<T> = Result<T, DbError>;

#[derive(Clone)]
struct Db {
    rock: SafeRW<DB>,
}

#[derive(Serialize, Deserialize)]
pub struct Data {
    ttl: u128,
    data: Vec<u8>,
}

pub struct DbManager {
    db_cfg: DbConfig,
    root_db: Db,
    dbs: SafeRW<HashMap<String, Db>>,
    executor: Mutex<ThreadPoolExecutor>,
}

impl Data {
    pub fn new(ttl: u128, data: Vec<u8>) -> Self {
        Data { ttl, data }
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

    fn r_lock(&self) -> ShardedLockReadGuard<'_, DB> {
        self.rock.read().expect("Can't acquire read lock")
    }

    fn w_lock(&self) -> ShardedLockWriteGuard<'_, DB> {
        self.rock.write().expect("Can't acquire write lock")
    }
}

impl DbManager {
    pub fn new(db_cfg: DbConfig) -> DbResult<Self> {
        db_cfg
            .rocks_options()
            .set_compaction_filter("expiration-filter", compaction_filter);

        let root_db = open_root_db(&db_cfg)?;
        let db_manager = DbManager {
            db_cfg,
            root_db,
            dbs: Arc::new(ShardedLock::new(HashMap::new())),
            executor: Mutex::new(ThreadPoolExecutor::new(1)),
        };
        db_manager.init();

        Ok(db_manager)
    }

    // will panic in main thread and prevent startup
    fn init(&self) {
        info!("Initializing dbs from root ...");
        //TODO db iterator
        self.root_db
            .r_lock()
            .iterator(IteratorMode::Start)
            .map(|(k, v)| {
                (
                    bytes_to_str(&k).expect("Failed to read from db"),
                    bytes_to_str(&v).expect("Failed to read from db"),
                )
            })
            .for_each(|(name, path)| {
                info!("Initializing db = {} on path = {}", &name, &path);
                self.open_on_path(name, path).expect("Failed to open db");
            });
    }

    pub async fn open(&self, db_name: String) -> DbResult<()> {
        if self.contains(&db_name) {
            warn!("Db {} already exists", &db_name);
            Err(DbError::Validation(format!(
                "Database {} already exists",
                db_name
            )))
        } else {
            let path = format!("{}/{}", self.db_cfg.path(), db_name);
            info!("Opening Db = {} on path = {}", &db_name, &path);

            self.root_db.put(&db_name, &path)?;
            Ok(self.open_on_path(db_name, path)?)
        }
    }

    fn open_on_path(&self, db_name: String, path: String) -> DbResult<()> {
        let db = Db::new(&path, &self.db_cfg.rocks_options())?;
        self.w_lock().insert(db_name, db);
        Ok(())
    }

    pub async fn close(&self, db_name: String) -> DbResult<()> {
        if self.not_contains(&db_name) {
            Err(DbError::Validation(format!(
                "Can't close {} db - doesn't exist",
                &db_name
            )))
        } else {
            if let Some(db) = self.w_lock().remove(&db_name) {
                info!("Closing db = {} ...", &db_name);
                let path = self.db_cfg.db_path(&db_name);
                self.root_db.w_lock().delete(&db_name)?;
                //possible expensive call moved to separate thread TODO channels
                self.tp_mutex().execute(move || match db.close(&db_name) {
                    Ok(_) => {
                        info!("Db = {} closed. Deleting db files...", &db_name);
                        remove_files(path);
                    }
                    Err(e) => error!("Error closing db = {}, e = {}", &db_name, e),
                });
            }

            Ok(())
        }
    }

    pub async fn store(&self, db_name: &str, key: &str, val: Bytes, ttl: u128) -> DbResult<()> {
        let bytes = Data::new(ttl, val.to_vec()).as_bytes()?;
        match self.w_lock().get(db_name) {
            Some(db) => db.put(&key, bytes),
            None => Err(not_exists(db_name)),
        }
    }

    pub async fn read(&self, db_name: &str, key: &str) -> DbResult<Option<Vec<u8>>> {
        match self.r_lock().get(db_name) {
            Some(db) => {
                if let Some(bytes) = db.get(&key)? {
                    let data = bytes.as_struct()?;
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

    pub fn contains(&self, db_name: &str) -> bool {
        self.r_lock().contains_key(db_name)
    }

    fn not_contains(&self, db_name: &str) -> bool {
        !self.contains(db_name)
    }

    fn tp_mutex(&self) -> MutexGuard<'_, ThreadPoolExecutor> {
        self.executor
            .lock()
            .expect("Failed to acquire executor lock")
    }

    fn r_lock(&self) -> ShardedLockReadGuard<'_, HashMap<String, Db>> {
        self.dbs.read().expect("Can't acquire read lock")
    }

    fn w_lock(&self) -> ShardedLockWriteGuard<'_, HashMap<String, Db>> {
        self.dbs.write().expect("Can't acquire write lock")
    }
}

fn open_root_db(db_cfg: &DbConfig) -> DbResult<Db> {
    Db::new(db_cfg.db_path(ROOT_DB_NAME), &db_cfg.root_db_options())
}

fn not_exists(db_name: &str) -> DbError {
    DbError::Validation(format!("Db {} - doesn't exist", &db_name))
}

fn is_expired(ttl: u128) -> Conversion<bool> {
    if ttl == 0 {
        Ok(false)
    } else {
        Ok(ttl < current_ms()?)
    }
}

fn remove_files<P>(path: P)
where
    P: AsRef<Path> + Debug,
{
    if let Err(e) = fs::remove_dir_all(&path) {
        error!(
            "Failed to remove db files on path = {:?}, err = {}",
            path, e
        );
    } else {
        info!("Removed files for db on path {:?}", path);
    }
}

fn compaction_filter(_level: u32, _key: &[u8], value: &[u8]) -> CompactionDecision {
    info!(
        "Running compaction filter in thread {:?}",
        thread::current()
    );
    if let Ok(data) = value.to_vec().as_struct() {
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

#[cfg(test)]
mod tests {
    use super::*;

    const ONE_DAY_MS: u128 = 1000 * 60 * 60 * 24;

    #[test]
    fn should_remove_expired() {
        let bytes = Data::new(1, b"data".to_vec()).as_bytes().unwrap();

        match compaction_filter(0, &[0], &bytes) {
            CompactionDecision::Remove => {}
            _ => panic!("Should have removed expired record"),
        }
    }

    #[test]
    fn should_keep_not_expired() {
        let bytes = Data::new(current_ms().unwrap() + ONE_DAY_MS, b"data".to_vec())
            .as_bytes()
            .unwrap();

        match compaction_filter(0, &[0], &bytes) {
            CompactionDecision::Keep => {}
            _ => panic!("Should have kept non expired record"),
        }
    }
}
