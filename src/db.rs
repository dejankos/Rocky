use std::collections::HashMap;
use std::fmt::Display;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::{error, fmt};

use crossbeam::sync::{ShardedLock, ShardedLockReadGuard, ShardedLockWriteGuard};
use executors::threadpool_executor::ThreadPoolExecutor;
use executors::{Executor};
use rocksdb::{Error, IteratorMode, Options, DB};
use serde::export::Formatter;

use crate::config::DbConfig;

const ROOT_DB_NAME: &str = "root";

type SafeRW<T> = Arc<ShardedLock<T>>;
type DbResult<T> = Result<T, DbError>;

trait RWLock {
    type Item;

    fn r_lock(&self) -> ShardedLockReadGuard<'_, Self::Item>;
    fn w_lock(&self) -> ShardedLockWriteGuard<'_, Self::Item>;
}

pub struct Db {
    rock: SafeRW<DB>,
}

pub struct DbManager {
    config: DbConfig,
    root_db: Db,
    dbs: SafeRW<HashMap<String, Db>>,
    executor: Mutex<ThreadPoolExecutor>,
}

#[derive(Debug)]
pub enum DbError {
    Rocks(Error),
    Validation(String),
}

impl Display for DbError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            DbError::Rocks(e) => write!(f, "Db::RocksDb error: {}", e),
            DbError::Validation(s) => write!(f, "Db::Validation error: {}", s),
        }
    }
}

impl error::Error for DbError {
    fn cause(&self) -> Option<&dyn error::Error> {
        match self {
            DbError::Rocks(e) => Some(e),
            DbError::Validation(_) => Some(self),
        }
    }
}

impl From<Error> for DbError {
    fn from(e: Error) -> Self {
        DbError::Rocks(e)
    }
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
    pub fn new<P>(path: P) -> DbResult<Self>
    where
        P: AsRef<Path>,
    {
        let rock = DB::open_default(path).map_err(DbError::from)?;
        Ok(Db {
            rock: Arc::new(ShardedLock::new(rock)),
        })
    }

    pub fn put(&self, key: &str, val: &str) -> DbResult<()> {
        self.w_lock().put(key, val).map_err(DbError::from)
    }

    pub fn get(&self, key: &str) -> DbResult<Option<Vec<u8>>> {
        self.r_lock().get(key).map_err(DbError::from)
    }

    pub fn remove(&self, key: &str) -> DbResult<()> {
        self.w_lock().delete(key).map_err(DbError::from)
    }

    pub fn close<P>(&self, path: P) -> DbResult<()>
    where
        P: AsRef<Path>,
    {
        DB::destroy(&Options::default(), path).map_err(DbError::from)
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
    pub fn new(config: DbConfig) -> DbResult<Self> {
        let root_db = Db::new(format!("{}/{}", config.path, ROOT_DB_NAME))?;
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
                info!("Opening Db = {} on path = {}", &name, &path);
                let db = Db::new(path).expect("Unable to open db");
                self.dbs.write().unwrap().insert(name, db);
            });

        Ok(())
    }

    pub fn open(&self, db_name: String) -> DbResult<()> {
        if self.is_present(&db_name) {
            debug!("Db {} already exists", &db_name);
            Err(DbError::Validation(format!(
                "Database {} already exists",
                db_name
            )))
        } else {
            let path = format!("{}/{}", self.config.path, db_name);
            let db = Db::new(&path)?;

            self.root_db.put(&db_name, &path)?;
            self.w_lock().insert(db_name, db);
            Ok(())
        }
    }

    pub fn close(&self, db_name: String) -> DbResult<()> {
        if self.not_present(&db_name) {
            Err(DbError::Validation(format!(
                "Can't close {} db - doesn't exist",
                &db_name
            )))
        } else {
            if let Some(db) = self.w_lock().remove(&db_name) {
                info!("Closing db = {} ...", &db_name);
                self.executor
                    .lock()
                    .expect("Failed to acquire executor lock")
                    .execute(move || match db.close(&db_name) {
                        Ok(_) => info!("Db = {} closed", &db_name),
                        Err(e) => error!("Error closing db = {}, e = {}", &db_name, e),
                    });
            }

            Ok(())
        }
    }

    fn is_present(&self, db_name: &str) -> bool {
        self.r_lock().contains_key(db_name)
    }

    fn not_present(&self, db_name: &str) -> bool {
        !self.is_present(db_name)
    }
}
