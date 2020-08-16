use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use crossbeam::sync::{ShardedLock, ShardedLockReadGuard, ShardedLockWriteGuard};
use rocksdb::{DB, Error, IteratorMode};

use crate::config::DbConfig;

const ROOT_DB_NAME: &str = "root";

type Safe<T> = Arc<ShardedLock<T>>;
type DbResult<T> = Result<T, Error>;

pub struct Db {
    rock: Safe<DB>,
}

pub struct DbManager {
    config: DbConfig,
    root_db: Db,
    dbs: Safe<HashMap<String, Db>>,
}

impl Db {
    pub fn new<P>(path: P) -> DbResult<Self>
        where
            P: AsRef<Path>,
    {
        let rock = DB::open_default(path)?;
        Ok(Db {
            rock: Arc::new(ShardedLock::new(rock)),
        })
    }

    pub fn put(&self, key: &str, val: &str) -> DbResult<()> {
        self.w_lock().put(key, val)
    }

    pub fn get(&self, key: &str) -> DbResult<Option<Vec<u8>>> {
        self.r_lock().get(key)
    }

    pub fn remove(&self, key: &str) -> DbResult<()> {
        self.w_lock().delete(key)
    }

    pub fn r_lock(&self) -> ShardedLockReadGuard<'_, DB> {
        self.rock.read().expect("Can't acquire read lock")
    }

    fn w_lock(&self) -> ShardedLockWriteGuard<'_, DB> {
        self.rock.write().expect("Can't acquire write lock")
    }
}

impl DbManager {
    pub fn new(config: DbConfig) -> Result<Self, Error> {
        let root_db = Db::new(format!("{}/{}", config.path, ROOT_DB_NAME))?;
        Ok(DbManager {
            config,
            root_db,
            dbs: Arc::new(ShardedLock::new(HashMap::new())),
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
            error!("Db {} already exists", &db_name);
        }

        let path = format!("{}/{}", self.config.path, db_name);
        let db = Db::new(&path)?;

        self.root_db.put(&db_name, &path)?;
        let v = self.root_db.get(&db_name);
        info!("val after open = {:?}", v);

        let mut guard = self.dbs.write().unwrap();
        guard.insert(db_name, db);

        Ok(())
    }

    fn is_present(&self, db_name: &str) -> bool {
        self.dbs.read().unwrap().contains_key(db_name)
    }
}
