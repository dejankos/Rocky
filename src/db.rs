
use std::collections::HashMap;

use std::path::Path;


use crossbeam::sync::{ShardedLock, ShardedLockReadGuard, ShardedLockWriteGuard};
use rocksdb::{DBIterator, Error, IteratorMode, DB};

use crate::config::DbConfig;
use std::sync::Arc;

const ROOT_DB_NAME: &'static str = "root";

pub struct Db {
    rock: Arc<ShardedLock<DB>>,
}

pub struct DbManager {
    config: DbConfig,
    root_db: Db,
    dbs: ShardedLock<HashMap<String, Db>>,
}

struct DbSnapshot<'a> {
    ss: DBIterator<'a>,
}

impl Db {
    pub fn new<P>(path: P) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        let rock = DB::open_default(path)?;
        Ok(Db {
            rock: Arc::new(ShardedLock::new(rock)),
        })
    }

    pub fn put(&self, key: &str, val: &str) -> Result<(), Error> {
        self.w_lock().put(key, val)
    }

    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        self.r_lock().get(key)
    }

    pub fn remove(&self, key: &str) {
        self.w_lock().delete(key);
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
            dbs: ShardedLock::new(HashMap::new()),
        })
    }

    pub fn init(&self) -> Result<(), Error> {
        info!("Init");

        let v = self.root_db.get("baza5");
        info!("v = {:?}", v);

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

    pub fn open(&self, db_name: String) -> Result<(), Error> {
        if self.is_present(&db_name) {
            error!("Db {} already exists", &db_name);
        }

        let path = format!("{}/{}", self.config.path, db_name);
        let db = Db::new(&path)?;

        self.root_db.put(&db_name, &path);
        let v = self.root_db.get(&db_name);
        info!("val after open = {:?}", v);

        let mut guard = self.dbs.write().unwrap();
        guard.insert(db_name, db);

        Ok(())
    }

    fn is_present(&self, db_name: &String) -> bool {
        self.dbs.read().unwrap().contains_key(db_name)
    }
}
