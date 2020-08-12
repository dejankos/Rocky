use std::collections::HashMap;

use rocksdb::{Error, DB};

use crate::config::DbConfig;

pub struct Db {
    rock: DB,
}

#[derive(Default)]
pub struct DbManager {
    dbs: HashMap<String, Db>,
    config: DbConfig,
}

impl Db {
    pub fn new(path: &str) -> Result<Self, Error> {
        let rock = DB::open_default(path)?;
        Ok(Db { rock })
    }

    pub fn put(&self, key: &str, val: &str) {
        self.rock.put(key, val).unwrap();
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.rock.get(key).unwrap()
    }

    pub fn remove(&self, key: &str) {
        self.rock.delete(key);
    }
}

impl DbManager {
    pub fn new(config: DbConfig) -> Self {
        DbManager {
            config,
            ..Default::default()
        }
    }

    pub fn open(&mut self, db_name: String) -> Result<(), Error> {
        let db = Db::new(self.config.path.as_str())?;
        self.dbs.insert(db_name, db);
        Ok(())
    }
}
