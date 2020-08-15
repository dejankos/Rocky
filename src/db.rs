use std::collections::HashMap;

use rocksdb::{Error, DB};

use crate::config::DbConfig;
use std::sync::RwLock;

const ROOT_DB_NAME: &'static str = "root";

pub struct Db {
    rock: DB,
}

#[derive(Default)]
pub struct DbManager {
    dbs: RwLock<HashMap<String, Db>>,
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
            dbs: RwLock::new(HashMap::new())
        }
    }

    pub fn open(&self, db_name: String) -> Result<(), Error> {
        if self.is_present(&db_name) {
            error!("Db {} already exists", &db_name);
        }

        info!("before insert ");

        let db = Db::new(format!("{}/{}", self.config.path, db_name).as_str())?;
        info!("after db open");

        let mut guard = self.dbs.write().unwrap();
        guard.insert(db_name, db);
        info!("data size = {}", guard.len());


        info!("after insert ");

        Ok(())
    }

    fn is_present(&self, db_name: &String) -> bool {
       self.dbs.read().unwrap().contains_key(db_name)
    }



}
