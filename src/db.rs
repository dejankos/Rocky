use rocksdb::DB;

pub struct Db {
    rock: DB
}


impl Db {
    pub fn new() -> Self {
        Db {
            rock: DB::open_default("/home/dkos/Private_WorkSpace/Clion_Workspace/Rocky/db").unwrap()
        }
    }

    pub fn put(&self, key: &str, val: &str) {
        self.rock.put(key, val).unwrap();
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.rock.get(key).unwrap()
    }

}