use crate::error::KvError;
use crate::{KvPair, Value};

mod memory;
mod sleddb;

pub use memory::MemTable;
pub use sleddb::SledDb;

// we don't care where the data is saved, we need to define how the storage will be used
pub trait Storage {
    // get a value from a table by key
    fn get(&self, table: &str, key: &str) -> Result<Option<Value>, KvError>;

    // set a value to a table by key, return the old value if exists
    fn set(&self, table: &str, key: String, value: Value) -> Result<Option<Value>, KvError>;

    // check if a key exists in a table
    fn contains(&self, table: &str, key: &str) -> Result<bool, KvError>;

    // remove a key from a table, return the old value if exists
    fn del(&self, table: &str, key: &str) -> Result<Option<Value>, KvError>;

    // get all KV pairs in a table
    fn get_all(&self, table: &str) -> Result<Vec<KvPair>, KvError>;

    // get kv pairs' iterator in a table
    fn get_iter(&self, table: &str) -> Result<Box<dyn Iterator<Item = KvPair>>, KvError>;
}

pub struct StorageIter<T> {
    iter: T,
}

impl<T> StorageIter<T> {
    pub fn new(iter: T) -> Self {
        Self { iter }
    }
}

impl<T> Iterator for StorageIter<T>
where
    T: Iterator,
    T::Item: Into<KvPair>,
{
    type Item = KvPair;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|item| item.into())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use crate::storage::sleddb::SledDb;
    use super::*;

    #[test]
    fn memtable_basic_interface_should_work() {
        let store = MemTable::new();
        test_basic_interface(store);
    }

    #[test]
    fn memtable_get_all_should_work() {
        let store = MemTable::new();
        test_get_all(store);
    }

    #[test]
    fn memtable_iter_should_work() {
        let store = MemTable::new();
        test_get_iter(store);
    }

    #[test]
    fn sleddb_basic_interface_should_work() {
        let dir = tempdir().unwrap();
        let store = SledDb::new(dir);
        test_basic_interface(store);
    }

    #[test]
    fn sleddb_get_all_should_work() {
        let dir = tempdir().unwrap();
        let store = SledDb::new(dir);
        test_get_all(store);
    }

    #[test]
    fn sleddb_iter_should_work() {
        let dir = tempdir().unwrap();
        let store = SledDb::new(dir);
        test_get_iter(store);
    }

    fn test_basic_interface(store: impl Storage) {
        let table = "test_table";
        let key = "test_key";
        let value = "test_value";
        assert_eq!(None, store.get(table, key).unwrap());
        assert_eq!(None, store.set(table, key.to_string(), value.into()).unwrap());
        assert_eq!(store.get(table, key).unwrap(), Some(value.into()));
        assert!(store.contains(table, key).unwrap());
        assert_eq!(store.del(table, key).unwrap(), Some(value.into()));
        assert_eq!(None, store.del(table, key).unwrap());
        assert_eq!(None, store.get(table, key).unwrap());
        assert!(!store.contains(table, key).unwrap());
    }

    fn test_get_all(store: impl Storage) {
        store.set("t2", "k1".into(), "v1".into()).unwrap();
        store.set("t2", "k2".into(), "v2".into()).unwrap();

        let mut pairs = store.get_all("t2").unwrap();
        pairs.sort_by(|a, b| a.partial_cmp(b).unwrap());

        assert_eq!(
            pairs,
            vec![
                KvPair::new("k1", "v1".into()),
                KvPair::new("k2", "v2".into()),
            ]
        );
    }

    fn test_get_iter(store: impl Storage) {
        store.set("t3", "k1".into(), "v1".into()).unwrap();
        store.set("t3", "k2".into(), "v2".into()).unwrap();

        let mut pairs = store.get_iter("t3").unwrap().collect::<Vec<_>>();
        pairs.sort_by(|a, b| a.partial_cmp(b).unwrap());

        assert_eq!(
            pairs,
            vec![
                KvPair::new("k1", "v1".into()),
                KvPair::new("k2", "v2".into()),
            ]
        );
    }
}
