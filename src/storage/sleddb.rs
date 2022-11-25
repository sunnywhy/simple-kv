use std::{path::Path, str};
use sled::{Db, Error, IVec};
use crate::{KvError, KvPair, Storage, StorageIter, Value};

#[derive(Debug)]
pub struct SledDb(Db);

impl SledDb {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self(sled::open(path).unwrap())
    }

    // since sled can scan_prefix, so we can use `prefix` to simulate `table`
    pub fn get_full_key(table: &str, key: &str) -> String {
        format!("{}:{}", table, key)
    }
}

fn flip<T, E>(x: Option<Result<T, E>>) -> Result<Option<T>, E> {
    x.map_or(Ok(None), |x| x.map(Some))
}

impl Storage for SledDb {
    fn get(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        let key = SledDb::get_full_key(table, key);
        let result = self.0.get(key.as_bytes())?.map(|v| v.as_ref().try_into());
        flip(result)
    }

    fn set(&self, table: &str, key: String, value: Value) -> Result<Option<Value>, KvError> {
        let key = SledDb::get_full_key(table, &key);
        let data: Vec<u8> = value.try_into()?;
        let result = self.0.insert(key.as_bytes(), data)?.map(|v| v.as_ref().try_into());
        flip(result)
    }

    fn contains(&self, table: &str, key: &str) -> Result<bool, KvError> {
        let key = SledDb::get_full_key(table, key);
        let result = self.0.contains_key(key.as_bytes())?;
        Ok(result)
    }

    fn del(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        let key = SledDb::get_full_key(table, key);
        let result = self.0.remove(key.as_bytes())?.map(|v| v.as_ref().try_into());
        flip(result)
    }

    fn get_all(&self, table: &str) -> Result<Vec<KvPair>, KvError> {
        let prefix = SledDb::get_full_key(table, "");
        let iter = self.0.scan_prefix(prefix.as_bytes());
        let result = iter
            .map(|item| {
                item.into()
            })
            .collect();
        Ok(result)
    }

    fn get_iter(&self, table: &str) -> Result<Box<dyn Iterator<Item=KvPair>>, KvError> {
        let prefix = SledDb::get_full_key(table, "");
        let iter = self.0.scan_prefix(prefix.as_bytes());
        Ok(Box::new(StorageIter::new(iter)))
    }
}

impl From<Result<(IVec, IVec), sled::Error>> for KvPair {
    fn from(data: Result<(IVec, IVec), Error>) -> Self {
        match data {
            Ok((key, value)) => match value.as_ref().try_into() {
                Ok(value) => KvPair::new(ivec_to_key(key.as_ref()), value),
                Err(_) => KvPair::default(),
            },
            _ => KvPair::default(),
        }
    }
}

fn ivec_to_key(ivec: &[u8]) -> &str {
    let key = str::from_utf8(ivec).unwrap();
    key.split(':').last().unwrap()
}