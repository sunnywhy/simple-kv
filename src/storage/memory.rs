use dashmap::DashMap;
use dashmap::mapref::one::Ref;

use crate::{KvPair, Storage, Value};
use crate::error::KvError;

#[derive(Debug, Default, Clone)]
pub struct MemTable {
    tables: DashMap<String, DashMap<String, Value>>,
}

impl MemTable {
    pub fn new() -> Self {
        Self::default()
    }

    fn get_or_create_table(&self, table_name: &str) -> Ref<String, DashMap<String, Value>> {
        self.tables.entry(table_name.to_string()).or_insert_with(DashMap::new).downgrade()
    }
}

impl Storage for MemTable {
    fn get(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        let table = self.get_or_create_table(table);
        Ok(table.get(key).map(|v| v.clone()))
    }

    fn set(&self, table: &str, key: String, value: Value) -> Result<Option<Value>, KvError> {
        let table = self.get_or_create_table(table);
        Ok(table.insert(key, value))
    }

    fn contains(&self, table: &str, key: &str) -> Result<bool, KvError> {
        let table = self.get_or_create_table(table);
        Ok(table.contains_key(key))
    }

    fn del(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        let table = self.get_or_create_table(table);
        Ok(table.remove(key).map(|(_, v)| v))
    }

    fn get_all(&self, table: &str) -> Result<Vec<KvPair>, KvError> {
        let table = self.get_or_create_table(table);
        Ok(table.iter().map(|item| KvPair::new(item.key(), item.value().clone())).collect())
    }

    fn get_iter(&self, table: &str) -> Result<Box<dyn Iterator<Item=KvPair>>, KvError> {
        // use clone() to get a snapshot of the table
        let table = self.get_or_create_table(table).clone();
        Ok(Box::new(table.into_iter().map(|item| item.into())))
    }
}