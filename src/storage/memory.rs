use dashmap::DashMap;
use dashmap::mapref::one::Ref;

use crate::Value;

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
