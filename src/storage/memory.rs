use dashmap::DashMap;

use crate::Value;

#[derive(Debug, Default, Clone)]
pub struct MemTable {
    data: DashMap<String, DashMap<String, Value>>,
}

impl MemTable {
    pub fn new() -> Self {
        Self::default()
    }
}
