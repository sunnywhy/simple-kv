use crate::pb::abi::{CommandRequest, Hset, KvPair, Value, value};
use crate::pb::abi::command_request::RequestData;

pub mod abi;

impl CommandRequest {
    pub fn new_hset(table: impl Into<String>, key: impl Into<String>, value: Value) -> Self {
        Self {
            request_data: Some(RequestData::Hset(Hset {
                table: table.into(),
                pair: Some(KvPair::new(key, value)),
            })),
        }
    }
}

impl KvPair {
    pub fn new(key: impl Into<String>, value: Value) -> Self {
        Self { key: key.into(), value: Some(value) }
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Self {
            value: Some(value::Value::String(s))
        }
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Self {
            value: Some(value::Value::String(s.into()))
        }
    }
}