use http::StatusCode;
use abi::command_request::RequestData;
use abi::*;
use crate::KvError;

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

    pub fn new_hget(table: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            request_data: Some(RequestData::Hget(Hget {
                table: table.into(),
                key: key.into(),
            })),
        }
    }

    pub fn new_hget_all(table: impl Into<String>) -> Self {
        Self {
            request_data: Some(RequestData::Hgetall(Hgetall {
                table: table.into(),
            })),
        }
    }
}

impl From<Value> for CommandResponse {
    fn from(value: Value) -> Self {
        Self {
           status: StatusCode::OK.as_u16() as u32,
            values: vec![value],
            ..Default::default()
        }
    }
}

impl From<Vec<KvPair>> for CommandResponse {
    fn from(pairs: Vec<KvPair>) -> Self {
        Self {
            status: StatusCode::OK.as_u16() as u32,
            pairs,
            ..Default::default()
        }
    }
}

impl From<KvError> for CommandResponse {
    fn from(error: KvError) -> Self {
        let status_code = match error {
            KvError::NotFound(_,_) => StatusCode::NOT_FOUND.as_u16(),
            KvError::InvalidCommand(_) => StatusCode::BAD_REQUEST.as_u16(),
            _ => StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
        };

        Self {
            status: status_code as u32,
            message: error.to_string(),
            ..Default::default()
        }
    }
}

impl KvPair {
    pub fn new(key: impl Into<String>, value: Value) -> Self {
        Self {
            key: key.into(),
            value: Some(value),
        }
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Self {
            value: Some(value::Value::String(s)),
        }
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Self {
            value: Some(value::Value::String(s.into())),
        }
    }
}

impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Self {
            value: Some(value::Value::Integer(i)),
        }
    }
}
