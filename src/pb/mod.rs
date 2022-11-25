use http::StatusCode;
use abi::command_request::RequestData;
use abi::*;
use prost::Message;
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

    pub fn new_hmget(table: impl Into<String>, keys: Vec<String>) -> Self {
        Self {
            request_data: Some(RequestData::Hmget(Hmget {
                table: table.into(),
                keys,
            })),
        }
    }

    pub fn new_hmset(table: impl Into<String>, pairs: Vec<KvPair>) -> Self {
        Self {
            request_data: Some(RequestData::Hmset(Hmset {
                table: table.into(),
                pairs,
            })),
        }
    }

    pub fn new_hdel(table: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            request_data: Some(RequestData::Hdel(Hdel {
                table: table.into(),
                key: key.into(),
            })),
        }
    }

    pub fn new_hmdel(table: impl Into<String>, keys: Vec<String>) -> Self {
        Self {
            request_data: Some(RequestData::Hmdel(Hmdel {
                table: table.into(),
                keys,
            })),
        }
    }

    pub fn new_hexist(table: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            request_data: Some(RequestData::Hexist(Hexist {
                table: table.into(),
                key: key.into(),
            })),
        }
    }

    pub fn new_hmexist(table: impl Into<String>, keys: Vec<String>) -> Self {
        Self {
            request_data: Some(RequestData::Hmexist(Hmexist {
                table: table.into(),
                keys,
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

impl From<Vec<Value>> for CommandResponse {
    fn from(values: Vec<Value>) -> Self {
        Self {
            status: StatusCode::OK.as_u16() as u32,
            values,
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

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Self {
            value: Some(value::Value::Bool(b)),
        }
    }
}

impl From<(String, Value)> for KvPair {
    fn from((key, value): (String, Value)) -> Self {
        KvPair::new(key, value)
    }
}

impl TryFrom<&[u8]> for Value {
    type Error = KvError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        Ok(Value::decode(bytes)?)
    }
}

impl TryFrom<Value> for Vec<u8> {
    type Error = KvError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        let mut buf = Vec::with_capacity(value.encoded_len());
        value.encode(&mut buf)?;
        Ok(buf)
    }
}
