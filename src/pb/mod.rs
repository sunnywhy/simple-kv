use bytes::Bytes;
use http::StatusCode;
use prost::Message;

use abi::*;
use abi::command_request::RequestData;

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

    pub fn new_subscribe(name: impl Into<String>) -> Self {
        Self {
            request_data: Some(RequestData::Subscribe(Subscribe { topic: name.into() })),
        }
    }

    pub fn new_unsubscribe(name: impl Into<String>, id: u32) -> Self {
        Self {
            request_data: Some(RequestData::Unsubscribe(Unsubscribe {
                topic: name.into(),
                id,
            })),
        }
    }

    pub fn new_publish(name: impl Into<String>, data: Vec<Value>) -> Self {
        Self {
            request_data: Some(RequestData::Publish(Publish {
                topic: name.into(),
                data,
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
            KvError::NotFound(_, _) => StatusCode::NOT_FOUND.as_u16(),
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

impl CommandResponse {
    pub fn ok() -> Self {
        let mut result = CommandResponse::default();
        result.status = StatusCode::OK.as_u16() as _;
        result
    }

    pub fn format(&self) -> String {
        format!("{:?}", self)
    }
}

impl Value {
    pub fn format(&self) -> String {
        format!("{:?}", self)
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

impl<const N: usize> From<&[u8; N]> for Value {
    fn from(bytes: &[u8; N]) -> Self {
        Bytes::copy_from_slice(&bytes[..]).into()
    }
}

impl From<Bytes> for Value {
    fn from(bytes: Bytes) -> Self {
        Self {
            value: Some(value::Value::Binary(bytes)),
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

impl TryFrom<&Value> for i64 {
    type Error = KvError;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        match value.value {
            Some(value::Value::Integer(i)) => Ok(i),
            _ => Err(KvError::ConvertError(value.format(), "integer")),
        }
    }
}

impl TryFrom<&CommandResponse> for i64 {
    type Error = KvError;

    fn try_from(value: &CommandResponse) -> Result<Self, Self::Error> {
        if value.status != StatusCode::OK.as_u16() as u32 {
            return Err(KvError::ConvertError(value.format(), "CommandResponse"));
        }
        match value.values.get(0) {
            Some(v) => v.try_into(),
            None => Err(KvError::ConvertError(value.format(), "CommandResponse")),
        }
    }
}