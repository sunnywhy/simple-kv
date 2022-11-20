use std::sync::Arc;
use crate::{CommandRequest, CommandResponse, KvError, KvPair, MemTable, Storage, Value};
use crate::command_request::RequestData;

mod command_service;

pub trait CommandService {
    fn execute(self, store: &impl Storage) -> CommandResponse;
}

pub struct Service<Store = MemTable> {
    inner: Arc<ServiceInner<Store>>
}

pub struct ServiceInner<Store> {
    store: Store
}

impl Clone for Service {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner)
        }
    }
}

impl <Store: Storage> Service<Store> {
    pub fn new(store: Store) -> Self {
        Self {
            inner: Arc::new(ServiceInner { store })
        }
    }

    pub fn execute(&self, request: CommandRequest) -> CommandResponse {
        // TODO: send on_received event
        let response = dispatch(request, &self.inner.store);
        // TODO: send on_executed event

        response
    }
}

pub fn dispatch(request: CommandRequest, store: &impl Storage) -> CommandResponse {
    match request.request_data {
        Some(RequestData::Hget(v)) => v.execute(store),
        Some(RequestData::Hgetall(v)) => v.execute(store),
        Some(RequestData::Hmget(v)) => v.execute(store),
        Some(RequestData::Hset(v)) => v.execute(store),
        Some(RequestData::Hmset(v)) => v.execute(store),
        Some(RequestData::Hdel(v)) => v.execute(store),
        Some(RequestData::Hmdel(v)) => v.execute(store),
        Some(RequestData::Hexist(v)) =>  v.execute(store),
        Some(RequestData::Hmexist(v)) => v.execute(store),
        None => KvError::InvalidCommand("invalid command".into()).into(),
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use crate::{MemTable, Value};
    use super::*;

    #[test]
    fn service_should_work() {
        let service = Service::new(MemTable::new());

        // service can run in multiple threads, so clone should be lightweight
        let cloned = service.clone();

        let handle = thread::spawn(move || {
            let request = CommandRequest::new_hset("score", "math", 10.into());
            let response = cloned.execute(request);
            assert_response_ok(response, &[Value::default()], &[]);
        });

        handle.join().unwrap();

        let response = service.execute(CommandRequest::new_hget("score", "math"));
        assert_response_ok(response, &[10.into()], &[]);
    }
}

#[cfg(test)]
pub fn assert_response_ok(mut response: CommandResponse, values: &[Value], pairs: &[KvPair]) {
    response.pairs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(response.status, 200);
    assert_eq!(response.message, "");
    assert_eq!(response.values, values);
    assert_eq!(response.pairs, pairs);
}

#[cfg(test)]
pub fn assert_response_error(response: CommandResponse, code: u32, message: &str) {
    assert_eq!(response.status, code);
    assert!(response.message.contains(message));
    assert_eq!(response.values, &[]);
    assert_eq!(response.pairs, &[]);
}
