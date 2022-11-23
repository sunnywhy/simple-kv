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
    store: Store,
    on_received: Vec<fn(&CommandRequest)>,
    on_executed: Vec<fn(&CommandResponse)>,
    on_before_send: Vec<fn(&mut CommandResponse)>,
    on_after_send: Vec<fn()>,
}

impl Clone for Service {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner)
        }
    }
}

// event notification, un-changeable
pub trait Notify<Args> {
    fn notify(&self, args: &Args);
}

// event notification, changeable
pub trait NotifyMut<Args> {
    fn notify(&self, args: &mut Args);
}

impl<Args> Notify<Args> for Vec<fn(&Args)> {
    fn notify(&self, args: &Args) {
        for f in self {
            f(args);
        }
    }
}

impl<Args> NotifyMut<Args> for Vec<fn(&mut Args)> {
    fn notify(&self, args: &mut Args) {
        for f in self {
            f(args);
        }
    }
}

impl <Store: Storage> Service<Store> {
    pub fn execute(&self, request: CommandRequest) -> CommandResponse {
        self.inner.on_received.notify(&request);
        let mut response = dispatch(request, &self.inner.store);
        self.inner.on_executed.notify(&response);
        self.inner.on_before_send.notify(&mut response);

        response
    }
}

impl<Store: Storage> From<ServiceInner<Store>> for Service<Store> {
    fn from(inner: ServiceInner<Store>) -> Self {
        Self {
            inner: Arc::new(inner)
        }
    }
}

impl<Store: Storage> ServiceInner<Store> {
    pub fn new(store: Store) -> Self {
        Self {
            store,
            on_received: vec![],
            on_executed: vec![],
            on_before_send: vec![],
            on_after_send: vec![],
        }
    }
    pub fn fn_received(mut self, f: fn(&CommandRequest)) -> Self {
        self.on_received.push(f);
        self
    }

    pub fn fn_executed(mut self, f: fn(&CommandResponse)) -> Self {
        self.on_executed.push(f);
        self
    }

    pub fn fn_before_send(mut self, f: fn(&mut CommandResponse)) -> Self {
        self.on_before_send.push(f);
        self
    }

    pub fn fn_after_send(mut self, f: fn()) -> Self {
        self.on_after_send.push(f);
        self
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
    use http::StatusCode;
    use tracing::info;
    use crate::{MemTable, Value};
    use super::*;

    #[test]
    fn service_should_work() {
        let service: Service = ServiceInner::new(MemTable::new()).into();

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

    #[test]
    fn event_registration_should_work() {
        fn b(cmd: &CommandRequest) {
            info!("Got {:?}", cmd);
        }
        fn c(res: &CommandResponse) {
            info!("{:?}", res);
        }
        fn d(res: &mut CommandResponse) {
            res.status = StatusCode::CREATED.as_u16() as u32;
        }
        fn e() {
            info!("Done");
        }

        let service: Service = ServiceInner::new(MemTable::new())
            .fn_received(|_: &CommandRequest| info!("Got a request"))
            .fn_received(b)
            .fn_executed(c)
            .fn_before_send(d)
            .fn_after_send(e)
            .into();

        let response = service.execute(CommandRequest::new_hset("score", "math", 25.into()));
        assert_eq!(response.status, StatusCode::CREATED.as_u16() as u32);
        assert_eq!(response.message, "");
        assert_eq!(response.values, vec![Value::default()]);
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
