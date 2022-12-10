use std::sync::Arc;

use futures::stream;
use tracing::debug;

use crate::{CommandRequest, CommandResponse, KvError, MemTable, Storage};
#[cfg(test)]
use crate::{KvPair, Value};
use crate::command_request::RequestData;
use crate::service::topic::{Broadcaster, Topic};
use crate::service::topic_service::{StreamingResponse, TopicService};

mod command_service;
mod topic_service;
mod topic;

pub trait CommandService {
    fn execute(self, store: &impl Storage) -> CommandResponse;
}

pub struct Service<Store = MemTable> {
    inner: Arc<ServiceInner<Store>>,
    broadcaster: Arc<Broadcaster>,
}

pub struct ServiceInner<Store> {
    store: Store,
    on_received: Vec<fn(&CommandRequest)>,
    on_executed: Vec<fn(&CommandResponse)>,
    on_before_send: Vec<fn(&mut CommandResponse)>,
    on_after_send: Vec<fn()>,
}

impl<Store> Clone for Service<Store> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            broadcaster: Arc::clone(&self.broadcaster),
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

impl<Store: Storage> Service<Store> {
    pub fn execute(&self, request: CommandRequest) -> StreamingResponse {
        self.inner.on_received.notify(&request);
        let mut response = dispatch(request.clone(), &self.inner.store);

        if response == CommandResponse::default() {
            dispatch_stream(request, Arc::clone(&self.broadcaster));
        } else {
            self.inner.on_executed.notify(&response);
            self.inner.on_before_send.notify(&mut response);
            if !self.inner.on_after_send.is_empty() {
                debug!("Modified response: {:?}", response);
            }
        }

        Box::pin(stream::once(async { Arc::new(response) }))
    }
}

impl<Store: Storage> From<ServiceInner<Store>> for Service<Store> {
    fn from(inner: ServiceInner<Store>) -> Self {
        Self {
            inner: Arc::new(inner),
            broadcaster: Default::default(),
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
        Some(RequestData::Hexist(v)) => v.execute(store),
        Some(RequestData::Hmexist(v)) => v.execute(store),
        None => KvError::InvalidCommand("invalid command".into()).into(),
        // if cannot handle, return an empty Response, then we can try to handle it by dispatch_stream
        _ => CommandResponse::default(),
    }
}

pub fn dispatch_stream(request: CommandRequest, topic: impl Topic) -> StreamingResponse {
    match request.request_data {
        Some(RequestData::Publish(v)) => v.execute(topic),
        Some(RequestData::Subscribe(v)) => v.execute(topic),
        Some(RequestData::Unsubscribe(v)) => v.execute(topic),
        // if comes here, then logic error, crash
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use futures::StreamExt;
    use http::StatusCode;
    use tracing::info;

    use super::*;

    #[tokio::test]
    async fn service_should_work() {
        let service: Service = ServiceInner::new(MemTable::new()).into();

        // service can run in multiple threads, so clone should be lightweight
        let cloned = service.clone();

        tokio::spawn(async move {
            let request = CommandRequest::new_hset("score", "math", 10.into());
            let mut response = cloned.execute(request);
            let data = response.next().await.unwrap();
            assert_response_ok(&data, &[Value::default()], &[]);
        }).await.unwrap();


        let mut response = service.execute(CommandRequest::new_hget("score", "math"));
        let data = response.next().await.unwrap();
        assert_response_ok(&data, &[10.into()], &[]);
    }

    #[tokio::test]
    async fn event_registration_should_work() {
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

        let mut response = service.execute(CommandRequest::new_hset("score", "math", 25.into()));
        let data = response.next().await.unwrap();

        assert_eq!(data.status, StatusCode::CREATED.as_u16() as u32);
        assert_eq!(data.message, "");
        assert_eq!(data.values, vec![Value::default()]);
    }
}

#[cfg(test)]
pub fn assert_response_ok(response: &CommandResponse, values: &[Value], pairs: &[KvPair]) {
    let mut sorted_pairs = response.pairs.clone();
    sorted_pairs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(response.status, 200);
    assert_eq!(response.message, "");
    assert_eq!(response.values, values);
    assert_eq!(sorted_pairs, pairs);
}

#[cfg(test)]
pub fn assert_response_error(response: &CommandResponse, code: u32, message: &str) {
    assert_eq!(response.status, code);
    assert!(response.message.contains(message));
    assert_eq!(response.values, &[]);
    assert_eq!(response.pairs, &[]);
}
