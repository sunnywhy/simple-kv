use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use dashmap::{DashMap, DashSet};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tracing::{debug, info, warn};

use crate::{CommandResponse, Value};

// biggest data can be saved in the topic
const BROADCAST_CAPACITY: usize = 128;

// next subscription id
static NEXT_ID: AtomicU32 = AtomicU32::new(1);

// get next subscription id
fn get_next_subscription_id() -> u32 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

pub trait Topic: Send + Sync + 'static {
    // subscribe a topic
    fn subscribe(self, name: String) -> mpsc::Receiver<Arc<CommandResponse>>;
    // unsubscribe a topic
    fn unsubscribe(self, name: String, id: u32);
    // publish data to a topic
    fn publish(self, name: String, value: Arc<CommandResponse>);
}

// data structure for topic publish and subscribe
#[derive(Default)]
pub struct Broadcaster {
    // all topics list
    topics: DashMap<String, DashSet<u32>>,
    // all subscribe list
    subscriptions: DashMap<u32, mpsc::Sender<Arc<CommandResponse>>>,
}

impl Topic for Arc<Broadcaster> {
    fn subscribe(self, name: String) -> Receiver<Arc<CommandResponse>> {
        let id = {
            let entry = self.topics.entry(name).or_default();
            let id = get_next_subscription_id();
            entry.value().insert(id);
            id
        };

        // generate a mpsc channel
        let (sender, receiver) = mpsc::channel(BROADCAST_CAPACITY);

        let v: Value = (id as i64).into();
        // send the subscription id to the receiver
        let sender1 = sender.clone();
        tokio::spawn(async move {
            if let Err(e) = sender1.send(Arc::new(v.into())).await {
                warn!("Failed to send subscription id: {}. Error: {:?}", id, e);
            }
        });

        // save sender to the subscription table
        self.subscriptions.insert(id, sender);
        debug!("Subscription {} is added", id);

        // return receiver to the context
        receiver
    }

    fn unsubscribe(self, name: String, id: u32) {
        if let Some(v) = self.topics.get_mut(&name) {
            v.remove(&id);

            // if topic is empty, delete the topic too
            if v.is_empty() {
                info!("Topic: {:?} is deleted", &name);
                drop(v);
                self.topics.remove(&name);
            }
        }

        debug!("Subscription {} is removed!", id);

        self.subscriptions.remove(&id);
    }

    fn publish(self, name: String, value: Arc<CommandResponse>) {
        tokio::spawn(async move {
            match self.topics.get(&name) {
                None => {}
                Some(v) => {
                    // copy all subscription ids under a topic
                    let ids = v.value().clone();

                    for id in ids.into_iter() {
                        if let Some(sender) = self.subscriptions.get(&id) {
                            if let Err(e) = sender.send(value.clone()).await {
                                warn!("Publish to {} failed! Error: {:?}", id, e);
                            }
                        }
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use crate::assert_response_ok;

    use super::*;

    #[tokio::test]
    async fn pub_sub_should_work() {
        let b = Arc::new(Broadcaster::default());
        let lobby = "lobby".to_string();

        // subscribe
        let mut stream1 = b.clone().subscribe(lobby.clone());
        let mut stream2 = b.clone().subscribe(lobby.clone());

        // publish
        let v: Value = "hello".into();
        b.clone().publish(lobby.clone(), Arc::new(v.clone().into()));

        // subscribers should receive published data
        let id1: i64 = stream1.recv().await.unwrap().as_ref().try_into().unwrap();
        let id2: i64 = stream2.recv().await.unwrap().as_ref().try_into().unwrap();

        assert_ne!(id1, id2);

        let res1 = stream1.recv().await.unwrap();
        let res2 = stream2.recv().await.unwrap();

        assert_eq!(res1, res2);
        assert_response_ok(&res1, &[v.clone()], &[]);

        // if unsubscribe, shouldn't receive new data
        b.clone().unsubscribe(lobby.clone(), id1 as _);

        let v: Value = "world".into();
        b.clone().publish(lobby.clone(), Arc::new(v.clone().into()));

        assert!(stream1.recv().await.is_none());
        let res2 = stream2.recv().await.unwrap();
        assert_response_ok(&res2, &[v.clone()], &[]);
    }
}