use std::pin::Pin;
use std::sync::Arc;

use futures::{Stream, stream};
use tokio_stream::wrappers::ReceiverStream;

use crate::{CommandResponse, Publish, Subscribe, Unsubscribe};
use crate::service::topic::Topic;

pub type StreamingResponse = Pin<Box<dyn Stream<Item=Arc<CommandResponse>> + Send>>;

pub trait TopicService {
    fn execute(self, topic: impl Topic) -> StreamingResponse;
}

impl TopicService for Subscribe {
    fn execute(self, topic: impl Topic) -> StreamingResponse {
        let receiver = topic.subscribe(self.topic);
        Box::pin(ReceiverStream::new(receiver))
    }
}

impl TopicService for Unsubscribe {
    fn execute(self, topic: impl Topic) -> StreamingResponse {
        topic.unsubscribe(self.topic, self.id);
        Box::pin(stream::once(async { Arc::new(CommandResponse::ok()) }))
    }
}

impl TopicService for Publish {
    fn execute(self, topic: impl Topic) -> StreamingResponse {
        topic.publish(self.topic, Arc::new(self.data.into()));
        Box::pin(stream::once(async { Arc::new(CommandResponse::ok()) }))
    }
}