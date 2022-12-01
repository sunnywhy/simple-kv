mod frame;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tracing::info;
pub use frame::FrameCoder;
use crate::{CommandRequest, CommandResponse, KvError, Service};

// handle the read/write of a socket accepted by the server
pub struct ProstServerStream<S> {
    inner: S,
    service: Service,
}

// handle the read/write of a socket by the client
pub struct ProstClientStream<S> {
    inner: S,
}

impl<S> ProstServerStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    pub fn new(inner: S, service: Service) -> Self {
        Self { inner, service }
    }

    pub async fn process(mut self) -> Result<(), KvError> {
        while let Ok(request) = self.recv().await {
            info!("received request: {:?}", request);
            let response = self.service.execute(request);
            self.send(response).await?;
        }
        Ok(())
    }

    async fn send(&mut self, response: CommandResponse) -> Result<(), KvError> {
        let mut buf = BytesMut::new();
        response.encode_frame(&mut buf)?;
        let encoded = buf.freeze();
        self.inner.write_all(&encoded).await?;

        Ok(())
    }

    async fn recv(&mut self) -> Result<CommandRequest, KvError> {
        let mut buf = BytesMut::new();
        frame::read_frame(&mut self.inner, &mut buf).await?;

        CommandRequest::decode_frame(&mut buf)
    }
}

impl<S> ProstClientStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    pub fn new(inner: S) -> Self {
        Self { inner }
    }

    pub async fn execute(&mut self, request: CommandRequest) -> Result<CommandResponse, KvError> {
        self.send(request).await?;
        Ok(self.recv().await?)
    }

    pub async fn send(&mut self, request: CommandRequest) -> Result<(), KvError> {
        let mut buf = BytesMut::new();
        request.encode_frame(&mut buf)?;
        let encoded = buf.freeze();
        self.inner.write_all(&encoded).await?;

        Ok(())
    }

    pub async fn recv(&mut self) -> Result<CommandResponse, KvError> {
        let mut buf = BytesMut::new();
        frame::read_frame(&mut self.inner, &mut buf).await?;

        CommandResponse::decode_frame(&mut buf)
    }
}

