mod frame;
mod tls;

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

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use bytes::Bytes;
    use tokio::net::{TcpListener, TcpStream};
    use crate::{assert_response_ok, MemTable, ServiceInner, Value};
    use super::*;

    #[tokio::test]
    async fn client_server_basic_communication_should_work() -> anyhow::Result<()> {
        let addr = start_server().await?;

        let stream = TcpStream::connect(addr).await?;
        let mut client = ProstClientStream::new(stream);

        // send HSET, wait for response
        let request = CommandRequest::new_hset("table", "key", "value".into());
        let response = client.execute(request).await?;

        // first time, response should be default value
        assert_response_ok(response, &[Value::default()], &[]);

        // another HSET
        let request = CommandRequest::new_hset("table", "key", "value2".into());
        let response = client.execute(request).await?;

        // second time, response should be the first value
        assert_response_ok(response, &["value".into()], &[]);

        Ok(())
    }

    #[tokio::test]
    async fn client_server_compression_should_work() -> anyhow::Result<()> {
        let addr = start_server().await?;

        let stream = TcpStream::connect(addr).await?;
        let mut client = ProstClientStream::new(stream);

        let v: Value = Bytes::from(vec![0u8;16384]).into();
        let request = CommandRequest::new_hset("table", "key", v.clone().into());
        let response = client.execute(request).await?;

        assert_response_ok(response, &[Value::default()], &[]);

        let request = CommandRequest::new_hget("table", "key");
        let response = client.execute(request).await?;

        // second time, response should be the first value
        assert_response_ok(response, &[v.into()], &[]);

        Ok(())
    }

    async fn start_server() -> anyhow::Result<SocketAddr> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        tokio::spawn(async move {
            let service: Service = ServiceInner::new(MemTable::new()).into();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let service = service.clone();
                let server = ProstServerStream::new(stream, service);
                tokio::spawn(server.process());
            }

        });

        Ok(addr)
    }
}