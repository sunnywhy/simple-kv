use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::info;

pub use frame::FrameCoder;
pub use multiplex::YamuxCtrl;
pub use tls::{TlsClientConnector, TlsServerAcceptor};

use crate::{CommandRequest, CommandResponse, KvError, Service};
use crate::network::stream::ProstStream;
use crate::network::stream_result::StreamResult;

mod frame;
mod stream;
mod tls;
mod multiplex;
mod stream_result;

// handle the read/write of a socket accepted by the server
pub struct ProstServerStream<S> {
    inner: ProstStream<S, CommandRequest, CommandResponse>,
    service: Service,
}

// handle the read/write of a socket by the client
pub struct ProstClientStream<S> {
    inner: ProstStream<S, CommandResponse, CommandRequest>,
}

impl<S> ProstServerStream<S>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
{
    pub fn new(stream: S, service: Service) -> Self {
        Self { inner: ProstStream::new(stream), service }
    }

    pub async fn process(mut self) -> Result<(), KvError> {
        let stream = &mut self.inner;
        while let Some(Ok(request)) = stream.next().await {
            info!("received request: {:?}", request);
            let mut response = self.service.execute(request);
            while let Some(data) = response.next().await {
                stream.send(&data).await.unwrap();
            }
        }
        Ok(())
    }
}

impl<S> ProstClientStream<S>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    pub fn new(stream: S) -> Self {
        Self { inner: ProstStream::new(stream) }
    }

    pub async fn execute_unary(&mut self, request: &CommandRequest) -> Result<CommandResponse, KvError> {
        let stream = &mut self.inner;
        stream.send(request).await?;

        match stream.next().await {
            Some(response) => response,
            None => Err(KvError::Internal("Did not receive response".into())),
        }
    }

    pub async fn execute_streaming(self, request: &CommandRequest) -> Result<StreamResult, KvError> {
        let mut stream = self.inner;
        stream.send(request).await?;
        stream.close().await?;

        StreamResult::new(stream).await
    }
}

#[cfg(test)]
pub mod utils {
    use std::task::Poll;

    use bytes::{BufMut, BytesMut};
    use tokio::io::{AsyncRead, AsyncWrite};

    #[derive(Default)]
    pub struct DummyStream {
        pub buf: BytesMut,
    }

    impl AsyncRead for DummyStream {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let len = buf.capacity();

            let data = self.get_mut().buf.split_to(len);

            buf.put_slice(&data);
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for DummyStream {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            self.get_mut().buf.put_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
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
        let response = client.execute_unary(&request).await?;

        // first time, response should be default value
        assert_response_ok(&response, &[Value::default()], &[]);

        // another HSET
        let request = CommandRequest::new_hset("table", "key", "value2".into());
        let response = client.execute_unary(&request).await?;

        // second time, response should be the first value
        assert_response_ok(&response, &["value".into()], &[]);

        Ok(())
    }

    #[tokio::test]
    async fn client_server_compression_should_work() -> anyhow::Result<()> {
        let addr = start_server().await?;

        let stream = TcpStream::connect(addr).await?;
        let mut client = ProstClientStream::new(stream);

        let v: Value = Bytes::from(vec![0u8; 16384]).into();
        let request = CommandRequest::new_hset("table", "key", v.clone().into());
        let response = client.execute_unary(&request).await?;

        assert_response_ok(&response, &[Value::default()], &[]);

        let request = CommandRequest::new_hget("table", "key");
        let response = client.execute_unary(&request).await?;

        // second time, response should be the first value
        assert_response_ok(&response, &[v.into()], &[]);

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
