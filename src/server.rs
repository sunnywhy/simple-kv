use anyhow::Result;
use tokio::net::{TcpListener};
use tracing::info;
use kv::{MemTable, ProstServerStream, Service, ServiceInner, TlsServerAcceptor};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let server_cert = include_str!("../fixtures/server.cert");
    let server_key = include_str!("../fixtures/server.key");

    let addr = "127.0.0.1:9527";
    let acceptor = TlsServerAcceptor::new(server_cert, server_key, None)?;
    let service: Service = ServiceInner::new(MemTable::new()).into();
    let listener = TcpListener::bind(addr).await?;
    info!("Listening on {}", addr);

    loop {
        let tls = acceptor.clone();
        let (stream, addr) = listener.accept().await?;
        info!("Got connection from {:?}", addr);
        let stream = tls.accept(stream).await?;
        let stream = ProstServerStream::new(stream, service.clone());
        tokio::spawn(async move {
            stream.process().await
        });
    }

}