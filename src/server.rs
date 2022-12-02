use anyhow::Result;
use tokio::net::{TcpListener, TcpStream};
use tracing::info;
use kv::{MemTable, ProstServerStream, Service, ServiceInner};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "127.0.0.1:9527";
    let service: Service = ServiceInner::new(MemTable::new()).into();
    let listener = TcpListener::bind(addr).await?;
    info!("Listening on {}", addr);

    loop {
        let (stream, addr) = listener.accept().await?;
        info!("Got connection from {:?}", addr);
        let service = service.clone();
        let stream = ProstServerStream::new(stream, service.clone());
        tokio::spawn(async move {
            stream.process().await
        });
    }

}