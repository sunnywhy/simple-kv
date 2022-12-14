use anyhow::Result;
use async_prost::AsyncProstStream;
use futures::prelude::*;
use tokio::net::TcpListener;
use tracing::info;

use kv::{CommandRequest, CommandResponse, MemTable, Service, ServiceInner};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let service: Service = ServiceInner::new(MemTable::new()).into();

    let addr = "127.0.0.1:9527";
    let listener = TcpListener::bind(addr).await?;
    info!("Listening on: {}", addr);

    loop {
        let (stream, addr) = listener.accept().await?;
        info!("Accepted connection from: {}", addr);

        let service_cloned = service.clone();
        tokio::spawn(async move {
            let mut stream =
                AsyncProstStream::<_, CommandRequest, CommandResponse, _>::from(stream).for_async();
            while let Some(Ok(cmd)) = stream.next().await {
                info!("Received command: {:?}", cmd);
                let mut resp = service_cloned.execute(cmd);
                while let Some(data) = resp.next().await {
                    stream.send((*data).clone()).await.unwrap();
                }
            }
            info!("Connection closed {}", addr);
        });
    }
}
