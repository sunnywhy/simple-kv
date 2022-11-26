use anyhow::Result;
use async_prost::AsyncProstStream;
use futures::prelude::*;
use tokio::net::TcpListener;
use tracing::info;

use kv::{CommandRequest, CommandResponse, Service, SledDb, ServiceInner};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let service: Service<SledDb> = ServiceInner::new(SledDb::new("/tmp/kvserver"))
        .fn_before_send(|resp| match resp.message.as_ref() {
            "" => resp.message = "altered. Original Message is empty.".into(),
            s => resp.message = format!("altered: {}.", s),
        })
        .into();

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
                let resp = service_cloned.execute(cmd);

                stream.send(resp).await.unwrap();
            }
            info!("Connection closed {}", addr);
        });
    }
}
