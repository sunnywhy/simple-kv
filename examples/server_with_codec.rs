use anyhow::Result;
use bytes::BytesMut;
use futures::prelude::*;
use prost::Message;
use tokio::net::TcpListener;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::info;

use kv::{CommandRequest, MemTable, Service, ServiceInner};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let service: Service = ServiceInner::new(MemTable::new()).into();

    let addr = "127.0.0.1:9527";
    let listener = TcpListener::bind(addr).await?;
    info!("Listening on: {}", addr);

    loop {
        let (stream, addr) = listener.accept().await?;
        info!("Accepted connection from: {:?}", addr);

        let service_cloned = service.clone();
        tokio::spawn(async move {
            let mut stream =
                Framed::new(stream, LengthDelimitedCodec::new());
            while let Some(Ok(mut buf)) = stream.next().await {
                let cmd = CommandRequest::decode(&mut buf).unwrap();
                info!("Received command: {:?}", cmd);
                let mut resp = service_cloned.execute(cmd);
                buf.clear();

                while let Some(data) = resp.next().await {
                    let mut buf = BytesMut::new();
                    data.encode(&mut buf).unwrap();
                    stream.send(buf.freeze()).await.unwrap();
                }
            }
            info!("Connection closed {:?}", addr);
        });
    }
}