use anyhow::Result;
use async_prost::AsyncProstStream;
use futures::prelude::*;
use tokio::net::TcpStream;

use kv::{CommandRequest, CommandResponse};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "127.0.0.1:9527";
    // connect to server
    let stream = TcpStream::connect(addr).await?;

    // use AsyncProstStream to handle the TCP Frame
    let mut client =
        AsyncProstStream::<_, CommandResponse, CommandRequest, _>::from(stream).for_async();

    // send a HSET request
    let cmd = CommandRequest::new_hset("table1", "hello", "world".into());
    client.send(cmd).await?;

    if let Some(Ok(data)) = client.next().await {
        println!("received response: {:?}", data);
    }

    Ok(())
}
