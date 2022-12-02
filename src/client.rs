use anyhow::Result;
use tokio::net::TcpStream;
use tracing::info;
use kv::{CommandRequest, ProstClientStream};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "127.0.0.1:9527";

    let stream = TcpStream::connect(addr).await?;
    let mut client = ProstClientStream::new(stream);

    // send HSET, wait for response
    let request = CommandRequest::new_hset("table", "key", "value".into());

    let response = client.execute(request).await?;
    info!("Got response {:?}", response);

    Ok(())
}
