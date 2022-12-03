use anyhow::Result;
use tokio::net::TcpStream;
use tracing::info;
use kv::{CommandRequest, ProstClientStream, TlsClientConnector};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let ca_cert = include_str!("../fixtures/ca.cert");

    let addr = "127.0.0.1:9527";

    let connector = TlsClientConnector::new("kvserver.acme.inc", None, Some(ca_cert))?;
    let stream = TcpStream::connect(addr).await?;
    let stream = connector.connect(stream).await?;
    let mut client = ProstClientStream::new(stream);

    // send HSET, wait for response
    let request = CommandRequest::new_hset("table", "key", "value".into());

    let response = client.execute(request).await?;
    info!("Got response {:?}", response);

    Ok(())
}
