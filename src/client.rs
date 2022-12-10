use std::time::Duration;

use anyhow::Result;
use futures::StreamExt;
use tokio::net::TcpStream;
use tokio::time;
use tokio_util::compat::Compat;
use tracing::info;

use kv::{CommandRequest, KvError, ProstClientStream, TlsClientConnector, YamuxCtrl};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let ca_cert = include_str!("../fixtures/ca.cert");

    let addr = "127.0.0.1:9527";

    let connector = TlsClientConnector::new("kvserver.acme.inc", None, Some(ca_cert))?;
    let stream = TcpStream::connect(addr).await?;
    let stream = connector.connect(stream).await?;

    let mut ctrl = YamuxCtrl::new_client(stream, None);

    let channel = "lobby";
    start_publishing(ctrl.open_stream().await?, channel)?;

    let stream = ctrl.open_stream().await?;
    let mut client = ProstClientStream::new(stream);

    // send HSET, wait for response
    let request = CommandRequest::new_hset("table", "key", "value".into());

    let response = client.execute_unary(&request).await?;
    info!("Got response {:?}", response);

    // generate a Subscribe command
    let request = CommandRequest::new_subscribe(channel);
    let mut stream = client.execute_streaming(&request).await?;
    let id = stream.id;
    start_unsubscribe(ctrl.open_stream().await?, channel, id)?;

    while let Some(Ok(data)) = stream.next().await {
        println!("Got published data: {:?}", data);
    }

    Ok(())
}

fn start_publishing(stream: Compat<yamux::Stream>, name: &str) -> Result<(), KvError> {
    let cmd = CommandRequest::new_publish(name, vec![1.into(), 2.into(), "hello".into()]);
    tokio::spawn(async move {
        time::sleep(Duration::from_millis(1000)).await;
        let mut client = ProstClientStream::new(stream);
        let res = client.execute_unary(&cmd).await.unwrap();
        println!("Finished publishing: {:?}", res);
    });

    Ok(())
}

fn start_unsubscribe(stream: Compat<yamux::Stream>, name: &str, id: u32) -> Result<(), KvError> {
    let cmd = CommandRequest::new_unsubscribe(name, id as _);
    tokio::spawn(async move {
        time::sleep(Duration::from_millis(2000)).await;
        let mut client = ProstClientStream::new(stream);
        let res = client.execute_unary(&cmd).await.unwrap();
        println!("Finished unsubscribing: {:?}", res);
    });

    Ok(())
}