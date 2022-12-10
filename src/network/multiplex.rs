use std::future::Future;
use std::marker::PhantomData;

use futures::{future, TryStreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};
use yamux::{Config, Connection, ConnectionError, Control, Mode, WindowUpdateMode};

/// Yamux control structure
pub struct YamuxCtrl<S> {
    /// yamux control, use it to create new stream
    ctrl: Control,
    _conn: PhantomData<S>,
}

impl<S> YamuxCtrl<S>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static
{
    /// create yamux client
    pub fn new_client(stream: S, config: Option<Config>) -> Self {
        Self::new(stream, config, true, |_stream| future::ready(Ok(())))
    }

    /// create yamux server, we need to handle the stream in the serverside
    pub fn new_server<F, Fut>(stream: S, config: Option<Config>, f: F) -> Self
        where
            F: FnMut(yamux::Stream) -> Fut,
            F: Send + 'static,
            Fut: Future<Output=Result<(), ConnectionError>> + Send + 'static,
    {
        Self::new(stream, config, false, f)
    }

    fn new<F, Fut>(stream: S, config: Option<Config>, is_client: bool, f: F) -> Self
        where
            F: FnMut(yamux::Stream) -> Fut,
            F: Send + 'static,
            Fut: Future<Output=Result<(), ConnectionError>> + Send + 'static,
    {
        let mode = if is_client {
            Mode::Client
        } else {
            Mode::Server
        };

        // create config
        let mut config = config.unwrap_or_default();
        config.set_window_update_mode(WindowUpdateMode::OnRead);

        // yamux:Stream used futures's strait, so we need to compat() to tokio's trait
        let conn = Connection::new(stream.compat(), config, mode);

        let ctrl = conn.control();

        // pull data from all stream
        tokio::spawn(yamux::into_stream(conn).try_for_each_concurrent(None, f));

        Self {
            ctrl,
            _conn: PhantomData::default(),
        }
    }

    pub async fn open_stream(&mut self) -> Result<Compat<yamux::Stream>, ConnectionError> {
        let stream = self.ctrl.open_stream().await?;
        Ok(stream.compat())
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use anyhow::Result;
    use futures::AsyncReadExt;
    use tokio::net::{TcpListener, TcpStream};
    use tokio_rustls::server;
    use tracing::warn;

    use crate::{assert_response_ok, CommandRequest, KvError, MemTable, ProstClientStream, ProstServerStream, Service, ServiceInner, Storage, TlsServerAcceptor};
    use crate::network::tls::tls_utils::{tls_acceptor, tls_connector};
    use crate::utils::DummyStream;

    use super::*;

    pub async fn start_server_with<Store>(
        addr: &str,
        tls: TlsServerAcceptor,
        store: Store,
        f: impl Fn(server::TlsStream<TcpStream>, Service) + Send + Sync + 'static,
    ) -> Result<SocketAddr, KvError>
        where
            Store: Storage,
            Service: From<ServiceInner<Store>>
    {
        let listener = TcpListener::bind(addr).await?;
        let addr = listener.local_addr()?;
        let service: Service = ServiceInner::new(store).into();

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => match tls.accept(stream).await {
                        Ok(stream) => f(stream, service.clone()),
                        Err(e) => {
                            warn!("tls accept error: {:?}", e);
                        }
                    },
                    Err(e) => {
                        warn!("accept error: {:?}", e);
                    }
                }
            }
        });

        Ok(addr)
    }

    // create a yamux server
    pub async fn start_yamux_server<Store>(
        addr: &str,
        tls: TlsServerAcceptor,
        store: Store,
    ) -> Result<SocketAddr, KvError>
        where
            Store: Storage,
            Service: From<ServiceInner<Store>>
    {
        let f = |stream, service: Service| {
            YamuxCtrl::new_server(stream, None, move |s| {
                let svc = service.clone();
                async move {
                    let stream = ProstServerStream::new(s.compat(), svc);
                    stream.process().await.unwrap();
                    Ok(())
                }
            });
        };
        start_server_with(addr, tls, store, f).await
    }

    #[tokio::test]
    async fn yamux_ctrl_client_server_should_work() -> Result<()> {
        // create yamux server that uses TLS
        let acceptor = tls_acceptor(false)?;
        let addr = start_yamux_server("127.0.0.1:0", acceptor, MemTable::new()).await?;

        let connector = tls_connector(false)?;
        let stream = TcpStream::connect(addr).await?;
        let stream = connector.connect(stream).await?;

        // create yamux client that uses TLS
        let mut ctrl = YamuxCtrl::new_client(stream, None);

        // open a new yamux stream from the client ctrl
        let stream = ctrl.open_stream().await?;
        let mut client = ProstClientStream::new(stream);
        let cmd = CommandRequest::new_hset("t1", "k1", "v1".into());
        client.execute_unary(&cmd).await.unwrap();

        let cmd = CommandRequest::new_hget("t1", "k1");
        let res = client.execute_unary(&cmd).await.unwrap();
        assert_response_ok(&res, &["v1".into()], &[]);

        Ok(())
    }
}