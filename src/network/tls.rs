use std::io::Cursor;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::{client, server, TlsAcceptor, TlsConnector};
use tokio_rustls::rustls::{AllowAnyAuthenticatedClient, Certificate, ClientConfig, NoClientAuth, PrivateKey, RootCertStore, ServerConfig};
use tokio_rustls::rustls::internal::pemfile;
use tokio_rustls::webpki::{DNSNameRef};
use crate::KvError;

// KV server's own ALPN (Application Layer Protocol Negotiation)
const ALPN_KV: &str = "kv";

// Has a TLS ServerConfig, and have a method `accept` to convert lower protocol to TLS
#[derive(Clone)]
pub struct TlsServerAcceptor {
    inner: Arc<ServerConfig>,
}

// Has a TLS Client, and have a method `connect` to convert lower protocol to TLS
#[derive(Clone)]
pub struct TlsClientConnector {
    pub config: Arc<ClientConfig>,
    pub domain: Arc<String>
}

impl TlsClientConnector {
    // load client cert/CA cert, generate the ClientConfig
    pub fn new(
        domain: impl Into<String>,
        identity: Option<(&str, &str)>,
        server_ca: Option<&str>,
    ) -> Result<Self, KvError> {
        let mut config = ClientConfig::new();

        if let Some((cert, key)) = identity {
            let certs = load_certs(cert)?;
            let key = load_key(key)?;
            config.set_single_client_cert(certs, key)?;
        }

        config.root_store = match rustls_native_certs::load_native_certs() {
            Ok(store) | Err((Some(store), _)) => store,
            Err((None, error)) => return Err(error.into()),
        };

        if let Some(cert) = server_ca {
            let mut buf = Cursor::new(cert);
            config.root_store.add_pem_file(&mut buf).unwrap();
        }
        Ok(Self {
            config: Arc::new(config),
            domain: Arc::new(domain.into()),
        })
    }

    // trigger TLS protocol, convert lower level stream to TLS stream
    pub async fn connect<S>(&self, stream: S) -> Result<client::TlsStream<S>, KvError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let dns = DNSNameRef::try_from_ascii_str(&self.domain)
            .map_err(|_| KvError::Internal("Invalid Dns name".into()))?;
        let stream = TlsConnector::from(self.config.clone())
            .connect(dns, stream)
            .await?;
        Ok(stream)
    }
}

impl TlsServerAcceptor {
    // load server cert/CA cert, generate the ServerConfig
    pub fn new(
        cert: &str,
        key: &str,
        client_ca: Option<&str>,
    ) -> Result<Self, KvError> {
        let certs = load_certs(cert)?;
        let key = load_key(key)?;
        let mut config = match client_ca {
            None => ServerConfig::new(NoClientAuth::new()),
            Some(ca) => {
                let mut buf = Cursor::new(ca);
                let mut store = RootCertStore::empty();
                store.add_pem_file(&mut buf).map_err(|_| KvError::CertificateParseError("CA", "cert"))?;
                ServerConfig::new(AllowAnyAuthenticatedClient::new(store))
            }
        };

        config.set_single_cert(certs, key)
            .map_err(|_| KvError::CertificateParseError("server", "cert"))?;
        config.set_protocols(&[Vec::from(&ALPN_KV[..])]);

        Ok(Self {
            inner: Arc::new(config),
        })
    }

    // trigger TLS protocol, convert lower level stream to TLS stream
    pub async fn accept<S>(&self, stream: S) -> Result<server::TlsStream<S>, KvError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let stream = TlsAcceptor::from(self.inner.clone())
            .accept(stream)
            .await?;
        Ok(stream)
    }
}

fn load_certs(cert: &str) -> Result<Vec<Certificate>, KvError> {
    let mut cert = Cursor::new(cert);
    pemfile::certs(&mut cert)
        .map_err(|_| KvError::CertificateParseError("server", "cert"))
}

fn load_key(key: &str) -> Result<PrivateKey, KvError> {
    let mut key = Cursor::new(key);

    // try PKCS8 to load private key first
    if let Ok(mut keys) = pemfile::pkcs8_private_keys(&mut key) {
        if !keys.is_empty() {
            return Ok(keys.remove(0));
        }
    }

    // try RSA key to load private key
    key.set_position(0);
    if let Ok(mut keys) = pemfile::rsa_private_keys(&mut key) {
        if !keys.is_empty() {
            return Ok(keys.remove(0));
        }
    }

    Err(KvError::CertificateParseError("private", "key"))
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use super::*;

    use anyhow::Result;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    const CA_CERT: &str = include_str!("../../fixtures/ca.cert");
    const SERVER_CERT: &str = include_str!("../../fixtures/server.cert");
    const SERVER_KEY: &str = include_str!("../../fixtures/server.key");
    const CLIENT_CERT: &str = include_str!("../../fixtures/client.cert");
    const CLIENT_KEY: &str = include_str!("../../fixtures/client.key");

    #[tokio::test]
    async fn tls_should_work() {
        let ca = Some(CA_CERT);
        let addr = start_server(None).await.unwrap();

        let connector = TlsClientConnector::new("kvserver.acme.inc", None, ca).unwrap();
        let stream = TcpStream::connect(addr).await.unwrap();
        let mut stream = connector.connect(stream).await.unwrap();
        stream.write_all(b"hello world!").await.unwrap();
        let mut buf = [0; 12];
        stream.read_exact(&mut buf).await.unwrap();

        assert_eq!(b"hello world!", &buf);
    }

    #[tokio::test]
    async fn tls_with_client_cert_should_work() {
        let ca = Some(CA_CERT);
        let addr = start_server(None).await.unwrap();

        let connector = TlsClientConnector::new("kvserver.acme.inc", Some((CLIENT_CERT, CLIENT_KEY)), ca).unwrap();
        let stream = TcpStream::connect(addr).await.unwrap();
        let mut stream = connector.connect(stream).await.unwrap();
        stream.write_all(b"hello world!").await.unwrap();
        let mut buf = [0; 12];
        stream.read_exact(&mut buf).await.unwrap();

        assert_eq!(b"hello world!", &buf);
    }

    #[tokio::test]
    async fn tls_with_bad_domain_should_fail() {
        let ca = Some(CA_CERT);
        let addr = start_server(None).await.unwrap();

        let connector = TlsClientConnector::new("kvserver1.acme.inc", None, ca).unwrap();
        let stream = TcpStream::connect(addr).await.unwrap();
        let mut result = connector.connect(stream).await;

        assert!(result.is_err());
    }


    async fn start_server(ca: Option<&str>) -> Result<SocketAddr> {
        let acceptor = TlsServerAcceptor::new(SERVER_CERT, SERVER_KEY, ca)?;
        let echo = TcpListener::bind("127.0.0.1:0").await?;
        let addr = echo.local_addr()?;

        tokio::spawn(async move {
            let (stream, _) = echo.accept().await.unwrap();
            let mut stream = acceptor.accept(stream).await.unwrap();
            let mut buf = [0; 12];
            stream.read_exact(&mut buf).await.unwrap();
            stream.write_all(&buf).await.unwrap();
        });

        Ok(addr)
    }
}