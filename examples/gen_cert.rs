use anyhow::Result;
use certify::{CA, CertType, generate_ca, generate_cert, load_ca};

struct CertPem {
    cert_type: CertType,
    cert: String,
    key: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let pem = create_ca()?;
    gen_files(&pem).await?;
    let ca = load_ca(&pem.cert, &pem.key)?;
    let pem = create_cert(&ca, &["kvserver.acme.inc"], "Acme KV server", false)?;
    gen_files(&pem).await?;
    let pem = create_cert(&ca, &[], "awesome-device-id", true)?;
    gen_files(&pem).await?;

    Ok(())
}

fn create_ca() -> Result<CertPem> {
    let (cert, key) = generate_ca(
        &["acme.inc"],
        "Canada",
        "Acme Inc",
        "Acme CA",
        None,
        Some(10 * 365)
    )?;
    Ok(CertPem { cert_type: CertType::CA, cert, key })
}

fn create_cert(ca: &CA, domains: &[&str], cn: &str, is_client: bool) -> Result<CertPem> {
    let (days, cert_type) = if is_client {
        (Some(365), CertType::Client)
    } else {
        (Some(5 * 365), CertType::Server)
    };
    let (cert, key) = generate_cert(
        ca,
        domains,
        "Canada",
        "Acme Inc",
        cn,
        None,
        is_client,
        days
    )?;
    Ok(CertPem { cert_type, cert, key })
}

async fn gen_files(pem: &CertPem) -> Result<()> {
    let name = match pem.cert_type {
        CertType::CA => "ca",
        CertType::Server => "server",
        CertType::Client => "client",
    };
    let cert_path = format!("fixtures/{}.cert", name);
    let key_path = format!("fixtures/{}.key", name);
    tokio::fs::write(cert_path, pem.cert.as_bytes()).await?;
    tokio::fs::write(key_path, pem.key.as_bytes()).await?;
    Ok(())
}