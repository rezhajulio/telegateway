// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
//! HTTPS serving with a self-signed certificate.
use std::net::SocketAddr;

use telegateway::{app, config};

/// Generate a self-signed certificate for localhost and return the paths
/// of the PEM files, holding the TempDir for cleanup.
fn self_signed_pems() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let certified_key = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, certified_key.cert.pem()).unwrap();
    std::fs::write(&key_path, certified_key.key_pair.serialize_pem()).unwrap();
    (dir, cert_path, key_path)
}

/// Ask the OS for a free port.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

#[tokio::test]
async fn serves_https_when_tls_configured() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let (_dir, cert_path, key_path) = self_signed_pems();
    let cert_str = cert_path.to_str().unwrap();
    let key_str = key_path.to_str().unwrap();
    let port = free_port();
    let db_dir = tempfile::tempdir().unwrap();
    let db = db_dir.path().join("outbox.db");
    let db = db.to_str().unwrap();
    let toml = format!(
        r#"
listen = "127.0.0.1:{port}"

[outbox]
path = "{db}"

[tls]
cert_path = "{cert_str}"
key_path = "{key_str}"

[[bots]]
id = "demo"
downstream_url = "http://127.0.0.1:1/webhook"
webhook_secret = "s3cret"
"#
    );
    let cfg = config::load_from_str(&toml).unwrap();
    let router = app::build_router(cfg.clone()).await.unwrap().router;
    let tls = cfg.tls.clone().unwrap();

    let rustls_config =
        axum_server::tls_rustls::RustlsConfig::from_pem_file(&tls.cert_path, &tls.key_path)
            .await
            .unwrap();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    tokio::spawn(async move {
        axum_server::bind_rustls(addr, rustls_config)
            .serve(router.into_make_service())
            .await
            .unwrap();
    });
    // Give the server a moment to bind.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();
    let resp = client
        .get(format!("https://127.0.0.1:{port}/healthz"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}
#[tokio::test]
async fn rejects_garbage_pem() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let dir = tempfile::tempdir().unwrap();
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, "not a certificate").unwrap();
    std::fs::write(&key_path, "not a key").unwrap();

    let result = axum_server::tls_rustls::RustlsConfig::from_pem_file(&cert_path, &key_path).await;
    assert!(result.is_err());
}
