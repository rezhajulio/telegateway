// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use telegateway::{app, config, register};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, Parser)]
#[command(
    name = "telegateway",
    version,
    about = "Reverse-proxy gateway for Telegram bots: webhook ingress, per-chat rate limiting, structured observability."
)]
struct Cli {
    /// Path to the TOML configuration file.
    #[arg(long, default_value = "config.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer().json())
        .init();

    let cfg = config::load(&cli.config)
        .with_context(|| format!("failed to load config from {}", cli.config.display()))?;
    let router = app::build_router(cfg.clone());

    let addr: SocketAddr = cfg
        .listen
        .parse()
        .context("invalid `listen` address in config")?;

    if cfg.auto_register {
        let cfg = cfg.clone();
        tokio::spawn(async move {
            register::register_all(&cfg, register::TELEGRAM_API_BASE).await;
        });
    }

    match &cfg.tls {
        Some(tls) => {
            let rustls_config =
                axum_server::tls_rustls::RustlsConfig::from_pem_file(&tls.cert_path, &tls.key_path)
                    .await
                    .context("failed to load TLS certificate/key")?;
            tracing::info!(%addr, bots = cfg.bots.len(), "telegateway listening (https)");
            axum_server::bind_rustls(addr, rustls_config)
                .serve(router.into_make_service())
                .await?;
        }
        None => {
            tracing::info!(%addr, bots = cfg.bots.len(), "telegateway listening (http)");
            let listener = tokio::net::TcpListener::bind(addr).await?;
            axum::serve(listener, router).await?;
        }
    }
    Ok(())
}
