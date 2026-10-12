// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use telegateway::{app, config};
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
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(
        %addr,
        bots = cfg.bots.len(),
        "telegateway listening"
    );
    axum::serve(listener, router).await?;
    Ok(())
}
