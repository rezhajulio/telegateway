// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use telegateway::{app, config, otel, outbox, register};
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

    let cfg = config::load(&cli.config)
        .with_context(|| format!("failed to load config from {}", cli.config.display()))?;

    let mut telemetry = otel::init(cfg.otel.as_ref()).context("failed to set up OpenTelemetry")?;
    // The OpenTelemetry layer only implements `Layer<Registry>`, so it
    // must be attached before the filter and fmt layers.
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    match telemetry.take_tracing_layer() {
        Some(otel_layer) => tracing_subscriber::registry()
            .with(otel_layer)
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer().json())
            .init(),
        None => tracing_subscriber::registry()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer().json())
            .init(),
    };

    let built = app::build_router(cfg.clone())
        .await
        .context("failed to build gateway")?;

    for dispatcher in built.dispatchers {
        tokio::spawn(async move { dispatcher.run().await });
    }
    tokio::spawn(outbox::dispatch::janitor(
        built.store.clone(),
        cfg.outbox.delivered_retention_days,
    ));
    // Keep the `telegateway.outbox.pending` gauge fresh. Records no-op
    // instruments when `[otel]` is absent.
    let gauge_store = built.store.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(60));
        loop {
            tick.tick().await;
            if let Ok(counts) = gauge_store.pending_counts().await {
                let metrics = otel::metrics();
                for (bot_id, n) in counts {
                    metrics
                        .outbox_pending
                        .record(n as u64, &otel::bot_attrs(&bot_id));
                }
            }
        }
    });
    let router = built.router;

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
            let handle = axum_server::Handle::new();
            let shutdown_handle = handle.clone();
            tokio::spawn(async move {
                shutdown_signal().await;
                shutdown_handle.graceful_shutdown(Some(Duration::from_secs(30)));
            });
            axum_server::bind_rustls(addr, rustls_config)
                .handle(handle)
                .serve(router.into_make_service())
                .await?;
        }
        None => {
            tracing::info!(%addr, bots = cfg.bots.len(), "telegateway listening (http)");
            let listener = tokio::net::TcpListener::bind(addr).await?;
            axum::serve(listener, router)
                .with_graceful_shutdown(shutdown_signal())
                .await?;
        }
    }

    // Flush pending telemetry before exit. Best-effort: shutdown failures
    // are already logged by the SDK and must not fail the process.
    telemetry.shutdown();
    Ok(())
}

/// Resolve when the process should shut down: SIGTERM or SIGINT on Unix,
/// Ctrl-C elsewhere.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("failed to install SIGTERM handler");
        let mut interrupt =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                .expect("failed to install SIGINT handler");
        tokio::select! {
            _ = terminate.recv() => {},
            _ = interrupt.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl-C handler");
    }
}
