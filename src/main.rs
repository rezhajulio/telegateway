// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
//! The `telegateway` binary: loads the config, wires up routing,
//! dispatchers, and telemetry, then serves until SIGTERM/SIGINT.
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use telegateway::{app, config, otel, outbox, register};
use tokio::sync::watch;
use tokio::task::JoinSet;
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
    // Install rustls ring crypto provider early before any TLS use.
    let _ = rustls::crypto::ring::default_provider().install_default();

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

    let run_res = run_gateway(cfg).await;

    // Flush pending telemetry before exit. Best-effort: shutdown failures
    // are already logged by the SDK and must not fail the process.
    telemetry.shutdown();
    run_res
}

async fn run_gateway(cfg: config::Config) -> anyhow::Result<()> {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let built = app::build_router(cfg.clone())
        .await
        .context("failed to build gateway")?;

    let mut workers: JoinSet<anyhow::Result<&'static str>> = JoinSet::new();

    // Spawn dispatchers (supervision: exiting before shutdown is an error)
    for dispatcher in built.dispatchers {
        let rx = shutdown_rx.clone();
        workers.spawn(async move {
            dispatcher.run(rx).await;
            Ok("dispatcher")
        });
    }

    // Spawn janitor
    let janitor_store = built.store.clone();
    let janitor_days = cfg.outbox.delivered_retention_days;
    let janitor_rx = shutdown_rx.clone();
    workers.spawn(async move {
        outbox::dispatch::janitor(janitor_store, janitor_days, janitor_rx).await;
        Ok("janitor")
    });

    // Gauge task: refresh telegateway.outbox.pending, recording 0 for absent bots
    let gauge_store = built.store.clone();
    let gauge_bots: Vec<String> = cfg.bots.iter().map(|b| b.id.clone()).collect();
    let mut gauge_shutdown_rx = shutdown_rx.clone();
    workers.spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(60));
        loop {
            tokio::select! {
                _ = tick.tick() => {
                    match gauge_store.pending_counts().await {
                        Ok(counts) => {
                            let metrics = otel::metrics();
                            let counts_map: std::collections::HashMap<_, _> = counts.into_iter().collect();
                            for bot_id in &gauge_bots {
                                let n = counts_map.get(bot_id).copied().unwrap_or(0);
                                metrics
                                    .outbox_pending
                                    .record(n as u64, &otel::bot_attrs(bot_id));
                            }
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "failed to fetch outbox pending counts");
                        }
                    }
                }
                _ = gauge_shutdown_rx.changed() => {
                    if *gauge_shutdown_rx.borrow() {
                        break;
                    }
                }
            }
        }
        Ok("gauge")
    });

    // Policy prune task: hourly call to built.policy.prune()
    let policy = built.policy.clone();
    let mut policy_shutdown_rx = shutdown_rx.clone();
    workers.spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(3600));
        loop {
            tokio::select! {
                _ = tick.tick() => {
                    policy.prune();
                }
                _ = policy_shutdown_rx.changed() => {
                    if *policy_shutdown_rx.borrow() {
                        break;
                    }
                }
            }
        }
        Ok("policy_prune")
    });

    let router = built.router;
    let admin_router = built.admin_router;
    let store = built.store.clone();

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

    // Spawn SIGHUP reload task if TLS configured
    let rustls_config = if let Some(tls) = &cfg.tls {
        let r_cfg =
            axum_server::tls_rustls::RustlsConfig::from_pem_file(&tls.cert_path, &tls.key_path)
                .await
                .context("failed to load TLS certificate/key")?;
        spawn_sighup_reload(r_cfg.clone(), tls.cert_path.clone(), tls.key_path.clone());
        Some(r_cfg)
    } else {
        None
    };

    let main_shutdown_rx = shutdown_rx.clone();
    let admin_shutdown_rx = shutdown_rx.clone();

    // Build admin server future if admin listener is configured
    let admin_server_fut = if let Some(admin_r) = admin_router {
        let admin_listen = cfg
            .admin
            .as_ref()
            .and_then(|a| a.listen.as_deref())
            .context("admin_router present but cfg.admin.listen missing")?;
        let admin_addr: SocketAddr = admin_listen
            .parse()
            .context("invalid admin.listen address in config")?;
        let admin_tls = rustls_config.clone();
        Some(serve_router(
            admin_addr,
            admin_r,
            admin_tls,
            admin_shutdown_rx,
            "admin",
        ))
    } else {
        None
    };

    let main_server_fut = serve_router(addr, router, rustls_config, main_shutdown_rx, "main");

    // Run server(s) and supervisor concurrently
    let mut unexpected_exit: Option<String> = None;

    tokio::select! {
        sig = shutdown_signal() => {
            tracing::info!(signal = sig, "shutdown requested; draining workers");
        }
        res = main_server_fut => {
            if let Err(e) = res {
                tracing::error!(error = %e, "main server exited with error");
                unexpected_exit = Some(format!("main server error: {e}"));
            } else {
                unexpected_exit = Some("main server exited prematurely".to_owned());
            }
        }
        res = async {
            if let Some(fut) = admin_server_fut {
                fut.await
            } else {
                std::future::pending().await
            }
        } => {
            if let Err(e) = res {
                tracing::error!(error = %e, "admin server exited with error");
                unexpected_exit = Some(format!("admin server error: {e}"));
            } else {
                unexpected_exit = Some("admin server exited prematurely".to_owned());
            }
        }
        Some(worker_res) = workers.join_next() => {
            match worker_res {
                Ok(Ok(name)) => {
                    tracing::error!(worker = name, "worker exited unexpectedly before shutdown");
                    unexpected_exit = Some(format!("worker '{name}' exited prematurely"));
                }
                Ok(Err(e)) => {
                    tracing::error!(error = %e, "worker exited with error before shutdown");
                    unexpected_exit = Some(format!("worker error: {e}"));
                }
                Err(e) => {
                    tracing::error!(error = %e, "worker task panicked before shutdown");
                    unexpected_exit = Some(format!("worker panicked: {e}"));
                }
            }
        }
    }

    // Signal shutdown to all workers and servers
    let _ = shutdown_tx.send(true);

    // Join workers with bounded 35s timeout
    let drain_timeout = Duration::from_secs(35);
    let drain = async {
        while let Some(res) = workers.join_next().await {
            if let Err(e) = res {
                tracing::warn!(error = %e, "worker join error during shutdown");
            }
        }
    };

    if tokio::time::timeout(drain_timeout, drain).await.is_err() {
        tracing::warn!("timeout joining workers on shutdown; aborting remaining");
        workers.abort_all();
    }

    // Close outbox database pool
    store.close().await;

    if let Some(reason) = unexpected_exit {
        anyhow::bail!("{reason}");
    }

    Ok(())
}

async fn serve_router(
    addr: SocketAddr,
    router: axum::Router,
    rustls_config: Option<axum_server::tls_rustls::RustlsConfig>,
    mut shutdown_rx: watch::Receiver<bool>,
    name: &'static str,
) -> anyhow::Result<()> {
    let wait_shutdown = async move {
        while !*shutdown_rx.borrow_and_update() {
            if shutdown_rx.changed().await.is_err() {
                break;
            }
        }
    };

    match rustls_config {
        Some(rc) => {
            tracing::info!(%addr, server = name, "listening (https)");
            let handle = axum_server::Handle::new();
            let shutdown_handle = handle.clone();
            tokio::spawn(async move {
                wait_shutdown.await;
                shutdown_handle.graceful_shutdown(Some(Duration::from_secs(30)));
            });
            axum_server::bind_rustls(addr, rc)
                .handle(handle)
                .serve(router.into_make_service())
                .await
                .with_context(|| format!("{name} https server failed"))?;
        }
        None => {
            tracing::info!(%addr, server = name, "listening (http)");
            let listener = tokio::net::TcpListener::bind(addr)
                .await
                .with_context(|| format!("failed to bind {name} on {addr}"))?;
            axum::serve(listener, router)
                .with_graceful_shutdown(wait_shutdown)
                .await
                .with_context(|| format!("{name} http server failed"))?;
        }
    }
    Ok(())
}

fn spawn_sighup_reload(
    rustls_config: axum_server::tls_rustls::RustlsConfig,
    cert_path: String,
    key_path: String,
) {
    #[cfg(unix)]
    {
        tokio::spawn(async move {
            let mut sighup = match tokio::signal::unix::signal(
                tokio::signal::unix::SignalKind::hangup(),
            ) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(error = %e, "failed to install SIGHUP handler for TLS reload");
                    return;
                }
            };
            while sighup.recv().await.is_some() {
                tracing::info!("received SIGHUP; reloading TLS certificate and key");
                match rustls_config
                    .reload_from_pem_file(&cert_path, &key_path)
                    .await
                {
                    Ok(()) => tracing::info!("TLS certificate and key successfully reloaded"),
                    Err(e) => {
                        tracing::error!(error = %e, "failed to reload TLS cert/key; retaining old cert")
                    }
                }
            }
        });
    }
    #[cfg(not(unix))]
    {
        let _ = (rustls_config, cert_path, key_path);
    }
}

/// Resolve when the process should shut down: SIGTERM or SIGINT on Unix,
/// Ctrl-C elsewhere. Returns signal name.
async fn shutdown_signal() -> &'static str {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("failed to install SIGTERM handler");
        let mut interrupt =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                .expect("failed to install SIGINT handler");
        tokio::select! {
            _ = terminate.recv() => "SIGTERM",
            _ = interrupt.recv() => "SIGINT",
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl-C handler");
        "Ctrl-C"
    }
}
