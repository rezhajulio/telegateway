// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
//! Router assembly: builds the axum router, shared state, and per-bot
//! outbox dispatchers from the config.
use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::Context;
use axum::{
    Router,
    http::StatusCode,
    routing::{get, post},
};
use tokio::sync::Notify;
use tower::limit::ConcurrencyLimitLayer;
use tower_http::{limit::RequestBodyLimitLayer, timeout::TimeoutLayer, trace::TraceLayer};

use crate::{
    config::{BotConfig, Config},
    outbox::{CircuitBreaker, Dispatcher, Store},
    policy::AdmissionPolicy,
    routes,
};

/// Shared state injected into every handler.
#[derive(Clone)]
pub struct AppState {
    /// Bot configs by id, for routing and forwarding.
    pub bots: Arc<Vec<BotConfig>>,
    /// Per-chat rate limiters.
    pub policy: AdmissionPolicy,
    /// Outbound HTTP client for downstream delivery.
    pub http: reqwest::Client,
    /// The durable outbox.
    pub store: Store,
    /// Wake signals for each bot's dispatcher, keyed by bot id.
    pub notifies: Arc<HashMap<String, Arc<Notify>>>,
    /// Bearer token for the admin routes; `None` means they are not
    /// registered.
    pub admin_token: Option<String>,
}

impl AppState {
    /// Look up a bot's config by id.
    pub fn bot(&self, id: &str) -> Option<&BotConfig> {
        self.bots.iter().find(|b| b.id == id)
    }
}

/// Everything `build_router` produces: the router plus the background
/// workers the caller must spawn.
pub struct BuiltApp {
    /// The axum router, ready to serve.
    pub router: Router,
    /// One dispatcher per bot; spawn each with `Dispatcher::run`.
    pub dispatchers: Vec<Arc<Dispatcher>>,
    /// The outbox store, shared with the dispatchers.
    pub store: Store,
}

/// Build the router and the per-bot outbox dispatchers. Middleware is
/// layered outside-in, in the order a request meets it: tracing, body
/// limit, timeout, concurrency guard.
pub async fn build_router(config: Config) -> anyhow::Result<BuiltApp> {
    let store = Store::open(&config.outbox.path)
        .await
        .context("failed to open outbox database")?;
    let http = reqwest::Client::new();

    let mut notifies = HashMap::new();
    let mut dispatchers = Vec::new();
    for bot in &config.bots {
        let notify = Arc::new(Notify::new());
        notifies.insert(bot.id.clone(), notify.clone());
        let breaker = CircuitBreaker::new(
            bot.id.clone(),
            config.circuit_breaker.failure_threshold,
            Duration::from_secs(config.circuit_breaker.cooldown_secs),
            Duration::from_secs(config.circuit_breaker.max_cooldown_secs),
        );
        dispatchers.push(Arc::new(Dispatcher::new(
            store.clone(),
            http.clone(),
            bot.clone(),
            config.outbox.max_attempts,
            Duration::from_millis(config.outbox.poll_interval_ms),
            notify,
            breaker,
        )));
    }

    let state = AppState {
        policy: AdmissionPolicy::new(&config.bots),
        bots: Arc::new(config.bots),
        http,
        store: store.clone(),
        notifies: Arc::new(notifies),
        admin_token: config.admin.map(|a| a.token),
    };

    let mut router = Router::new()
        .route("/healthz", get(routes::health::healthz))
        .route("/readyz", get(routes::health::readyz))
        .route("/webhooks/{bot_id}", post(routes::webhook::webhook_handler));
    if state.admin_token.is_some() {
        router = router
            .route("/outbox", get(routes::admin::list_outbox))
            .route("/outbox/replay", post(routes::admin::replay));
    }
    let router = router
        .layer(TraceLayer::new_for_http())
        .layer(RequestBodyLimitLayer::new(1024 * 1024))
        // A gateway-side timeout means "retry later" to Telegram.
        .layer(TimeoutLayer::with_status_code(
            StatusCode::BAD_GATEWAY,
            Duration::from_secs(30),
        ))
        .layer(ConcurrencyLimitLayer::new(512))
        .with_state(state);

    Ok(BuiltApp {
        router,
        dispatchers,
        store,
    })
}
