// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
//! Router assembly: builds the axum router, shared state, and per-bot
//! outbox dispatchers from the config.
use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::Context;
use axum::{
    Router,
    error_handling::HandleErrorLayer,
    http::StatusCode,
    routing::{get, post},
};
use tokio::sync::Notify;
use tower::{limit::ConcurrencyLimitLayer, load_shed::LoadShedLayer};
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
    /// Maximum allowed pending updates before rejecting with 503.
    pub max_pending: u64,
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
    /// Optional separate router for admin API when `config.admin.listen` is set.
    pub admin_router: Option<Router>,
    /// One dispatcher per bot; spawn each with `Dispatcher::run`.
    pub dispatchers: Vec<Arc<Dispatcher>>,
    /// The outbox store, shared with the dispatchers.
    pub store: Store,
    /// Admission policy for rate limiting and pruning.
    pub policy: AdmissionPolicy,
}

/// Build the router and the per-bot outbox dispatchers.
///
/// Middleware execution order: in Axum, calling `.layer(...)` on a router
/// wraps from the inside out, meaning the last `.layer` added is outermost.
/// When using `tower::ServiceBuilder`, layers are declared outside-in:
/// the first layer declared is the outermost layer encountering requests.
/// The request pipeline here processes: tracing -> body size limit (1 MiB)
/// -> request timeout (504 Gateway Timeout after 30s) -> load shedding /
/// concurrency limit (503 Service Unavailable when 512 in-flight requests
/// are exceeded) -> route handler.
pub async fn build_router(config: Config) -> anyhow::Result<BuiltApp> {
    let store = Store::open(&config.outbox.path)
        .await
        .context("failed to open outbox database")?;
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(5))
        .build()
        .context("failed to build outbound HTTP client")?;

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
        dispatchers.push(Arc::new(
            Dispatcher::new(
                store.clone(),
                http.clone(),
                bot.clone(),
                config.outbox.max_attempts,
                Duration::from_millis(config.outbox.poll_interval_ms),
                notify,
                breaker,
            )
            .with_concurrency(config.outbox.delivery_concurrency),
        ));
    }

    let policy = AdmissionPolicy::new(&config.bots);
    let admin_listen = config.admin.as_ref().and_then(|a| a.listen.clone());
    let state = AppState {
        policy: policy.clone(),
        bots: Arc::new(config.bots),
        http,
        store: store.clone(),
        notifies: Arc::new(notifies),
        admin_token: config.admin.map(|a| a.token),
        max_pending: config.outbox.max_pending,
    };

    let middleware = tower::ServiceBuilder::new()
        .layer(TraceLayer::new_for_http())
        .layer(RequestBodyLimitLayer::new(1024 * 1024))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::GATEWAY_TIMEOUT,
            Duration::from_secs(30),
        ))
        .layer(HandleErrorLayer::new(handle_middleware_error))
        .layer(LoadShedLayer::new())
        .layer(ConcurrencyLimitLayer::new(512));

    let (router, admin_router) = match admin_listen {
        Some(_) => {
            let admin_router = Router::new()
                .route("/outbox", get(routes::admin::list_outbox))
                .route("/outbox/replay", post(routes::admin::replay))
                .layer(middleware.clone())
                .with_state(state.clone());
            let public_router = Router::new()
                .route("/healthz", get(routes::health::healthz))
                .route("/readyz", get(routes::health::readyz))
                .route("/webhooks/{bot_id}", post(routes::webhook::webhook_handler))
                .layer(middleware)
                .with_state(state);
            (public_router, Some(admin_router))
        }
        None => {
            let mut router = Router::new()
                .route("/healthz", get(routes::health::healthz))
                .route("/readyz", get(routes::health::readyz))
                .route("/webhooks/{bot_id}", post(routes::webhook::webhook_handler));
            if state.admin_token.is_some() {
                router = router
                    .route("/outbox", get(routes::admin::list_outbox))
                    .route("/outbox/replay", post(routes::admin::replay));
            }
            let router = router.layer(middleware).with_state(state);
            (router, None)
        }
    };

    Ok(BuiltApp {
        router,
        admin_router,
        dispatchers,
        store,
        policy,
    })
}

async fn handle_middleware_error(err: tower::BoxError) -> (StatusCode, &'static str) {
    if err.is::<tower::load_shed::error::Overloaded>() {
        (StatusCode::SERVICE_UNAVAILABLE, "service overloaded")
    } else {
        (StatusCode::INTERNAL_SERVER_ERROR, "internal error")
    }
}
