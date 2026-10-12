// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
use std::sync::Arc;
use std::time::Duration;

use axum::{
    Router,
    http::StatusCode,
    routing::{get, post},
};
use tower::limit::ConcurrencyLimitLayer;
use tower_http::{limit::RequestBodyLimitLayer, timeout::TimeoutLayer, trace::TraceLayer};

use crate::{
    config::{BotConfig, Config},
    policy::AdmissionPolicy,
    routes,
};

/// Shared state injected into every handler.
#[derive(Clone)]
pub struct AppState {
    pub bots: Arc<Vec<BotConfig>>,
    pub policy: AdmissionPolicy,
    pub http: reqwest::Client,
}

impl AppState {
    pub fn bot(&self, id: &str) -> Option<&BotConfig> {
        self.bots.iter().find(|b| b.id == id)
    }
}

/// Build the router. Middleware is layered outside-in, in the order a
/// request meets it: tracing, body limit, timeout, concurrency guard.
pub fn build_router(config: Config) -> Router {
    let state = AppState {
        policy: AdmissionPolicy::new(&config.bots),
        bots: Arc::new(config.bots),
        http: reqwest::Client::new(),
    };

    Router::new()
        .route("/healthz", get(routes::health::healthz))
        .route("/readyz", get(routes::health::readyz))
        .route("/webhooks/{bot_id}", post(routes::webhook::webhook_handler))
        .layer(TraceLayer::new_for_http())
        .layer(RequestBodyLimitLayer::new(1024 * 1024))
        // A gateway-side timeout means "retry later" to Telegram.
        .layer(TimeoutLayer::with_status_code(
            StatusCode::BAD_GATEWAY,
            Duration::from_secs(30),
        ))
        .layer(ConcurrencyLimitLayer::new(512))
        .with_state(state)
}
