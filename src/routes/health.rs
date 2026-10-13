// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
//! Liveness and readiness probes.
use axum::{extract::State, http::StatusCode, response::IntoResponse};

use crate::app::AppState;

/// Liveness probe: the process is running.
pub async fn healthz() -> &'static str {
    "ok"
}

/// Readiness probe: the gateway is accepting traffic and the outbox
/// database answers.
pub async fn readyz(State(state): State<AppState>) -> impl IntoResponse {
    match state.store.ping().await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(err) => {
            tracing::error!(error = %err, "readyz: outbox unreachable");
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
    }
}
