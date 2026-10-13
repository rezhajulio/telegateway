// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
//! Admin routes, guarded by the `[admin]` token. Only registered when the
//! token is configured.
use axum::{
    Json,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Deserialize;

use crate::app::AppState;

#[derive(Debug, Deserialize)]
pub struct DeadQuery {
    pub bot_id: Option<String>,
}

/// List dead-lettered updates, newest first. Optional `?bot_id=` filter.
pub async fn dead_letters(
    State(state): State<AppState>,
    Query(query): Query<DeadQuery>,
    headers: HeaderMap,
) -> Response {
    let expected = match state.admin_token.as_deref() {
        Some(token) => token,
        None => return StatusCode::NOT_FOUND.into_response(),
    };
    if !bearer_ok(&headers, expected) {
        return (StatusCode::UNAUTHORIZED, "bad admin token").into_response();
    }

    match state.store.list_dead(query.bot_id.as_deref()).await {
        Ok(dead) => (StatusCode::OK, Json(dead)).into_response(),
        Err(err) => {
            tracing::error!(error = %err, "failed to list dead letters");
            (StatusCode::INTERNAL_SERVER_ERROR, "outbox unavailable").into_response()
        }
    }
}

fn bearer_ok(headers: &HeaderMap, expected: &str) -> bool {
    let provided = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    constant_time_eq::constant_time_eq(provided.as_bytes(), expected.as_bytes())
}
