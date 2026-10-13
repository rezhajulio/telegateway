// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
//! Admin routes, guarded by the `[admin]` token. Only registered when the
//! token is configured.
use axum::{
    Json,
    body::Bytes,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    app::AppState,
    outbox::{OUTBOX_STATUSES, deliver_payload},
    telegram,
};

/// Query parameters for the outbox listing.
#[derive(Debug, Deserialize)]
pub struct ListQuery {
    /// One of `pending`, `in_flight`, `delivered`, `dead`, `all`.
    /// Defaults to `dead`.
    pub status: Option<String>,
    /// Filter to one bot.
    pub bot_id: Option<String>,
    /// Max rows, clamped to 1..=500. Defaults to 50.
    pub limit: Option<i64>,
}

/// List outbox rows, newest first.
pub async fn list_outbox(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
    headers: HeaderMap,
) -> Response {
    if !admin_ok(&state, &headers) {
        return admin_denied(&state);
    }

    let status = query.status.as_deref().unwrap_or("dead");
    if !OUTBOX_STATUSES.contains(&status) && status != "all" {
        return (
            StatusCode::BAD_REQUEST,
            format!("unknown status `{status}`: expected one of pending, in_flight, delivered, dead, all"),
        )
            .into_response();
    }
    let limit = query.limit.unwrap_or(50).clamp(1, 500);

    match state
        .store
        .list(status, query.bot_id.as_deref(), limit)
        .await
    {
        Ok(rows) => (StatusCode::OK, Json(rows)).into_response(),
        Err(err) => {
            tracing::error!(error = %err, "failed to list outbox");
            (StatusCode::INTERNAL_SERVER_ERROR, "outbox unavailable").into_response()
        }
    }
}

/// Body of a replay request: either a stored update or an ad-hoc payload.
#[derive(Debug, Deserialize)]
pub struct ReplayRequest {
    /// Which bot's downstream receives the replay.
    pub bot_id: String,
    /// Re-fire the newest stored payload for this update. Exactly one of
    /// `update_id` and `payload` is required.
    pub update_id: Option<i64>,
    /// Ad-hoc payload to fire instead of a stored one.
    pub payload: Option<Value>,
}

#[derive(Debug, Serialize)]
struct ReplayResponse {
    ok: bool,
    bot_id: String,
    update_id: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    downstream_status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// Re-deliver a past update to a bot's downstream, synchronously, for
/// debugging. The downstream sees the original `update_id`, so the bot
/// treats it like the real thing.
#[tracing::instrument(name = "admin.replay", skip(state, headers, body))]
pub async fn replay(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    if !admin_ok(&state, &headers) {
        return admin_denied(&state);
    }

    let req: ReplayRequest = match serde_json::from_slice(&body) {
        Ok(req) => req,
        Err(err) => {
            return (StatusCode::BAD_REQUEST, format!("invalid json body: {err}")).into_response();
        }
    };
    let bot = match state.bot(&req.bot_id) {
        Some(bot) => bot.clone(),
        None => return (StatusCode::NOT_FOUND, "unknown bot").into_response(),
    };

    let (update_id, payload) = match (req.update_id, req.payload) {
        (Some(_), Some(_)) => {
            return (
                StatusCode::BAD_REQUEST,
                "pass exactly one of `update_id` and `payload`",
            )
                .into_response();
        }
        (None, None) => {
            return (
                StatusCode::BAD_REQUEST,
                "pass exactly one of `update_id` and `payload`",
            )
                .into_response();
        }
        (Some(update_id), None) => match state.store.find_payload(&req.bot_id, update_id).await {
            Ok(Some(payload)) => (update_id, payload),
            Ok(None) => {
                return (
                    StatusCode::NOT_FOUND,
                    "no stored update for this bot and update_id",
                )
                    .into_response();
            }
            Err(err) => {
                tracing::error!(error = %err, "replay lookup failed");
                return (StatusCode::INTERNAL_SERVER_ERROR, "outbox unavailable").into_response();
            }
        },
        (None, Some(payload)) => {
            let parsed_id = match telegram::envelope(&payload) {
                Ok(envelope) => envelope.update_id,
                Err(err) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        format!("bad update envelope: {err}"),
                    )
                        .into_response();
                }
            };
            (parsed_id, payload.to_string())
        }
    };

    tracing::info!(
        bot_id = %bot.id,
        update_id,
        replay = true,
        "replaying update"
    );
    match deliver_payload(&state.http, &bot, update_id, &payload).await {
        Ok(status) => {
            crate::otel::metrics()
                .updates_replayed
                .add(1, &crate::otel::bot_attrs(&bot.id));
            (
                StatusCode::OK,
                Json(ReplayResponse {
                    ok: true,
                    bot_id: bot.id,
                    update_id,
                    downstream_status: Some(status.as_u16()),
                    error: None,
                }),
            )
                .into_response()
        }
        Err(error) => (
            StatusCode::BAD_GATEWAY,
            Json(ReplayResponse {
                ok: false,
                bot_id: bot.id,
                update_id,
                downstream_status: None,
                error: Some(error.to_string()),
            }),
        )
            .into_response(),
    }
}

/// Whether the request carries the admin token. When no token is
/// configured the admin routes are not registered at all, so this only
/// runs with a token present.
fn admin_ok(state: &AppState, headers: &HeaderMap) -> bool {
    match state.admin_token.as_deref() {
        Some(expected) => bearer_ok(headers, expected),
        None => false,
    }
}

fn admin_denied(state: &AppState) -> Response {
    if state.admin_token.is_none() {
        StatusCode::NOT_FOUND.into_response()
    } else {
        (StatusCode::UNAUTHORIZED, "bad admin token").into_response()
    }
}

/// Constant-time check for the bearer token. Length difference may leak length.
fn bearer_ok(headers: &HeaderMap, expected: &str) -> bool {
    let provided = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    constant_time_eq::constant_time_eq(provided.as_bytes(), expected.as_bytes())
}
