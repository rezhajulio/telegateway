// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
use std::time::Duration;

use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::Value;

use crate::{app::AppState, telegram};

/// Telegram's header carrying the webhook secret configured via `setWebhook`.
const SECRET_HEADER: &str = "x-telegram-bot-api-secret-token";

/// Receive a Telegram webhook update, apply admission policy, and forward it
/// to the downstream bot.
///
/// Response contract with Telegram:
/// - `200` for accepted, duplicate, or policy-rejected updates. Telegram
///   retries anything else, and retrying a rate-limited update only makes
///   the abuse worse.
/// - `4xx` for malformed requests (unknown bot, bad secret, bad envelope).
/// - `502` when the downstream bot is unreachable or errors, so Telegram
///   retries the delivery later.
pub async fn webhook_handler(
    State(state): State<AppState>,
    Path(bot_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let bot = match state.bot(&bot_id) {
        Some(bot) => bot.clone(),
        None => {
            tracing::warn!(bot_id = %bot_id, "unknown bot");
            return (StatusCode::NOT_FOUND, "unknown bot").into_response();
        }
    };

    if !secret_ok(&headers, &bot.webhook_secret) {
        tracing::warn!(bot_id = %bot_id, "rejected: bad webhook secret");
        return (StatusCode::UNAUTHORIZED, "bad webhook secret").into_response();
    }

    let envelope = match telegram::envelope(&body) {
        Ok(envelope) => envelope,
        Err(err) => {
            tracing::warn!(bot_id = %bot_id, error = %err, "rejected: bad update envelope");
            return (StatusCode::BAD_REQUEST, "bad update envelope").into_response();
        }
    };

    if !state.policy.admit(&bot_id, envelope.chat_key) {
        tracing::warn!(
            bot_id = %bot_id,
            update_id = envelope.update_id,
            chat_key = ?envelope.chat_key,
            "rejected: rate limited"
        );
        return (StatusCode::OK, "rate limited").into_response();
    }

    let started = std::time::Instant::now();
    let forward = state
        .http
        .post(&bot.downstream_url)
        .timeout(Duration::from_secs(25))
        .header("content-type", "application/json")
        .header("x-telegateway-bot", &bot_id)
        .header("x-telegateway-update-id", envelope.update_id.to_string())
        .json(&body)
        .send()
        .await;

    match forward {
        Ok(resp) => {
            let status = resp.status();
            tracing::info!(
                bot_id = %bot_id,
                update_id = envelope.update_id,
                kind = ?envelope.kind,
                chat_key = ?envelope.chat_key,
                downstream_status = %status,
                latency_ms = started.elapsed().as_millis(),
                "update forwarded"
            );
            if status.is_success() {
                (StatusCode::OK, "ok").into_response()
            } else {
                (StatusCode::BAD_GATEWAY, "downstream error").into_response()
            }
        }
        Err(err) => {
            tracing::error!(
                bot_id = %bot_id,
                update_id = envelope.update_id,
                error = %err,
                "downstream unreachable"
            );
            (StatusCode::BAD_GATEWAY, "downstream unreachable").into_response()
        }
    }
}

/// Constant-time comparison so the secret cannot be probed byte-by-byte
/// through timing differences.
fn secret_ok(headers: &HeaderMap, expected: &str) -> bool {
    let provided = headers
        .get(SECRET_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    constant_time_eq::constant_time_eq(provided.as_bytes(), expected.as_bytes())
}
