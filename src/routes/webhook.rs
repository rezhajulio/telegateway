// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
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

/// Receive a Telegram webhook update, apply admission policy, and persist it
/// to the outbox. Delivery to the downstream bot happens asynchronously.
///
/// Response contract with Telegram:
/// - `200` for accepted, duplicate, or policy-rejected updates. Telegram
///   retries anything else, and retrying a rate-limited update only makes
///   the abuse worse.
/// - `4xx` for malformed requests (unknown bot, bad secret, bad envelope).
/// - `503` when the outbox is unavailable, so Telegram retries the delivery
///   later. A downed downstream bot no longer surfaces here: the dispatcher
///   retries delivery in the background.
#[tracing::instrument(
    name = "webhook.ingress",
    skip(state, headers, body),
    fields(bot_id = tracing::field::Empty, update_id = tracing::field::Empty)
)]
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

    match state
        .store
        .insert(&bot_id, envelope.update_id, &body.to_string())
        .await
    {
        Ok(true) => {
            crate::otel::metrics()
                .updates_accepted
                .add(1, &crate::otel::bot_attrs(&bot_id));
            tracing::Span::current().record("bot_id", bot_id.as_str());
            tracing::Span::current().record("update_id", envelope.update_id);
            tracing::info!(
                bot_id = %bot_id,
                update_id = envelope.update_id,
                kind = ?envelope.kind,
                chat_key = ?envelope.chat_key,
                "update accepted"
            );
        }
        Ok(false) => tracing::debug!(
            bot_id = %bot_id,
            update_id = envelope.update_id,
            "duplicate update ignored"
        ),
        Err(err) => {
            tracing::error!(
                bot_id = %bot_id,
                update_id = envelope.update_id,
                error = %err,
                "outbox write failed"
            );
            return (StatusCode::SERVICE_UNAVAILABLE, "outbox unavailable").into_response();
        }
    }

    if let Some(notify) = state.notifies.get(&bot_id) {
        notify.notify_one();
    }
    (StatusCode::OK, "ok").into_response()
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
