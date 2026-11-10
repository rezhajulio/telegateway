// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
//! Webhook ingress: validate the secret, rate-limit per chat, persist to
//! the outbox, and answer Telegram.
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::Value;

use crate::{app::AppState, outbox::InsertOutcome, telegram};

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
    body: Bytes,
) -> Response {
    tracing::Span::current().record("bot_id", bot_id.as_str());

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

    let body_str = match std::str::from_utf8(&body) {
        Ok(s) => s,
        Err(_) => {
            tracing::warn!(bot_id = %bot_id, "rejected: invalid utf-8 payload");
            return (StatusCode::BAD_REQUEST, "invalid utf-8 payload").into_response();
        }
    };

    let value: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(err) => {
            tracing::warn!(bot_id = %bot_id, error = %err, "rejected: invalid json");
            return (StatusCode::BAD_REQUEST, "invalid json").into_response();
        }
    };

    let envelope = match telegram::envelope(&value) {
        Ok(envelope) => envelope,
        Err(err) => {
            tracing::warn!(bot_id = %bot_id, error = %err, "rejected: bad update envelope");
            return (StatusCode::BAD_REQUEST, "bad update envelope").into_response();
        }
    };

    tracing::Span::current().record("update_id", envelope.update_id);

    // Ordering note: `admit` spends a token from the chat's bucket and
    // governor has no refund, so every check that can reject an update
    // without inserting it runs first. Redeliveries already in the outbox
    // never reach `admit`, and an outbox-full 503 sheds without burning
    // quota. Two concurrent redeliveries of the same `update_id` can still
    // both miss `find_payload` and both spend a token; `INSERT OR IGNORE`
    // keeps one row. The cap itself is enforced atomically by
    // `insert_with_cap`, so the `pending_total` check below is only a cheap
    // pre-filter to keep the common shed path from spending a token. An
    // insert that loses the cap race after `admit` cannot give the token
    // back.
    match state.store.find_payload(&bot_id, envelope.update_id).await {
        Ok(Some(_)) => {
            tracing::debug!(
                bot_id = %bot_id,
                update_id = envelope.update_id,
                "duplicate update ignored"
            );
            return (StatusCode::OK, "ok").into_response();
        }
        Ok(None) => {}
        Err(err) => {
            tracing::error!(
                bot_id = %bot_id,
                update_id = envelope.update_id,
                error = %err,
                "outbox lookup failed"
            );
            return (StatusCode::SERVICE_UNAVAILABLE, "outbox unavailable").into_response();
        }
    }

    // (b) Queue cap pre-check before admission, so a shed update burns no
    // quota. `insert_with_cap` in (d) is the authoritative, atomic cap.
    match state.store.pending_total(&bot_id).await {
        Ok(pending) if (pending as u64) >= state.max_pending => {
            tracing::warn!(
                bot_id = %bot_id,
                pending,
                max_pending = state.max_pending,
                "outbox full"
            );
            return (StatusCode::SERVICE_UNAVAILABLE, "outbox full").into_response();
        }
        Ok(_) => {}
        Err(err) => {
            tracing::error!(
                bot_id = %bot_id,
                error = %err,
                "outbox pending_total check failed"
            );
            return (StatusCode::SERVICE_UNAVAILABLE, "outbox unavailable").into_response();
        }
    }

    // (c) Admission policy: rate limit check for new updates. This spends a
    // token, and only the insert below can still fail (503, token stays
    // spent). The error path is rare; the cheap rejections already returned.
    if !state.policy.admit(&bot_id, envelope.chat_key) {
        tracing::warn!(
            bot_id = %bot_id,
            update_id = envelope.update_id,
            chat_key = ?envelope.chat_key,
            "rejected: rate limited"
        );
        return (StatusCode::OK, "rate limited").into_response();
    }

    // (d) Persist original payload bytes. The count-and-insert happens in one
    // statement, so concurrent handlers cannot overshoot `max_pending`.
    match state
        .store
        .insert_with_cap(
            &bot_id,
            envelope.update_id,
            envelope.chat_key,
            body_str,
            state.max_pending,
        )
        .await
    {
        Ok(InsertOutcome::Inserted) => {
            crate::otel::metrics()
                .updates_accepted
                .add(1, &crate::otel::bot_attrs(&bot_id));
            tracing::info!(
                bot_id = %bot_id,
                update_id = envelope.update_id,
                kind = ?envelope.kind,
                chat_key = ?envelope.chat_key,
                "update accepted"
            );
        }
        Ok(InsertOutcome::Duplicate) => {
            tracing::debug!(
                bot_id = %bot_id,
                update_id = envelope.update_id,
                "duplicate update ignored"
            );
        }
        Ok(InsertOutcome::CapExceeded) => {
            // Lost the cap race after `admit` spent its token. The row was not
            // stored, so Telegram's retry can still land it.
            tracing::warn!(
                bot_id = %bot_id,
                update_id = envelope.update_id,
                max_pending = state.max_pending,
                "outbox full"
            );
            return (StatusCode::SERVICE_UNAVAILABLE, "outbox full").into_response();
        }
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
/// through timing differences. Note: length differences may leak length,
/// which is acceptable.
fn secret_ok(headers: &HeaderMap, expected: &str) -> bool {
    let provided = headers
        .get(SECRET_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    constant_time_eq::constant_time_eq(provided.as_bytes(), expected.as_bytes())
}
