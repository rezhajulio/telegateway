// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
//! Webhook self-registration: when `auto_register` is set, point each bot's
//! Telegram webhook at this gateway on startup, instead of doing it by hand
//! with curl.
use std::time::Duration;

use reqwest::redirect::Policy;
use thiserror::Error;

use crate::config::{BotConfig, Config};

/// Telegram Bot API base URL. A parameter (not a constant) so tests can
/// point it at a stub server.
pub const TELEGRAM_API_BASE: &str = "https://api.telegram.org";

/// Build a hardened HTTP client for Telegram Bot API calls.
pub fn default_client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(Policy::none())
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(10))
        .build()
        .expect("failed to build reqwest client")
}

/// Everything that can go wrong while registering a webhook.
#[derive(Debug, Error)]
pub enum RegisterError {
    /// The HTTP request itself failed (URL stripped to prevent token leak).
    #[error("request failed: {0}")]
    Request(reqwest::Error),
    /// Telegram answered with HTTP status (e.g. 5xx, 429). Retryable.
    #[error("server status: {0}")]
    Status(reqwest::StatusCode),
    /// Telegram answered, but with `ok: false` and client error. Not retried.
    #[error("telegram refused: {0}")]
    Refused(String),
}

impl RegisterError {
    /// Whether this error is retryable (transient transport error, 5xx, or 429).
    pub fn is_retryable(&self) -> bool {
        match self {
            RegisterError::Request(_) => true,
            RegisterError::Status(s) => {
                s.is_server_error() || *s == reqwest::StatusCode::TOO_MANY_REQUESTS
            }
            RegisterError::Refused(_) => false,
        }
    }
}

/// The public webhook URL Telegram will call for this bot.
pub fn webhook_url(public_url: &str, bot_id: &str) -> String {
    format!("{}/webhooks/{bot_id}", public_url.trim_end_matches('/'),)
}

/// Register one bot's webhook with Telegram via `setWebhook`.
#[tracing::instrument(
    name = "telegram.register",
    skip(client, bot),
    fields(bot_id = %bot.id)
)]
pub async fn register_bot(
    client: &reqwest::Client,
    api_base: &str,
    bot: &BotConfig,
    public_url: &str,
) -> Result<(), RegisterError> {
    let token = bot.bot_token.as_deref().unwrap_or("");
    let url = format!("{}/bot{token}/setWebhook", api_base.trim_end_matches('/'));
    let resp = client
        .post(&url)
        .json(&serde_json::json!({
            "url": webhook_url(public_url, &bot.id),
            "secret_token": bot.webhook_secret,
        }))
        .send()
        .await
        .map_err(reqwest::Error::without_url)
        .map_err(RegisterError::Request)?;

    let status = resp.status();
    if status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        return Err(RegisterError::Status(status));
    }
    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(reqwest::Error::without_url)
        .map_err(RegisterError::Request)?;

    if body.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
        Ok(())
    } else {
        let description = body
            .get("description")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown error");
        Err(RegisterError::Refused(description.to_owned()))
    }
}

/// Retry registering a single bot up to 5 attempts with backoff.
pub async fn register_bot_with_backoff(
    client: &reqwest::Client,
    api_base: &str,
    bot: &BotConfig,
    public_url: &str,
    base_delay: Duration,
) -> Result<(), RegisterError> {
    let mut attempts = 0;
    let mut delay = base_delay;
    loop {
        attempts += 1;
        match register_bot(client, api_base, bot, public_url).await {
            Ok(()) => return Ok(()),
            Err(err) => {
                if attempts >= 5 || !err.is_retryable() {
                    return Err(err);
                }
                tracing::warn!(
                    bot_id = %bot.id,
                    attempt = attempts,
                    retry_in = ?delay,
                    error = %err,
                    "webhook registration failed; retrying"
                );
                tokio::time::sleep(delay).await;
                delay = delay.saturating_mul(2);
            }
        }
    }
}

/// Internal implementation of `register_all` taking an injectable base delay.
#[doc(hidden)]
pub async fn register_all_with_delay(config: &Config, api_base: &str, base_delay: Duration) {
    let Some(public_url) = config.public_url.as_deref() else {
        return;
    };
    let client = default_client();
    for bot in &config.bots {
        if bot.bot_token.as_ref().map(|t| t.is_empty()).unwrap_or(true) {
            tracing::debug!(
                bot_id = %bot.id,
                "skipping webhook registration: no bot_token"
            );
            continue;
        }
        match register_bot_with_backoff(&client, api_base, bot, public_url, base_delay).await {
            Ok(()) => tracing::info!(
                bot_id = %bot.id,
                url = webhook_url(public_url, &bot.id),
                "webhook registered"
            ),
            Err(e) => tracing::warn!(
                bot_id = %bot.id,
                error = %e,
                "webhook registration failed"
            ),
        }
    }
}

/// Register every bot that has a token. Failures are logged, never fatal:
/// Telegram being unreachable must not take down the gateway.
pub async fn register_all(config: &Config, api_base: &str) {
    register_all_with_delay(config, api_base, Duration::from_secs(2)).await;
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn webhook_url_joins_bot_route() {
        assert_eq!(
            webhook_url("https://gw.example.com", "echo-python"),
            "https://gw.example.com/webhooks/echo-python"
        );
    }

    #[test]
    fn webhook_url_tolerates_trailing_slash() {
        assert_eq!(
            webhook_url("https://gw.example.com/", "echo-go"),
            "https://gw.example.com/webhooks/echo-go"
        );
    }
}
