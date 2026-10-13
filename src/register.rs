// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
//! Webhook self-registration: when `auto_register` is set, point each bot's
//! Telegram webhook at this gateway on startup, instead of doing it by hand
//! with curl.
use std::time::Duration;

use thiserror::Error;

use crate::config::{BotConfig, Config};

/// Telegram Bot API base URL. A parameter (not a constant) so tests can
/// point it at a stub server.
pub const TELEGRAM_API_BASE: &str = "https://api.telegram.org";

/// Everything that can go wrong while registering a webhook.
#[derive(Debug, Error)]
pub enum RegisterError {
    /// The HTTP request itself failed.
    #[error("request failed: {0}")]
    Request(#[from] reqwest::Error),
    /// Telegram answered, but with `ok: false`.
    #[error("telegram refused: {0}")]
    Refused(String),
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
    let body: serde_json::Value = client
        .post(&url)
        .timeout(Duration::from_secs(10))
        .json(&serde_json::json!({
            "url": webhook_url(public_url, &bot.id),
            "secret_token": bot.webhook_secret,
        }))
        .send()
        .await?
        .json()
        .await?;

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

/// Register every bot that has a token. Failures are logged, never fatal:
/// Telegram being unreachable must not take down the gateway.
pub async fn register_all(config: &Config, api_base: &str) {
    let Some(public_url) = config.public_url.as_deref() else {
        return;
    };
    let client = reqwest::Client::new();
    for bot in &config.bots {
        if bot.bot_token.as_ref().map(|t| t.is_empty()).unwrap_or(true) {
            tracing::debug!(
                bot_id = %bot.id,
                "skipping webhook registration: no bot_token"
            );
            continue;
        }
        match register_bot(&client, api_base, bot, public_url).await {
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
