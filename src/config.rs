// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::Path;

use serde::Deserialize;
use thiserror::Error;

/// Root configuration, loaded from a TOML file.
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Address the gateway listens on, e.g. "127.0.0.1:8080".
    #[serde(default = "default_listen")]
    pub listen: String,
    /// The bots served by this gateway.
    #[serde(default)]
    pub bots: Vec<BotConfig>,
}

/// Configuration for a single downstream bot.
#[derive(Debug, Clone, Deserialize)]
pub struct BotConfig {
    /// Public identifier used in the webhook route: `POST /webhooks/:id`.
    pub id: String,
    /// Internal HTTP endpoint the update is forwarded to.
    pub downstream_url: String,
    /// Value of Telegram's `X-Telegram-Bot-Api-Secret-Token` header.
    pub webhook_secret: String,
    /// Token-bucket quota, per chat, refilled every minute.
    #[serde(default = "default_rate_limit")]
    pub rate_limit_per_minute: u32,
}

fn default_listen() -> String {
    "127.0.0.1:8080".to_owned()
}

fn default_rate_limit() -> u32 {
    30
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("cannot read config file: {0}")]
    Io(#[from] std::io::Error),
    #[error("cannot parse config file: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("invalid configuration: {0}")]
    Validation(String),
}

/// Load and validate the configuration file.
pub fn load(path: &Path) -> Result<Config, ConfigError> {
    let text = std::fs::read_to_string(path)?;
    load_from_str(&text)
}

/// Parse and validate configuration from a TOML string.
pub fn load_from_str(text: &str) -> Result<Config, ConfigError> {
    let config: Config = toml::from_str(text)?;
    validate(&config)?;
    Ok(config)
}

fn validate(config: &Config) -> Result<(), ConfigError> {
    config.listen.parse::<SocketAddr>().map_err(|e| {
        ConfigError::Validation(format!("`listen` is not a valid socket address: {e}"))
    })?;

    if config.bots.is_empty() {
        return Err(ConfigError::Validation(
            "at least one `[[bots]]` entry is required".to_owned(),
        ));
    }

    let mut ids = HashSet::new();
    for bot in &config.bots {
        if bot.id.trim().is_empty() {
            return Err(ConfigError::Validation(
                "bot `id` must not be empty".to_owned(),
            ));
        }
        if !ids.insert(bot.id.clone()) {
            return Err(ConfigError::Validation(format!(
                "duplicate bot id `{}`",
                bot.id
            )));
        }
        reqwest::Url::parse(&bot.downstream_url).map_err(|e| {
            ConfigError::Validation(format!(
                "bot `{}`: `downstream_url` is not a valid URL: {e}",
                bot.id
            ))
        })?;
        if bot.webhook_secret.is_empty() {
            return Err(ConfigError::Validation(format!(
                "bot `{}`: `webhook_secret` must not be empty",
                bot.id
            )));
        }
        if bot.rate_limit_per_minute == 0 {
            return Err(ConfigError::Validation(format!(
                "bot `{}`: `rate_limit_per_minute` must be at least 1",
                bot.id
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_toml() -> &'static str {
        r#"
listen = "127.0.0.1:8080"

[[bots]]
id = "demo"
downstream_url = "http://127.0.0.1:8081/webhook"
webhook_secret = "s3cret"
rate_limit_per_minute = 30
"#
    }

    #[test]
    fn parses_valid_config() {
        let config: Config = toml::from_str(valid_toml()).unwrap();
        validate(&config).unwrap();
        assert_eq!(config.bots.len(), 1);
        assert_eq!(config.bots[0].id, "demo");
    }

    #[test]
    fn rejects_duplicate_bot_ids() {
        let text = valid_toml().to_owned()
            + r#"
[[bots]]
id = "demo"
downstream_url = "http://127.0.0.1:8082/webhook"
webhook_secret = "other"
"#;
        let config: Config = toml::from_str(&text).unwrap();
        assert!(validate(&config).is_err());
    }

    #[test]
    fn rejects_empty_bots() {
        let config: Config = toml::from_str(r#"listen = "127.0.0.1:8080""#).unwrap();
        assert!(validate(&config).is_err());
    }

    #[test]
    fn rejects_bad_downstream_url() {
        let text = valid_toml().replace("http://127.0.0.1:8081/webhook", "://not-a-url");
        let config: Config = toml::from_str(&text).unwrap();
        assert!(validate(&config).is_err());
    }
}
