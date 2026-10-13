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
    /// Public base URL Telegram uses to reach the gateway, e.g.
    /// "https://gateway.example.com". Required when `auto_register` is true.
    #[serde(default)]
    pub public_url: Option<String>,
    /// Register every bot's webhook with Telegram on startup.
    #[serde(default)]
    pub auto_register: bool,
    /// TLS termination settings. When present, the gateway serves HTTPS
    /// on `listen` instead of plain HTTP.
    #[serde(default)]
    pub tls: Option<TlsConfig>,
    /// Durable outbox: accepted updates are persisted and delivered
    /// asynchronously with retries.
    #[serde(default)]
    pub outbox: OutboxConfig,
    /// Circuit breaker per downstream bot.
    #[serde(default)]
    pub circuit_breaker: CircuitBreakerConfig,
    /// Admin API guard. When absent, admin routes are not registered.
    #[serde(default)]
    pub admin: Option<AdminConfig>,
    /// OpenTelemetry export. When absent, no traces/metrics leave the
    /// process and every instrument is a no-op.
    #[serde(default)]
    pub otel: Option<OtelConfig>,
}

/// Configuration for a single downstream bot.
#[derive(Debug, Clone, Deserialize)]
pub struct BotConfig {
    /// Public identifier used in the webhook route: `POST /webhooks/:id`.
    pub id: String,
    /// Bot token from BotFather. Only needed for webhook registration;
    /// bots without one are skipped by `auto_register`.
    #[serde(default)]
    pub bot_token: Option<String>,
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

/// TLS termination with certificates you provide. For setups with a reverse
/// proxy in front, leave this out and terminate TLS there instead.
#[derive(Debug, Clone, Deserialize)]
pub struct TlsConfig {
    /// Path to the PEM-encoded certificate chain.
    pub cert_path: String,
    /// Path to the PEM-encoded private key.
    pub key_path: String,
}

/// Durable outbox settings.
#[derive(Debug, Clone, Deserialize)]
pub struct OutboxConfig {
    /// SQLite file holding the outbox. Created on first start.
    #[serde(default = "default_outbox_path")]
    pub path: String,
    /// Failed deliveries are retried this many times before the update is
    /// dead-lettered.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    /// Sweep interval for due retries, in milliseconds. Ingest also wakes
    /// the dispatcher directly, so this only bounds retry latency.
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    /// Delivered rows are pruned after this many days; dead letters after
    /// 30 days.
    #[serde(default = "default_delivered_retention_days")]
    pub delivered_retention_days: u64,
}

impl Default for OutboxConfig {
    fn default() -> Self {
        Self {
            path: default_outbox_path(),
            max_attempts: default_max_attempts(),
            poll_interval_ms: default_poll_interval_ms(),
            delivered_retention_days: default_delivered_retention_days(),
        }
    }
}

fn default_outbox_path() -> String {
    "outbox.db".to_owned()
}

fn default_max_attempts() -> u32 {
    25
}

fn default_poll_interval_ms() -> u64 {
    1000
}

fn default_delivered_retention_days() -> u64 {
    7
}

/// Circuit breaker settings, applied per downstream bot.
#[derive(Debug, Clone, Deserialize)]
pub struct CircuitBreakerConfig {
    /// Consecutive delivery failures that trip the breaker.
    #[serde(default = "default_failure_threshold")]
    pub failure_threshold: u32,
    /// Seconds before a half-open probe goes out.
    #[serde(default = "default_cooldown_secs")]
    pub cooldown_secs: u64,
    /// Cap for the doubling cooldown after failed probes.
    #[serde(default = "default_max_cooldown_secs")]
    pub max_cooldown_secs: u64,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: default_failure_threshold(),
            cooldown_secs: default_cooldown_secs(),
            max_cooldown_secs: default_max_cooldown_secs(),
        }
    }
}

fn default_failure_threshold() -> u32 {
    5
}

fn default_cooldown_secs() -> u64 {
    60
}

fn default_max_cooldown_secs() -> u64 {
    600
}

/// Admin API settings. The token guards the `/outbox/*` routes.
#[derive(Debug, Clone, Deserialize)]
pub struct AdminConfig {
    pub token: String,
}

/// OpenTelemetry export settings.
#[derive(Debug, Clone, Deserialize)]
pub struct OtelConfig {
    /// OTLP/HTTP collector base URL, e.g. "http://localhost:4318".
    /// Traces go to `{endpoint}/v1/traces`, metrics to `{endpoint}/v1/metrics`.
    pub endpoint: String,
    /// `service.name` resource attribute.
    #[serde(default = "default_otel_service_name")]
    pub service_name: String,
}

fn default_otel_service_name() -> String {
    "telegateway".to_owned()
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

    if config.auto_register
        && config
            .public_url
            .as_ref()
            .map(|u| u.trim().is_empty())
            .unwrap_or(true)
    {
        return Err(ConfigError::Validation(
            "`public_url` is required when `auto_register` is true".to_owned(),
        ));
    }

    if config.outbox.max_attempts == 0 {
        return Err(ConfigError::Validation(
            "`outbox.max_attempts` must be at least 1".to_owned(),
        ));
    }

    if config.circuit_breaker.failure_threshold == 0 {
        return Err(ConfigError::Validation(
            "`circuit_breaker.failure_threshold` must be at least 1".to_owned(),
        ));
    }

    if config.circuit_breaker.cooldown_secs == 0 {
        return Err(ConfigError::Validation(
            "`circuit_breaker.cooldown_secs` must be at least 1".to_owned(),
        ));
    }

    if config
        .admin
        .as_ref()
        .is_some_and(|admin| admin.token.trim().is_empty())
    {
        return Err(ConfigError::Validation(
            "`admin.token` must not be empty".to_owned(),
        ));
    }

    if config
        .otel
        .as_ref()
        .is_some_and(|otel| reqwest::Url::parse(&otel.endpoint).is_err())
    {
        return Err(ConfigError::Validation(
            "`otel.endpoint` is not a valid URL".to_owned(),
        ));
    }

    if let Some(tls) = &config.tls {
        for (name, path) in [
            ("tls.cert_path", tls.cert_path.as_str()),
            ("tls.key_path", tls.key_path.as_str()),
        ] {
            if path.trim().is_empty() {
                return Err(ConfigError::Validation(format!(
                    "`{name}` must not be empty"
                )));
            }
            if !Path::new(path).is_file() {
                return Err(ConfigError::Validation(format!(
                    "`{name}`: file not found: {path}"
                )));
            }
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
    fn rejects_auto_register_without_public_url() {
        let text = "auto_register = true\n".to_owned() + valid_toml();
        let config: Config = toml::from_str(&text).unwrap();
        assert!(validate(&config).is_err());
    }

    #[test]
    fn accepts_auto_register_with_public_url() {
        let text = "auto_register = true\npublic_url = \"https://gw.example.com\"\n".to_owned()
            + valid_toml();
        let config: Config = toml::from_str(&text).unwrap();
        validate(&config).unwrap();
    }

    #[test]
    fn rejects_bad_downstream_url() {
        let text = valid_toml().replace("http://127.0.0.1:8081/webhook", "://not-a-url");
        let config: Config = toml::from_str(&text).unwrap();
        assert!(validate(&config).is_err());
    }

    #[test]
    fn rejects_tls_with_missing_files() {
        let text = valid_toml().to_owned()
            + r#"
[tls]
cert_path = "/nonexistent/cert.pem"
key_path = "/nonexistent/key.pem"
"#;
        let config: Config = toml::from_str(&text).unwrap();
        assert!(matches!(validate(&config), Err(ConfigError::Validation(_))));
    }

    #[test]
    fn accepts_tls_section_when_files_exist() {
        let dir = std::env::temp_dir();
        let cert = dir.join("telegateway-test-cert.pem");
        let key = dir.join("telegateway-test-key.pem");
        std::fs::write(&cert, "pem").unwrap();
        std::fs::write(&key, "pem").unwrap();
        let text = format!(
            "{}\n[tls]\ncert_path = \"{}\"\nkey_path = \"{}\"\n",
            valid_toml(),
            cert.display(),
            key.display()
        );
        let config: Config = toml::from_str(&text).unwrap();
        validate(&config).unwrap();
        assert!(config.tls.is_some());
        std::fs::remove_file(&cert).ok();
        std::fs::remove_file(&key).ok();
    }
}
