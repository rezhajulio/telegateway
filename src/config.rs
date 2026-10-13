// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
//! TOML configuration: parsing, defaults, and validation.
use std::collections::HashSet;
use std::fmt;
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
    /// <https://gateway.example.com>. Required when `auto_register` is true.
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
#[derive(Clone, Deserialize)]
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

impl fmt::Debug for BotConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BotConfig")
            .field("id", &self.id)
            .field("bot_token", &self.bot_token.as_ref().map(|_| "[REDACTED]"))
            .field("downstream_url", &self.downstream_url)
            .field("webhook_secret", &"[REDACTED]")
            .field("rate_limit_per_minute", &self.rate_limit_per_minute)
            .finish()
    }
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
    /// 30 days (or beyond `max_dead_letters` per bot).
    #[serde(default = "default_delivered_retention_days")]
    pub delivered_retention_days: u64,
    /// Maximum pending and in-flight updates allowed per bot before new
    /// updates are shed with HTTP 503.
    #[serde(default = "default_max_pending")]
    pub max_pending: u64,
    /// Dead letters kept per bot; the oldest beyond this are pruned
    /// hourly, in addition to the 30-day age limit.
    #[serde(default = "default_max_dead_letters")]
    pub max_dead_letters: u64,
}

impl Default for OutboxConfig {
    fn default() -> Self {
        Self {
            path: default_outbox_path(),
            max_attempts: default_max_attempts(),
            poll_interval_ms: default_poll_interval_ms(),
            delivered_retention_days: default_delivered_retention_days(),
            max_pending: default_max_pending(),
            max_dead_letters: default_max_dead_letters(),
        }
    }
}

fn default_max_pending() -> u64 {
    10_000
}

fn default_max_dead_letters() -> u64 {
    10_000
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
#[derive(Clone, Deserialize)]
pub struct AdminConfig {
    /// Bearer token checked against the `Authorization` header.
    pub token: String,
    /// Separate listen address for the admin API. When unset, admin routes
    /// share the main `listen` address.
    #[serde(default)]
    pub listen: Option<String>,
}

impl fmt::Debug for AdminConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AdminConfig")
            .field("token", &"[REDACTED]")
            .field("listen", &self.listen)
            .finish()
    }
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

/// Everything that can go wrong while loading configuration.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// The file could not be read.
    #[error("cannot read config file: {0}")]
    Io(#[from] std::io::Error),
    /// The file is not valid TOML.
    #[error("cannot parse config file: {0}")]
    Parse(#[from] toml::de::Error),
    /// The TOML parsed but the values don't make sense.
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
    let top_addr = config.listen.parse::<SocketAddr>().map_err(|e| {
        ConfigError::Validation(format!("`listen` is not a valid socket address: {e}"))
    })?;

    if config.bots.is_empty() {
        return Err(ConfigError::Validation(
            "at least one `[[bots]]` entry is required".to_owned(),
        ));
    }

    let mut ids = HashSet::new();
    for bot in &config.bots {
        if !is_valid_bot_id(&bot.id) {
            return Err(ConfigError::Validation(format!(
                "bot `{}`: `id` must contain 1-64 characters matching [A-Za-z0-9_-]",
                bot.id
            )));
        }
        if !ids.insert(bot.id.clone()) {
            return Err(ConfigError::Validation(format!(
                "duplicate bot id `{}`",
                bot.id
            )));
        }
        let downstream = reqwest::Url::parse(&bot.downstream_url).map_err(|e| {
            ConfigError::Validation(format!(
                "bot `{}`: `downstream_url` is not a valid URL: {e}",
                bot.id
            ))
        })?;
        if !matches!(downstream.scheme(), "http" | "https") || !downstream.has_host() {
            return Err(ConfigError::Validation(format!(
                "bot `{}`: `downstream_url` scheme must be http or https and include a host",
                bot.id
            )));
        }
        if !is_valid_webhook_secret(&bot.webhook_secret) {
            return Err(ConfigError::Validation(format!(
                "bot `{}`: `webhook_secret` must contain 1-256 characters matching [A-Za-z0-9_-]",
                bot.id
            )));
        }
        if let Some(token) = &bot.bot_token {
            let trimmed = token.trim();
            if trimmed.is_empty()
                || token.contains('/')
                || token.contains(|c: char| c.is_whitespace() || c == '?' || c == '#')
            {
                return Err(ConfigError::Validation(format!(
                    "bot `{}`: `bot_token` must not be empty or contain whitespace, '/', '?', or '#'",
                    bot.id
                )));
            }
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

    if let Some(public_url) = &config.public_url {
        let parsed = reqwest::Url::parse(public_url).map_err(|e| {
            ConfigError::Validation(format!("`public_url` is not a valid URL: {e}"))
        })?;
        if parsed.scheme() != "https" || !parsed.has_host() {
            return Err(ConfigError::Validation(
                "`public_url` scheme must be https and include a host".to_owned(),
            ));
        }
    }

    if config.outbox.max_attempts == 0 {
        return Err(ConfigError::Validation(
            "`outbox.max_attempts` must be at least 1".to_owned(),
        ));
    }

    if config.outbox.poll_interval_ms == 0 {
        return Err(ConfigError::Validation(
            "`outbox.poll_interval_ms` must be at least 1".to_owned(),
        ));
    }

    if !(1..=36500).contains(&config.outbox.delivered_retention_days) {
        return Err(ConfigError::Validation(
            "`outbox.delivered_retention_days` must be between 1 and 36500".to_owned(),
        ));
    }

    if config.outbox.max_pending == 0 {
        return Err(ConfigError::Validation(
            "`outbox.max_pending` must be at least 1".to_owned(),
        ));
    }

    if config.outbox.max_dead_letters == 0 {
        return Err(ConfigError::Validation(
            "`outbox.max_dead_letters` must be at least 1".to_owned(),
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

    if config.circuit_breaker.max_cooldown_secs < config.circuit_breaker.cooldown_secs {
        return Err(ConfigError::Validation(
            "`circuit_breaker.max_cooldown_secs` must be greater than or equal to `cooldown_secs`"
                .to_owned(),
        ));
    }

    if let Some(admin) = &config.admin {
        if admin.token.len() < 32 {
            return Err(ConfigError::Validation(
                "`admin.token` must be at least 32 characters long".to_owned(),
            ));
        }
        if let Some(admin_listen) = &admin.listen {
            let admin_addr = admin_listen.parse::<SocketAddr>().map_err(|e| {
                ConfigError::Validation(format!(
                    "`admin.listen` is not a valid socket address: {e}"
                ))
            })?;
            if admin_listen == &config.listen || admin_addr == top_addr {
                return Err(ConfigError::Validation(
                    "`admin.listen` must differ from top-level `listen`".to_owned(),
                ));
            }
        }
    }

    if let Some(otel) = &config.otel {
        let parsed = reqwest::Url::parse(&otel.endpoint).map_err(|e| {
            ConfigError::Validation(format!("`otel.endpoint` is not a valid URL: {e}"))
        })?;
        if !matches!(parsed.scheme(), "http" | "https") || !parsed.has_host() {
            return Err(ConfigError::Validation(
                "`otel.endpoint` scheme must be http or https and include a host".to_owned(),
            ));
        }
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

fn is_valid_bot_id(id: &str) -> bool {
    (1..=64).contains(&id.len())
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

fn is_valid_webhook_secret(secret: &str) -> bool {
    (1..=256).contains(&secret.len())
        && secret
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
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
        assert_eq!(config.outbox.max_pending, 10_000);
        assert_eq!(config.outbox.poll_interval_ms, 1000);
        assert_eq!(config.outbox.delivered_retention_days, 7);
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
    fn validates_bot_id() {
        // success case
        let text = valid_toml().replace(r#"id = "demo""#, r#"id = "bot_123-abc""#);
        let config: Config = toml::from_str(&text).unwrap();
        validate(&config).unwrap();

        // failure: empty
        let text = valid_toml().replace(r#"id = "demo""#, r#"id = """#);
        let config: Config = toml::from_str(&text).unwrap();
        assert!(validate(&config).is_err());

        // failure: invalid char
        let text = valid_toml().replace(r#"id = "demo""#, r#"id = "demo@bot""#);
        let config: Config = toml::from_str(&text).unwrap();
        assert!(validate(&config).is_err());

        // failure: too long (>64 chars)
        let long_id = "a".repeat(65);
        let text = valid_toml().replace(r#"id = "demo""#, &format!(r#"id = "{long_id}""#));
        let config: Config = toml::from_str(&text).unwrap();
        assert!(validate(&config).is_err());
    }

    #[test]
    fn validates_webhook_secret() {
        // success
        let valid_secret = "a".repeat(256);
        let text = valid_toml().replace(
            r#"webhook_secret = "s3cret""#,
            &format!(r#"webhook_secret = "{valid_secret}""#),
        );
        let config: Config = toml::from_str(&text).unwrap();
        validate(&config).unwrap();

        // failure: empty
        let text = valid_toml().replace(r#"webhook_secret = "s3cret""#, r#"webhook_secret = """#);
        let config: Config = toml::from_str(&text).unwrap();
        assert!(validate(&config).is_err());

        // failure: invalid char
        let text = valid_toml().replace(
            r#"webhook_secret = "s3cret""#,
            r#"webhook_secret = "bad secret!""#,
        );
        let config: Config = toml::from_str(&text).unwrap();
        assert!(validate(&config).is_err());

        // failure: >256 chars
        let too_long = "a".repeat(257);
        let text = valid_toml().replace(
            r#"webhook_secret = "s3cret""#,
            &format!(r#"webhook_secret = "{too_long}""#),
        );
        let config: Config = toml::from_str(&text).unwrap();
        assert!(validate(&config).is_err());
    }

    #[test]
    fn validates_downstream_url() {
        // success with https
        let text = valid_toml().replace(
            "http://127.0.0.1:8081/webhook",
            "https://internal.example.com:8443/webhook",
        );
        let config: Config = toml::from_str(&text).unwrap();
        validate(&config).unwrap();

        // failure: bad scheme (ftp)
        let text = valid_toml().replace(
            "http://127.0.0.1:8081/webhook",
            "ftp://127.0.0.1:8081/webhook",
        );
        let config: Config = toml::from_str(&text).unwrap();
        assert!(validate(&config).is_err());

        // failure: bad scheme (file)
        let text = valid_toml().replace("http://127.0.0.1:8081/webhook", "file:///tmp/socket");
        let config: Config = toml::from_str(&text).unwrap();
        assert!(validate(&config).is_err());

        // failure: no host
        let text = valid_toml().replace("http://127.0.0.1:8081/webhook", "http://");
        let config: Config = toml::from_str(&text).unwrap();
        assert!(validate(&config).is_err());
    }

    #[test]
    fn validates_public_url() {
        // success
        let text = format!("public_url = \"https://gw.example.com\"\n{}", valid_toml());
        let config: Config = toml::from_str(&text).unwrap();
        validate(&config).unwrap();

        // failure: http scheme
        let text = format!("public_url = \"http://gw.example.com\"\n{}", valid_toml());
        let config: Config = toml::from_str(&text).unwrap();
        assert!(validate(&config).is_err());

        // failure: not a url
        let text = format!("public_url = \"not-a-url\"\n{}", valid_toml());
        let config: Config = toml::from_str(&text).unwrap();
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
    fn validates_bot_token() {
        // success
        let text = format!(
            "{}\nbot_token = \"123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11\"\n",
            valid_toml()
        );
        let config: Config = toml::from_str(&text).unwrap();
        validate(&config).unwrap();

        // failure: empty
        let text = format!("{}\nbot_token = \"\"\n", valid_toml());
        let config: Config = toml::from_str(&text).unwrap();
        assert!(validate(&config).is_err());

        // failure: contains slash
        let text = format!("{}\nbot_token = \"123/token\"\n", valid_toml());
        let config: Config = toml::from_str(&text).unwrap();
        assert!(validate(&config).is_err());

        // failure: contains whitespace
        let text = format!("{}\nbot_token = \"123 token\"\n", valid_toml());
        let config: Config = toml::from_str(&text).unwrap();
        assert!(validate(&config).is_err());

        // failure: contains query / fragment
        let text = format!("{}\nbot_token = \"123?token\"\n", valid_toml());
        let config: Config = toml::from_str(&text).unwrap();
        assert!(validate(&config).is_err());

        let text = format!("{}\nbot_token = \"123#token\"\n", valid_toml());
        let config: Config = toml::from_str(&text).unwrap();
        assert!(validate(&config).is_err());
    }

    #[test]
    fn validates_outbox_poll_interval_ms() {
        // success
        let text = format!("{}\n[outbox]\npoll_interval_ms = 500\n", valid_toml());
        let config: Config = toml::from_str(&text).unwrap();
        validate(&config).unwrap();

        // failure: 0
        let text = format!("{}\n[outbox]\npoll_interval_ms = 0\n", valid_toml());
        let config: Config = toml::from_str(&text).unwrap();
        assert!(validate(&config).is_err());
    }

    #[test]
    fn validates_outbox_delivered_retention_days() {
        // success
        let text = format!(
            "{}\n[outbox]\ndelivered_retention_days = 30\n",
            valid_toml()
        );
        let config: Config = toml::from_str(&text).unwrap();
        validate(&config).unwrap();

        // failure: 0
        let text = format!("{}\n[outbox]\ndelivered_retention_days = 0\n", valid_toml());
        let config: Config = toml::from_str(&text).unwrap();
        assert!(validate(&config).is_err());

        // failure: >36500
        let text = format!(
            "{}\n[outbox]\ndelivered_retention_days = 40000\n",
            valid_toml()
        );
        let config: Config = toml::from_str(&text).unwrap();
        assert!(validate(&config).is_err());
    }

    #[test]
    fn validates_outbox_max_dead_letters() {
        let text = format!("{}\n[outbox]\nmax_dead_letters = 50\n", valid_toml());
        let config: Config = toml::from_str(&text).unwrap();
        validate(&config).unwrap();
        assert_eq!(config.outbox.max_dead_letters, 50);

        let text = format!("{}\n[outbox]\nmax_dead_letters = 0\n", valid_toml());
        let config: Config = toml::from_str(&text).unwrap();
        assert!(validate(&config).is_err());
    }

    #[test]
    fn validates_outbox_max_pending() {
        // success
        let text = format!("{}\n[outbox]\nmax_pending = 500\n", valid_toml());
        let config: Config = toml::from_str(&text).unwrap();
        validate(&config).unwrap();

        // failure: 0
        let text = format!("{}\n[outbox]\nmax_pending = 0\n", valid_toml());
        let config: Config = toml::from_str(&text).unwrap();
        assert!(validate(&config).is_err());
    }

    #[test]
    fn validates_circuit_breaker_cooldowns() {
        // success
        let text = format!(
            "{}\n[circuit_breaker]\ncooldown_secs = 10\nmax_cooldown_secs = 60\n",
            valid_toml()
        );
        let config: Config = toml::from_str(&text).unwrap();
        validate(&config).unwrap();

        // failure: max < cooldown
        let text = format!(
            "{}\n[circuit_breaker]\ncooldown_secs = 60\nmax_cooldown_secs = 10\n",
            valid_toml()
        );
        let config: Config = toml::from_str(&text).unwrap();
        assert!(validate(&config).is_err());
    }

    #[test]
    fn validates_admin_token_length() {
        // success: >= 32 chars
        let text = format!(
            "{}\n[admin]\ntoken = \"12345678901234567890123456789012\"\n",
            valid_toml()
        );
        let config: Config = toml::from_str(&text).unwrap();
        validate(&config).unwrap();

        // failure: < 32 chars
        let text = format!("{}\n[admin]\ntoken = \"short-token\"\n", valid_toml());
        let config: Config = toml::from_str(&text).unwrap();
        assert!(validate(&config).is_err());
    }

    #[test]
    fn validates_admin_listen() {
        // success: different from top-level listen
        let text = format!(
            "{}\n[admin]\ntoken = \"12345678901234567890123456789012\"\nlisten = \"127.0.0.1:9090\"\n",
            valid_toml()
        );
        let config: Config = toml::from_str(&text).unwrap();
        validate(&config).unwrap();

        // failure: same as top-level listen
        let text = format!(
            "{}\n[admin]\ntoken = \"12345678901234567890123456789012\"\nlisten = \"127.0.0.1:8080\"\n",
            valid_toml()
        );
        let config: Config = toml::from_str(&text).unwrap();
        assert!(validate(&config).is_err());

        // failure: invalid socket address
        let text = format!(
            "{}\n[admin]\ntoken = \"12345678901234567890123456789012\"\nlisten = \"not-a-socket-addr\"\n",
            valid_toml()
        );
        let config: Config = toml::from_str(&text).unwrap();
        assert!(validate(&config).is_err());
    }

    #[test]
    fn validates_otel_endpoint() {
        // success: http / https
        let text = format!(
            "{}\n[otel]\nendpoint = \"http://localhost:4318\"\n",
            valid_toml()
        );
        let config: Config = toml::from_str(&text).unwrap();
        validate(&config).unwrap();

        // failure: grpc or bad scheme
        let text = format!(
            "{}\n[otel]\nendpoint = \"grpc://localhost:4317\"\n",
            valid_toml()
        );
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
        let dir = tempfile::tempdir().unwrap();
        let cert = dir.path().join("cert.pem");
        let key = dir.path().join("key.pem");
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
    }

    #[test]
    fn secrets_redacted_in_debug() {
        let text = r#"
listen = "127.0.0.1:8080"

[admin]
token = "admin-secret-token-value-must-be-32-chars-long"

[[bots]]
id = "demo"
bot_token = "bot-secret-token-value-12345"
downstream_url = "http://127.0.0.1:8081/webhook"
webhook_secret = "webhook-secret-value-12345"
"#;
        let config: Config = toml::from_str(text).unwrap();
        validate(&config).unwrap();
        let debug = format!("{config:?}");
        assert!(!debug.contains("admin-secret-token-value-must-be-32-chars-long"));
        assert!(!debug.contains("bot-secret-token-value-12345"));
        assert!(!debug.contains("webhook-secret-value-12345"));
        assert!(debug.contains("[REDACTED]"));
    }
}
