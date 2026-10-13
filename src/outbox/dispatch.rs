// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
//! Asynchronous delivery: one task per bot drains its outbox rows,
//! forwarding each downstream with retries. A circuit breaker per bot
//! stops delivery attempts entirely while the downstream is known down.
//! A separate janitor prunes old delivered and dead rows.
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::Notify;

use crate::config::BotConfig;

use super::{CircuitBreaker, Decision, OutboxRow, Store, now_millis};

/// How many rows one drain pass claims at most.
const CLAIM_LIMIT: i64 = 32;

/// Delivery error from downstream HTTP request.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum DeliveryError {
    /// Network / protocol / timeout transport error without credential URLs.
    #[error("{0}")]
    Transport(String),
    /// Downstream HTTP status code and optional `Retry-After` duration.
    #[error("downstream answered {status}")]
    Status {
        /// Downstream HTTP response status.
        status: reqwest::StatusCode,
        /// Optional Retry-After duration.
        retry_after: Option<Duration>,
    },
}

impl DeliveryError {
    /// Returns the status code if this is a Status error.
    pub fn status(&self) -> Option<reqwest::StatusCode> {
        match self {
            Self::Status { status, .. } => Some(*status),
            Self::Transport(_) => None,
        }
    }

    /// Returns the Retry-After duration if present.
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::Status { retry_after, .. } => *retry_after,
            Self::Transport(_) => None,
        }
    }
}

fn is_permanent(err: &DeliveryError) -> bool {
    match err {
        DeliveryError::Status { status, .. } => {
            status.is_client_error()
                && *status != reqwest::StatusCode::REQUEST_TIMEOUT
                && *status != reqwest::StatusCode::TOO_EARLY
                && *status != reqwest::StatusCode::TOO_MANY_REQUESTS
        }
        DeliveryError::Transport(_) => false,
    }
}

/// Forward one payload to the downstream bot. Shared by the dispatcher
/// and the replay API.
pub async fn deliver_payload(
    http: &reqwest::Client,
    bot: &BotConfig,
    update_id: i64,
    payload: &str,
) -> Result<axum::http::StatusCode, DeliveryError> {
    let mut trace_headers = reqwest::header::HeaderMap::new();
    crate::otel::inject_trace_context(&mut trace_headers);
    let resp = http
        .post(&bot.downstream_url)
        .timeout(Duration::from_secs(25))
        .headers(trace_headers)
        .header("content-type", "application/json")
        .header("x-telegateway-bot", &bot.id)
        .header("x-telegateway-update-id", update_id.to_string())
        .body(payload.to_owned())
        .send()
        .await
        .map_err(|e| DeliveryError::Transport(e.without_url().to_string()))?;
    let status = resp.status();
    if status.is_success() {
        Ok(status)
    } else {
        let retry_after = resp
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.trim().parse::<u64>().ok())
            .map(Duration::from_secs);
        Err(DeliveryError::Status {
            status,
            retry_after,
        })
    }
}

/// Per-bot delivery task: drains due outbox rows while the circuit
/// breaker allows it, backs off on failure, and probes for recovery.
pub struct Dispatcher {
    store: Store,
    http: reqwest::Client,
    bot: BotConfig,
    max_attempts: u32,
    poll_interval: Duration,
    notify: Arc<Notify>,
    breaker: Mutex<CircuitBreaker>,
}

impl Dispatcher {
    /// Build a dispatcher. `notify` wakes the delivery loop when new
    /// updates are accepted.
    pub fn new(
        store: Store,
        http: reqwest::Client,
        bot: BotConfig,
        max_attempts: u32,
        poll_interval: Duration,
        notify: Arc<Notify>,
        breaker: CircuitBreaker,
    ) -> Self {
        Self {
            store,
            http,
            bot,
            max_attempts,
            poll_interval: poll_interval.max(Duration::from_millis(1)),
            notify,
            breaker: Mutex::new(breaker),
        }
    }

    fn lock_breaker(&self) -> std::sync::MutexGuard<'_, CircuitBreaker> {
        self.breaker.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[doc(hidden)] // test support
    pub fn decide(&self) -> Decision {
        self.lock_breaker().decide(Instant::now())
    }

    /// Process every currently-due row once, sequentially. Returns the
    /// number of rows handled. Stops early (requeueing unattempted rows)
    /// when the circuit breaker opens mid-drain or shutdown occurs.
    pub async fn drain_once(&self) -> usize {
        let (_tx, rx) = tokio::sync::watch::channel(false);
        self.drain_once_with_shutdown(&rx).await
    }

    /// Process due rows while checking shutdown and breaker between rows.
    pub async fn drain_once_with_shutdown(
        &self,
        shutdown: &tokio::sync::watch::Receiver<bool>,
    ) -> usize {
        let mut done = 0;
        loop {
            if *shutdown.borrow() || !matches!(self.decide(), Decision::Allow) {
                break;
            }
            let rows = match self.store.claim_due(&self.bot.id, CLAIM_LIMIT).await {
                Ok(rows) => rows,
                Err(e) => {
                    tracing::error!(bot_id = %self.bot.id, error = %e, "outbox claim failed");
                    return done;
                }
            };
            if rows.is_empty() {
                break;
            }
            let mut rows = rows.into_iter().peekable();
            while let Some(row) = rows.next() {
                if *shutdown.borrow() || !matches!(self.decide(), Decision::Allow) {
                    let ids: Vec<i64> = std::iter::once(row.id).chain(rows.map(|r| r.id)).collect();
                    if let Err(e) = self.store.requeue(&ids).await {
                        tracing::error!(
                            bot_id = %self.bot.id,
                            error = %e,
                            "failed to requeue rows after breaker opened or shutdown"
                        );
                    }
                    return done;
                }
                self.deliver_row(&row).await;
                done += 1;
            }
        }
        done
    }

    fn record_breaker_state(&self) {
        let value = self.lock_breaker().gauge_value(Instant::now());
        crate::otel::metrics()
            .breaker_state
            .record(value, &crate::otel::bot_attrs(&self.bot.id));
    }

    async fn mark_delivered_with_retry(&self, row_id: i64, update_id: i64) {
        const RETRY_DELAYS: [Duration; 3] = [
            Duration::from_millis(50),
            Duration::from_millis(200),
            Duration::from_millis(800),
        ];
        let mut last_err = match self.store.mark_delivered(row_id).await {
            Ok(()) => return,
            Err(e) => e,
        };
        for delay in RETRY_DELAYS {
            tokio::time::sleep(delay).await;
            match self.store.mark_delivered(row_id).await {
                Ok(()) => return,
                Err(e) => last_err = e,
            }
        }
        tracing::error!(
            bot_id = %self.bot.id,
            update_id,
            error = %last_err,
            "failed to mark update delivered after retries"
        );
    }

    #[tracing::instrument(
        name = "outbox.deliver",
        skip(self, row),
        fields(bot_id = %self.bot.id, update_id = row.update_id)
    )]
    async fn deliver_row(&self, row: &OutboxRow) {
        let started = Instant::now();
        let metrics = crate::otel::metrics();
        let attrs = crate::otel::bot_attrs(&self.bot.id);
        match deliver_payload(&self.http, &self.bot, row.update_id, &row.payload).await {
            Ok(_) => {
                self.lock_breaker().on_success();
                self.record_breaker_state();
                metrics.updates_delivered.add(1, &attrs);
                metrics
                    .delivery_latency_ms
                    .record(started.elapsed().as_millis() as f64, &attrs);
                self.mark_delivered_with_retry(row.id, row.update_id).await;
                tracing::info!(
                    bot_id = %self.bot.id,
                    update_id = row.update_id,
                    latency_ms = started.elapsed().as_millis(),
                    "update delivered"
                );
            }
            Err(error) => {
                let err_str = error.to_string();
                if is_permanent(&error) {
                    metrics.updates_failed.add(1, &attrs);
                    metrics.updates_dead_lettered.add(1, &attrs);
                    if let Err(e) = self.store.mark_dead(row.id, &err_str).await {
                        tracing::error!(
                            bot_id = %self.bot.id,
                            update_id = row.update_id,
                            error = %e,
                            "failed to mark permanent failure dead"
                        );
                    }
                    tracing::warn!(
                        bot_id = %self.bot.id,
                        update_id = row.update_id,
                        error = %err_str,
                        "permanent delivery failure, dead-lettered"
                    );
                } else {
                    self.lock_breaker().on_failure(Instant::now());
                    self.record_breaker_state();
                    metrics.updates_failed.add(1, &attrs);
                    let min_delay = match &error {
                        DeliveryError::Status {
                            status,
                            retry_after,
                        } if *status == reqwest::StatusCode::TOO_MANY_REQUESTS
                            || *status == reqwest::StatusCode::SERVICE_UNAVAILABLE =>
                        {
                            *retry_after
                        }
                        _ => None,
                    };
                    match self
                        .store
                        .mark_failed_with_min_delay(row.id, &err_str, self.max_attempts, min_delay)
                        .await
                    {
                        Ok(Some(delay)) => tracing::warn!(
                            bot_id = %self.bot.id,
                            update_id = row.update_id,
                            attempt = row.attempts,
                            retry_in_secs = delay.as_secs(),
                            error = %err_str,
                            "delivery failed, retrying"
                        ),
                        Ok(None) => {
                            metrics.updates_dead_lettered.add(1, &attrs);
                            tracing::warn!(
                                bot_id = %self.bot.id,
                                update_id = row.update_id,
                                attempts = row.attempts,
                                error = %err_str,
                                "delivery failed, dead-lettered"
                            );
                        }
                        Err(e) => tracing::error!(
                            bot_id = %self.bot.id,
                            update_id = row.update_id,
                            error = %e,
                            "failed to record delivery failure"
                        ),
                    }
                }
            }
        }
    }

    /// Send one probe delivery while half-open. On success the breaker
    /// closes and the bot's pending rows are woken for immediate drain;
    /// on failure the breaker reopens with a doubled cooldown (unless permanent).
    /// Returns true if a delivery was attempted, false otherwise.
    #[tracing::instrument(name = "outbox.probe", skip(self), fields(bot_id = %self.bot.id))]
    pub async fn probe(&self) -> bool {
        let rows = match self.store.claim_due(&self.bot.id, 1).await {
            Ok(rows) => rows,
            Err(e) => {
                tracing::error!(bot_id = %self.bot.id, error = %e, "outbox claim failed");
                return false;
            }
        };
        let Some(row) = rows.into_iter().next() else {
            return false;
        };
        tracing::info!(
            bot_id = %self.bot.id,
            update_id = row.update_id,
            "circuit breaker probe"
        );
        let metrics = crate::otel::metrics();
        let attrs = crate::otel::bot_attrs(&self.bot.id);
        match deliver_payload(&self.http, &self.bot, row.update_id, &row.payload).await {
            Ok(_) => {
                self.lock_breaker().on_success();
                self.record_breaker_state();
                metrics.updates_delivered.add(1, &attrs);
                self.mark_delivered_with_retry(row.id, row.update_id).await;
                if let Err(e) = self.store.wake_pending(&self.bot.id).await {
                    tracing::error!(
                        bot_id = %self.bot.id,
                        error = %e,
                        "failed to wake pending rows after recovery"
                    );
                }
                self.notify.notify_one();
            }
            Err(error) => {
                let err_str = error.to_string();
                if is_permanent(&error) {
                    metrics.updates_failed.add(1, &attrs);
                    metrics.updates_dead_lettered.add(1, &attrs);
                    if let Err(e) = self.store.mark_dead(row.id, &err_str).await {
                        tracing::error!(
                            bot_id = %self.bot.id,
                            update_id = row.update_id,
                            error = %e,
                            "failed to mark permanent failure dead"
                        );
                    }
                    tracing::warn!(
                        bot_id = %self.bot.id,
                        update_id = row.update_id,
                        error = %err_str,
                        "permanent probe delivery failure, dead-lettered"
                    );
                } else {
                    self.lock_breaker().on_failure(Instant::now());
                    self.record_breaker_state();
                    metrics.updates_failed.add(1, &attrs);
                    let min_delay = match &error {
                        DeliveryError::Status {
                            status,
                            retry_after,
                        } if *status == reqwest::StatusCode::TOO_MANY_REQUESTS
                            || *status == reqwest::StatusCode::SERVICE_UNAVAILABLE =>
                        {
                            *retry_after
                        }
                        _ => None,
                    };
                    match self
                        .store
                        .mark_failed_with_min_delay(row.id, &err_str, self.max_attempts, min_delay)
                        .await
                    {
                        Ok(None) => {
                            metrics.updates_dead_lettered.add(1, &attrs);
                        }
                        Ok(Some(_)) => {}
                        Err(e) => {
                            tracing::error!(
                                bot_id = %self.bot.id,
                                update_id = row.update_id,
                                error = %e,
                                "failed to record delivery failure"
                            );
                        }
                    }
                }
            }
        }
        true
    }

    /// Run the delivery loop: drain while the breaker allows, sleep while
    /// it denies, probe while half-open. Exits gracefully when shutdown is signaled.
    pub async fn run(self: Arc<Self>, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        loop {
            if *shutdown.borrow() {
                break;
            }
            match self.decide() {
                Decision::Allow => {
                    self.drain_once_with_shutdown(&shutdown).await;
                    if *shutdown.borrow() {
                        break;
                    }
                    tokio::select! {
                        _ = shutdown.changed() => {
                            if *shutdown.borrow() { break; }
                        }
                        () = self.notify.notified() => {},
                        () = tokio::time::sleep(self.poll_interval) => {},
                    }
                }
                Decision::Deny { retry_after } => {
                    tokio::select! {
                        _ = shutdown.changed() => {
                            if *shutdown.borrow() { break; }
                        }
                        () = self.notify.notified() => {},
                        () = tokio::time::sleep(retry_after) => {},
                    }
                }
                Decision::Probe => {
                    if !self.probe().await {
                        tokio::select! {
                            _ = shutdown.changed() => {
                                if *shutdown.borrow() { break; }
                            }
                            () = self.notify.notified() => {},
                            () = tokio::time::sleep(self.poll_interval) => {},
                        }
                    }
                }
            }
        }
    }
}

/// Periodically prune delivered rows older than `delivered_retention_days`
/// and dead letters older than 30 days or beyond `max_dead_per_bot` per bot. Exits gracefully when shutdown is signaled.
pub async fn janitor(
    store: Store,
    delivered_retention_days: u64,
    max_dead_per_bot: u64,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(3600));
    loop {
        if *shutdown.borrow() {
            break;
        }
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() { break; }
            }
            _ = interval.tick() => {}
        }
        if *shutdown.borrow() {
            break;
        }
        let now = now_millis();
        let days = delivered_retention_days.min(36500) as i64;
        let delivered_cutoff = now.saturating_sub(days.saturating_mul(86_400_000));
        let dead_cutoff = now.saturating_sub(30 * 86_400_000);
        if let Err(e) = store
            .prune(
                delivered_cutoff,
                dead_cutoff,
                i64::try_from(max_dead_per_bot).unwrap_or(i64::MAX),
            )
            .await
        {
            tracing::error!(error = %e, "outbox prune failed");
        }
    }
}
