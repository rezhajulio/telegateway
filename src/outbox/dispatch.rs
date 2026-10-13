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

/// Forward one payload to the downstream bot. Shared by the dispatcher
/// and the replay API.
pub async fn deliver_payload(
    http: &reqwest::Client,
    bot: &BotConfig,
    update_id: i64,
    payload: &str,
) -> Result<axum::http::StatusCode, String> {
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
        .map_err(|e| e.to_string())?;
    let status = resp.status();
    if status.is_success() {
        Ok(status)
    } else {
        Err(format!("downstream answered {status}"))
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
            poll_interval,
            notify,
            breaker: Mutex::new(breaker),
        }
    }

    fn decide(&self) -> Decision {
        self.breaker
            .lock()
            .expect("breaker mutex poisoned")
            .decide(Instant::now())
    }

    /// Process every currently-due row once, sequentially. Returns the
    /// number of rows handled. Stops early (requeueing unattempted rows)
    /// when the circuit breaker opens mid-drain.
    pub async fn drain_once(&self) -> usize {
        let mut done = 0;
        loop {
            if !matches!(self.decide(), Decision::Allow) {
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
                if !matches!(self.decide(), Decision::Allow) {
                    let ids: Vec<i64> = std::iter::once(row.id).chain(rows.map(|r| r.id)).collect();
                    if let Err(e) = self.store.requeue(&ids).await {
                        tracing::error!(
                            bot_id = %self.bot.id,
                            error = %e,
                            "failed to requeue rows after breaker opened"
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
        let value = self
            .breaker
            .lock()
            .expect("breaker mutex poisoned")
            .gauge_value(Instant::now());
        crate::otel::metrics()
            .breaker_state
            .record(value, &crate::otel::bot_attrs(&self.bot.id));
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
                self.breaker
                    .lock()
                    .expect("breaker mutex poisoned")
                    .on_success();
                self.record_breaker_state();
                metrics.updates_delivered.add(1, &attrs);
                metrics
                    .delivery_latency_ms
                    .record(started.elapsed().as_millis() as f64, &attrs);
                if let Err(e) = self.store.mark_delivered(row.id).await {
                    tracing::error!(
                        bot_id = %self.bot.id,
                        update_id = row.update_id,
                        error = %e,
                        "failed to mark update delivered"
                    );
                }
                tracing::info!(
                    bot_id = %self.bot.id,
                    update_id = row.update_id,
                    latency_ms = started.elapsed().as_millis(),
                    "update delivered"
                );
            }
            Err(error) => {
                self.breaker
                    .lock()
                    .expect("breaker mutex poisoned")
                    .on_failure(Instant::now());
                self.record_breaker_state();
                metrics.updates_failed.add(1, &attrs);
                match self
                    .store
                    .mark_failed(row.id, &error, self.max_attempts)
                    .await
                {
                    Ok(Some(delay)) => tracing::warn!(
                        bot_id = %self.bot.id,
                        update_id = row.update_id,
                        attempt = row.attempts,
                        retry_in_secs = delay.as_secs(),
                        error = %error,
                        "delivery failed, retrying"
                    ),
                    Ok(None) => {
                        metrics.updates_dead_lettered.add(1, &attrs);
                        tracing::warn!(
                            bot_id = %self.bot.id,
                            update_id = row.update_id,
                            attempts = row.attempts,
                            error = %error,
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

    /// Send one probe delivery while half-open. On success the breaker
    /// closes and the bot's pending rows are woken for immediate drain;
    /// on failure the breaker reopens with a doubled cooldown.
    #[tracing::instrument(name = "outbox.probe", skip(self), fields(bot_id = %self.bot.id))]
    pub async fn probe(&self) {
        let rows = match self.store.claim_due(&self.bot.id, 1).await {
            Ok(rows) => rows,
            Err(e) => {
                tracing::error!(bot_id = %self.bot.id, error = %e, "outbox claim failed");
                return;
            }
        };
        let Some(row) = rows.into_iter().next() else {
            return;
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
                self.breaker
                    .lock()
                    .expect("breaker mutex poisoned")
                    .on_success();
                self.record_breaker_state();
                metrics.updates_delivered.add(1, &attrs);
                if let Err(e) = self.store.mark_delivered(row.id).await {
                    tracing::error!(
                        bot_id = %self.bot.id,
                        update_id = row.update_id,
                        error = %e,
                        "failed to mark update delivered"
                    );
                }
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
                self.breaker
                    .lock()
                    .expect("breaker mutex poisoned")
                    .on_failure(Instant::now());
                self.record_breaker_state();
                metrics.updates_failed.add(1, &attrs);
                if let Err(e) = self
                    .store
                    .mark_failed(row.id, &error, self.max_attempts)
                    .await
                {
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

    /// Run the delivery loop: drain while the breaker allows, sleep while
    /// it denies, probe while half-open.
    pub async fn run(self: Arc<Self>) {
        loop {
            match self.decide() {
                Decision::Allow => {
                    self.drain_once().await;
                    tokio::select! {
                        () = self.notify.notified() => {},
                        () = tokio::time::sleep(self.poll_interval) => {},
                    }
                }
                Decision::Deny { retry_after } => {
                    tokio::select! {
                        () = self.notify.notified() => {},
                        () = tokio::time::sleep(retry_after) => {},
                    }
                }
                Decision::Probe => {
                    self.probe().await;
                }
            }
        }
    }
}

/// Periodically prune delivered rows older than `delivered_retention_days`
/// and dead letters older than 30 days.
pub async fn janitor(store: Store, delivered_retention_days: u64) {
    let mut interval = tokio::time::interval(Duration::from_secs(3600));
    loop {
        interval.tick().await;
        let now = now_millis();
        let delivered_cutoff = now - delivered_retention_days as i64 * 86_400_000;
        let dead_cutoff = now - 30 * 86_400_000;
        if let Err(e) = store.prune(delivered_cutoff, dead_cutoff).await {
            tracing::error!(error = %e, "outbox prune failed");
        }
    }
}
