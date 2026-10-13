// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
//! Asynchronous delivery: one task per bot drains its outbox rows,
//! forwarding each downstream with retries. A separate janitor prunes
//! old delivered and dead rows.
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Notify;

use crate::config::BotConfig;

use super::{OutboxRow, Store, now_millis};

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
    let resp = http
        .post(&bot.downstream_url)
        .timeout(Duration::from_secs(25))
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

/// Per-bot delivery task.
pub struct Dispatcher {
    store: Store,
    http: reqwest::Client,
    bot: BotConfig,
    max_attempts: u32,
    poll_interval: Duration,
    notify: Arc<Notify>,
}

impl Dispatcher {
    pub fn new(
        store: Store,
        http: reqwest::Client,
        bot: BotConfig,
        max_attempts: u32,
        poll_interval: Duration,
        notify: Arc<Notify>,
    ) -> Self {
        Self {
            store,
            http,
            bot,
            max_attempts,
            poll_interval,
            notify,
        }
    }

    /// Process every currently-due row once, sequentially. Returns the
    /// number of rows handled.
    pub async fn drain_once(&self) -> usize {
        let mut done = 0;
        loop {
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
            for row in rows {
                self.deliver_row(&row).await;
                done += 1;
            }
        }
        done
    }

    async fn deliver_row(&self, row: &OutboxRow) {
        let started = Instant::now();
        match deliver_payload(&self.http, &self.bot, row.update_id, &row.payload).await {
            Ok(_) => {
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
                    Ok(None) => tracing::warn!(
                        bot_id = %self.bot.id,
                        update_id = row.update_id,
                        attempts = row.attempts,
                        error = %error,
                        "delivery failed, dead-lettered"
                    ),
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

    /// Drain due rows, then wait for new work or the next retry sweep.
    pub async fn run(self: Arc<Self>) {
        loop {
            self.drain_once().await;
            tokio::select! {
                () = self.notify.notified() => {},
                () = tokio::time::sleep(self.poll_interval) => {},
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
