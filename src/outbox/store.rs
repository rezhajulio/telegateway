// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
//! SQLite-backed outbox storage. One row per accepted update; the dispatcher
//! claims due rows atomically so a crash can never lose or double-claim one.
use std::time::Duration;

use sqlx::{
    Row,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool},
};

use super::{backoff_delay, now_millis};

/// A queued update claimed for delivery.
#[derive(Debug)]
pub struct OutboxRow {
    pub id: i64,
    pub bot_id: String,
    pub update_id: i64,
    pub payload: String,
    pub attempts: i64,
}

/// An update that exhausted its retries.
#[derive(Debug, serde::Serialize)]
pub struct DeadLetter {
    pub bot_id: String,
    pub update_id: i64,
    pub attempts: i64,
    pub last_error: Option<String>,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
pub struct Store {
    pool: SqlitePool,
}

impl Store {
    /// Open (creating) the database, run migrations, and requeue rows left
    /// `in_flight` by a previous run: they may never have been delivered.
    pub async fn open(path: &str) -> Result<Self, sqlx::Error> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePool::connect_with(options).await?;
        sqlx::migrate!("./migrations").run(&pool).await?;
        let store = Self { pool };
        store.reset_in_flight().await?;
        Ok(store)
    }

    /// Cheap liveness check for the readiness probe.
    pub async fn ping(&self) -> Result<(), sqlx::Error> {
        sqlx::query("SELECT 1").fetch_one(&self.pool).await?;
        Ok(())
    }

    async fn reset_in_flight(&self) -> Result<(), sqlx::Error> {
        let res = sqlx::query(
            "UPDATE outbox SET status = 'pending', next_attempt_at = NULL WHERE status = 'in_flight'",
        )
        .execute(&self.pool)
        .await?;
        if res.rows_affected() > 0 {
            tracing::info!(
                requeued = res.rows_affected(),
                "outbox: requeued in-flight updates from previous run"
            );
        }
        Ok(())
    }

    /// Insert an accepted update. Returns `false` when the
    /// `(bot_id, update_id)` pair is already queued — a Telegram redelivery.
    pub async fn insert(
        &self,
        bot_id: &str,
        update_id: i64,
        payload: &str,
    ) -> Result<bool, sqlx::Error> {
        let res = sqlx::query(
            "INSERT OR IGNORE INTO outbox(bot_id, update_id, payload, created_at)
             VALUES(?, ?, ?, ?)",
        )
        .bind(bot_id)
        .bind(update_id)
        .bind(payload)
        .bind(now_millis())
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() == 1)
    }

    /// Atomically claim due rows for one bot and bump their attempt counter.
    pub async fn claim_due(&self, bot_id: &str, limit: i64) -> Result<Vec<OutboxRow>, sqlx::Error> {
        let rows = sqlx::query(
            "UPDATE outbox SET status = 'in_flight', attempts = attempts + 1
             WHERE id IN (
               SELECT id FROM outbox
               WHERE bot_id = ? AND status = 'pending'
                 AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
               ORDER BY id LIMIT ?
             )
             RETURNING id, bot_id, update_id, payload, attempts",
        )
        .bind(bot_id)
        .bind(now_millis())
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .iter()
            .map(|r| OutboxRow {
                id: r.get("id"),
                bot_id: r.get("bot_id"),
                update_id: r.get("update_id"),
                payload: r.get("payload"),
                attempts: r.get("attempts"),
            })
            .collect())
    }

    pub async fn mark_delivered(&self, id: i64) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE outbox SET status = 'delivered' WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Record a failed attempt. Returns the backoff when the row is
    /// requeued, or `None` when it crossed `max_attempts` and was
    /// dead-lettered.
    pub async fn mark_failed(
        &self,
        id: i64,
        error: &str,
        max_attempts: u32,
    ) -> Result<Option<Duration>, sqlx::Error> {
        let row = sqlx::query("SELECT attempts FROM outbox WHERE id = ?")
            .bind(id)
            .fetch_one(&self.pool)
            .await?;
        let attempts: i64 = row.get("attempts");
        if attempts >= i64::from(max_attempts) {
            sqlx::query("UPDATE outbox SET status = 'dead', last_error = ? WHERE id = ?")
                .bind(error)
                .bind(id)
                .execute(&self.pool)
                .await?;
            Ok(None)
        } else {
            let delay = backoff_delay(attempts as u32);
            let next = now_millis() + delay.as_millis() as i64;
            sqlx::query(
                "UPDATE outbox SET status = 'pending', next_attempt_at = ?, last_error = ? WHERE id = ?",
            )
            .bind(next)
            .bind(error)
            .bind(id)
            .execute(&self.pool)
            .await?;
            Ok(Some(delay))
        }
    }

    /// Dead letters, newest first, optionally for one bot.
    pub async fn list_dead(&self, bot_id: Option<&str>) -> Result<Vec<DeadLetter>, sqlx::Error> {
        let rows = match bot_id {
            Some(bot) => {
                sqlx::query(
                    "SELECT bot_id, update_id, attempts, last_error, created_at
                     FROM outbox WHERE status = 'dead' AND bot_id = ?
                     ORDER BY id DESC",
                )
                .bind(bot)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query(
                    "SELECT bot_id, update_id, attempts, last_error, created_at
                     FROM outbox WHERE status = 'dead'
                     ORDER BY id DESC",
                )
                .fetch_all(&self.pool)
                .await?
            }
        };
        Ok(rows
            .iter()
            .map(|r| DeadLetter {
                bot_id: r.get("bot_id"),
                update_id: r.get("update_id"),
                attempts: r.get("attempts"),
                last_error: r.get("last_error"),
                created_at: r.get("created_at"),
            })
            .collect())
    }

    /// Drop delivered rows older than `delivered_cutoff` and dead rows older
    /// than `dead_cutoff` (both unix millis).
    pub async fn prune(&self, delivered_cutoff: i64, dead_cutoff: i64) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM outbox WHERE status = 'delivered' AND created_at < ?")
            .bind(delivered_cutoff)
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM outbox WHERE status = 'dead' AND created_at < ?")
            .bind(dead_cutoff)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Test helper: make a row due immediately, bypassing backoff.
    pub async fn force_due(&self, id: i64) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE outbox SET next_attempt_at = 0 WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Test helper: same, addressed by bot and update id.
    pub async fn force_due_by_update(
        &self,
        bot_id: &str,
        update_id: i64,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE outbox SET next_attempt_at = 0 WHERE bot_id = ? AND update_id = ?")
            .bind(bot_id)
            .bind(update_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Test helper: look up a row's status.
    pub async fn status_of(&self, bot_id: &str, update_id: i64) -> Result<String, sqlx::Error> {
        let row = sqlx::query("SELECT status FROM outbox WHERE bot_id = ? AND update_id = ?")
            .bind(bot_id)
            .bind(update_id)
            .fetch_one(&self.pool)
            .await?;
        Ok(row.get("status"))
    }
}
