// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
//! SQLite-backed outbox storage. One row per accepted update; the dispatcher
//! claims due rows atomically so a crash can never lose or double-claim one.
use std::time::Duration;

use sqlx::{
    Row,
    sqlite::{
        SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions, SqliteSynchronous,
    },
};

use super::now_millis;

/// A queued update claimed for delivery.
#[derive(Debug)]
pub struct OutboxRow {
    /// Database row id.
    pub id: i64,
    /// Which bot this update is for.
    pub bot_id: String,
    /// Telegram's update id; dedupe key together with `bot_id`.
    pub update_id: i64,
    /// The raw JSON body Telegram sent.
    pub payload: String,
    /// Delivery attempts so far.
    pub attempts: i64,
}

/// One row in the outbox listing.
#[derive(Debug, serde::Serialize)]
pub struct OutboxEntry {
    /// Database row id.
    pub id: i64,
    /// Which bot this update is for.
    pub bot_id: String,
    /// Telegram's update id.
    pub update_id: i64,
    /// pending, in_flight, delivered, or dead.
    pub status: String,
    /// Delivery attempts so far.
    pub attempts: i64,
    /// Unix millis of the next scheduled attempt, if any.
    pub next_attempt_at: Option<i64>,
    /// Last delivery error, if any.
    pub last_error: Option<String>,
    /// Unix millis when the update was accepted.
    pub created_at: i64,
}

/// Statuses accepted by the list filter.
pub const OUTBOX_STATUSES: &[&str] = &["pending", "in_flight", "delivered", "dead"];

/// SQLite-backed outbox storage. Clone is cheap: clones share the pool.
#[derive(Debug, Clone)]
pub struct Store {
    pool: SqlitePool,
}

impl Store {
    /// Open (creating) the database, run migrations, and requeue rows left
    /// `in_flight` by a previous run as `pending` (preserving their backoff
    /// and attempt count): they may never have been delivered.
    pub async fn open(path: &str) -> Result<Self, sqlx::Error> {
        // WAL mode with Synchronous::Normal avoids fsync per transaction while
        // maintaining crash safety against application/OS failure.
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .acquire_timeout(Duration::from_secs(2))
            .connect_with(options)
            .await?;
        sqlx::migrate!("./migrations").run(&pool).await?;
        let store = Self { pool };
        store.reset_in_flight().await?;
        Ok(store)
    }

    /// Count of rows with status 'pending' or 'in_flight' for the given bot.
    pub async fn pending_total(&self, bot_id: &str) -> Result<i64, sqlx::Error> {
        let row = sqlx::query(
            "SELECT COUNT(*) AS count FROM outbox WHERE bot_id = ? AND status IN ('pending', 'in_flight')",
        )
        .bind(bot_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(row.get::<i64, _>("count"))
    }

    /// Close the underlying connection pool.
    pub async fn close(&self) {
        self.pool.close().await;
    }
    /// Cheap liveness check for the readiness probe.
    pub async fn ping(&self) -> Result<(), sqlx::Error> {
        sqlx::query("SELECT 1").fetch_one(&self.pool).await?;
        Ok(())
    }

    /// Requeue rows left `in_flight` by a previous run as `pending`, keeping
    /// their `next_attempt_at` backoff and attempt count untouched so
    /// a crash-looping row does not retry without backoff.
    async fn reset_in_flight(&self) -> Result<(), sqlx::Error> {
        let res = sqlx::query("UPDATE outbox SET status = 'pending' WHERE status = 'in_flight'")
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
    /// `(bot_id, update_id)` pair is already queued: a Telegram redelivery.
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
    /// Returns rows ordered by database id.
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
        let mut rows: Vec<OutboxRow> = rows
            .iter()
            .map(|r| OutboxRow {
                id: r.get("id"),
                bot_id: r.get("bot_id"),
                update_id: r.get("update_id"),
                payload: r.get("payload"),
                attempts: r.get("attempts"),
            })
            .collect();
        rows.sort_by_key(|r| r.id);
        Ok(rows)
    }

    /// Mark a claimed row delivered.
    pub async fn mark_delivered(&self, id: i64) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE outbox SET status = 'delivered' WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Immediately mark a row dead (permanent failure).
    pub async fn mark_dead(&self, id: i64, error: &str) -> Result<bool, sqlx::Error> {
        let res = sqlx::query(
            "UPDATE outbox SET status = 'dead', last_error = ? WHERE id = ? AND status = 'in_flight'",
        )
        .bind(error)
        .bind(id)
        .execute(&self.pool)
        .await?;
        let affected = res.rows_affected() > 0;
        if !affected {
            tracing::warn!(id, "outbox mark_dead skipped: row not in_flight");
        }
        Ok(affected)
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
        self.mark_failed_with_min_delay(id, error, max_attempts, None)
            .await
    }

    /// Record a failed attempt with an optional minimum delay (e.g. from Retry-After).
    /// Atomically updates the row if `status = 'in_flight'`. Returns `None` if dead-lettered
    /// or if the row is no longer in_flight.
    pub async fn mark_failed_with_min_delay(
        &self,
        id: i64,
        error: &str,
        max_attempts: u32,
        min_delay: Option<Duration>,
    ) -> Result<Option<Duration>, sqlx::Error> {
        let now = now_millis();
        let min_delay_ms = min_delay
            .map(|d| d.as_millis().min(3_600_000) as i64)
            .unwrap_or(0);
        let jitter = 0.8 + fastrand::f64() * 0.4;
        let row = sqlx::query(
            "UPDATE outbox
             SET status = CASE WHEN attempts >= ? THEN 'dead' ELSE 'pending' END,
                 next_attempt_at = CASE
                     WHEN attempts >= ? THEN NULL
                     ELSE ? + MIN(3600000, MAX(?, CAST((5000 * (1 << MIN(MAX(attempts - 1, 0), 10))) * ? AS INTEGER)))
                 END,
                 last_error = ?
             WHERE id = ? AND status = 'in_flight'
             RETURNING status, attempts, next_attempt_at",
        )
        .bind(i64::from(max_attempts))
        .bind(i64::from(max_attempts))
        .bind(now)
        .bind(min_delay_ms)
        .bind(jitter)
        .bind(error)
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            tracing::warn!(id, "outbox mark_failed skipped: row not in_flight");
            return Ok(None);
        };

        let status: String = row.get("status");
        if status == "dead" {
            Ok(None)
        } else {
            let next: Option<i64> = row.get("next_attempt_at");
            let delay_ms = next.unwrap_or(now).saturating_sub(now);
            Ok(Some(Duration::from_millis(delay_ms as u64)))
        }
    }
    /// Outbox rows, newest first. `status` is one of `pending`,
    /// `in_flight`, `delivered`, `dead`, or `all`. Optionally filtered by
    /// bot and capped at `limit` rows.
    pub async fn list(
        &self,
        status: &str,
        bot_id: Option<&str>,
        limit: i64,
    ) -> Result<Vec<OutboxEntry>, sqlx::Error> {
        let mut sql = String::from(
            "SELECT id, bot_id, update_id, status, attempts, next_attempt_at, last_error, created_at
             FROM outbox WHERE 1 = 1",
        );
        if status != "all" {
            sql.push_str(" AND status = ?");
        }
        if bot_id.is_some() {
            sql.push_str(" AND bot_id = ?");
        }
        sql.push_str(" ORDER BY id DESC LIMIT ?");

        let mut query = sqlx::query(&sql);
        if status != "all" {
            query = query.bind(status);
        }
        if let Some(bot) = bot_id {
            query = query.bind(bot);
        }
        let rows = query.bind(limit).fetch_all(&self.pool).await?;
        Ok(rows
            .iter()
            .map(|r| OutboxEntry {
                id: r.get("id"),
                bot_id: r.get("bot_id"),
                update_id: r.get("update_id"),
                status: r.get("status"),
                attempts: r.get("attempts"),
                next_attempt_at: r.get("next_attempt_at"),
                last_error: r.get("last_error"),
                created_at: r.get("created_at"),
            })
            .collect())
    }

    /// Newest stored payload for a `(bot_id, update_id)` pair, whatever its
    /// status. Used by the replay API.
    pub async fn find_payload(
        &self,
        bot_id: &str,
        update_id: i64,
    ) -> Result<Option<String>, sqlx::Error> {
        let row = sqlx::query(
            "SELECT payload FROM outbox
             WHERE bot_id = ? AND update_id = ?
             ORDER BY id DESC LIMIT 1",
        )
        .bind(bot_id)
        .bind(update_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| r.get("payload")))
    }

    /// Drop delivered rows older than `delivered_cutoff`, dead rows older
    /// than `dead_cutoff` (both unix millis).
    pub async fn prune(
        &self,
        delivered_cutoff: i64,
        dead_cutoff: i64,
        max_dead_per_bot: i64,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM outbox WHERE status = 'delivered' AND created_at < ?")
            .bind(delivered_cutoff)
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM outbox WHERE status = 'dead' AND created_at < ?")
            .bind(dead_cutoff)
            .execute(&self.pool)
            .await?;
        // Cap dead letters per bot: keep only the newest `max_dead_per_bot`.
        sqlx::query(
            "DELETE FROM outbox WHERE id IN (
               SELECT id FROM (
                 SELECT id, ROW_NUMBER() OVER (PARTITION BY bot_id ORDER BY id DESC) AS rn
                 FROM outbox WHERE status = 'dead'
               ) WHERE rn > ?
             )",
        )
        .bind(max_dead_per_bot)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Requeue claimed rows as pending without burning attempts.
    /// Used when the circuit breaker opens mid-drain or during shutdown.
    pub async fn requeue(&self, ids: &[i64]) -> Result<(), sqlx::Error> {
        if ids.is_empty() {
            return Ok(());
        }
        let mut builder = sqlx::QueryBuilder::new(
            "UPDATE outbox SET status = 'pending', attempts = MAX(attempts - 1, 0) WHERE id IN (",
        );
        let mut separated = builder.separated(", ");
        for id in ids {
            separated.push_bind(*id);
        }
        separated.push_unseparated(")");
        builder.build().execute(&self.pool).await?;
        Ok(())
    }

    /// Clear backoff timers for a bot's pending rows, so they become due
    /// immediately. Used on circuit-breaker recovery.
    pub async fn wake_pending(&self, bot_id: &str) -> Result<(), sqlx::Error> {
        sqlx::query(
            "UPDATE outbox SET next_attempt_at = NULL WHERE bot_id = ? AND status = 'pending'",
        )
        .bind(bot_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Pending row counts per bot, for the `telegateway.outbox.pending` gauge.
    pub async fn pending_counts(&self) -> Result<Vec<(String, i64)>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT bot_id, COUNT(*) AS n FROM outbox WHERE status = 'pending' GROUP BY bot_id",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .iter()
            .map(|r| (r.get("bot_id"), r.get::<i64, _>("n")))
            .collect())
    }

    // test support
    #[doc(hidden)]
    pub async fn force_due(&self, id: i64) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE outbox SET next_attempt_at = 0 WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    // test support
    #[doc(hidden)]
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

    // test support
    #[doc(hidden)]
    pub async fn status_of(&self, bot_id: &str, update_id: i64) -> Result<String, sqlx::Error> {
        let row = sqlx::query("SELECT status FROM outbox WHERE bot_id = ? AND update_id = ?")
            .bind(bot_id)
            .bind(update_id)
            .fetch_one(&self.pool)
            .await?;
        Ok(row.get("status"))
    }
}
