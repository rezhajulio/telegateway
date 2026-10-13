// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
//! Outbox storage and dispatcher behavior against a stub downstream.
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use telegateway::{
    config::BotConfig,
    outbox::{CircuitBreaker, Decision, Dispatcher, Store, deliver_payload},
};
use tokio::sync::Notify;

struct TestDb {
    _dir: tempfile::TempDir,
    path: String,
}

impl TestDb {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("outbox.db").to_str().unwrap().to_owned();
        Self { _dir: dir, path }
    }

    fn path(&self) -> &str {
        &self.path
    }
}

fn test_bot(downstream_url: String) -> BotConfig {
    BotConfig {
        id: "demo".to_owned(),
        bot_token: None,
        downstream_url,
        webhook_secret: "s3cret".to_owned(),
        rate_limit_per_minute: 30,
    }
}

fn dispatcher_with_breaker(
    store: Store,
    bot: BotConfig,
    max_attempts: u32,
    failure_threshold: u32,
    cooldown: Duration,
) -> Dispatcher {
    Dispatcher::new(
        store,
        reqwest::Client::new(),
        bot.clone(),
        max_attempts,
        Duration::from_millis(50),
        Arc::new(Notify::new()),
        CircuitBreaker::new(bot.id.clone(), failure_threshold, cooldown, cooldown * 4),
    )
}

fn dispatcher(store: Store, bot: BotConfig, max_attempts: u32) -> Dispatcher {
    dispatcher_with_breaker(store, bot, max_attempts, 10_000, Duration::from_secs(3600))
}

/// Stub downstream bot: fails the first `fail_times` requests with 500,
/// then answers 200. Records every body it receives.
async fn stub_downstream(
    fail_times: usize,
) -> (String, tokio::sync::mpsc::UnboundedReceiver<String>) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let counter = Arc::new(AtomicU64::new(0));
    let router = axum::Router::new().route(
        "/webhook",
        axum::routing::post(move |body: String| {
            let tx = tx.clone();
            let counter = counter.clone();
            async move {
                let n = counter.fetch_add(1, Ordering::SeqCst);
                let _ = tx.send(body);
                if (n as usize) < fail_times {
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR
                } else {
                    axum::http::StatusCode::OK
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (format!("http://{addr}/webhook"), rx)
}

#[tokio::test]
async fn insert_dedupes_on_bot_and_update_id() {
    let db = TestDb::new();
    let store = Store::open(db.path()).await.unwrap();
    assert!(store.insert("demo", 1, "{}").await.unwrap());
    assert!(!store.insert("demo", 1, "{}").await.unwrap());
    // Same update_id for a different bot is a different row.
    assert!(store.insert("other", 1, "{}").await.unwrap());
}

#[tokio::test]
async fn claim_bumps_attempts_and_hides_claimed_rows() {
    let db = TestDb::new();
    let store = Store::open(db.path()).await.unwrap();
    store.insert("demo", 1, "{}").await.unwrap();
    store.insert("demo", 2, "{}").await.unwrap();

    let rows = store.claim_due("demo", 32).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|r| r.attempts == 1));

    assert!(store.claim_due("demo", 32).await.unwrap().is_empty());
    // Other bots' rows are untouched.
    store.insert("other", 1, "{}").await.unwrap();
    assert_eq!(store.claim_due("other", 32).await.unwrap().len(), 1);
}

#[tokio::test]
async fn failed_attempts_back_off_then_dead_letter() {
    let db = TestDb::new();
    let store = Store::open(db.path()).await.unwrap();
    store.insert("demo", 1, "{}").await.unwrap();

    // Attempt 1 fails: requeued with a future next_attempt_at.
    let rows = store.claim_due("demo", 32).await.unwrap();
    let delay = store
        .mark_failed(rows[0].id, "boom", 2)
        .await
        .unwrap()
        .expect("should retry");
    assert!(delay.as_secs() >= 4, "backoff starts around 5s");
    assert!(store.claim_due("demo", 32).await.unwrap().is_empty());

    // Attempt 2 fails: max_attempts reached, dead-lettered.
    store.force_due(rows[0].id).await.unwrap();
    let rows = store.claim_due("demo", 32).await.unwrap();
    assert!(
        store
            .mark_failed(rows[0].id, "boom again", 2)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(store.status_of("demo", 1).await.unwrap(), "dead");

    let dead = store.list("dead", None, 50).await.unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].update_id, 1);
    assert_eq!(dead[0].attempts, 2);
    assert_eq!(dead[0].last_error.as_deref(), Some("boom again"));

    let filtered = store.list("dead", Some("other"), 50).await.unwrap();
    assert!(filtered.is_empty());
}

#[tokio::test]
async fn delivered_rows_are_not_reclaimed() {
    let db = TestDb::new();
    let store = Store::open(db.path()).await.unwrap();
    store.insert("demo", 1, "{}").await.unwrap();
    let rows = store.claim_due("demo", 32).await.unwrap();
    store.mark_delivered(rows[0].id).await.unwrap();
    assert!(store.claim_due("demo", 32).await.unwrap().is_empty());
    assert_eq!(store.status_of("demo", 1).await.unwrap(), "delivered");
}

#[tokio::test]
async fn in_flight_rows_are_requeued_on_open() {
    let db = TestDb::new();
    let store = Store::open(db.path()).await.unwrap();
    store.insert("demo", 1, "{}").await.unwrap();
    assert_eq!(store.claim_due("demo", 32).await.unwrap().len(), 1);
    drop(store);

    // Simulate a restart: the in-flight row becomes pending again.
    let store = Store::open(db.path()).await.unwrap();
    let rows = store.claim_due("demo", 32).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].attempts, 2, "attempt counter survives the crash");
}

#[tokio::test]
async fn in_flight_rows_keep_backoff_on_restart() {
    let db = TestDb::new();
    let store = Store::open(db.path()).await.unwrap();
    store.insert("demo", 1, "{}").await.unwrap();
    let rows = store.claim_due("demo", 32).await.unwrap();
    // Simulate failure with backoff in future
    store.mark_failed(rows[0].id, "err", 5).await.unwrap();
    // Re-claim after force_due
    store.force_due(rows[0].id).await.unwrap();
    let rows2 = store.claim_due("demo", 32).await.unwrap();
    assert_eq!(rows2.len(), 1);
    // Now row is in_flight. Simulate crash and restart.
    drop(store);

    let store = Store::open(db.path()).await.unwrap();
    // Status was reset to pending, but attempts and backoff were preserved.
    let entry = store.list("pending", Some("demo"), 1).await.unwrap();
    assert_eq!(entry.len(), 1);
    assert_eq!(entry[0].attempts, 2);
}

#[tokio::test]
async fn list_filters_by_status_bot_and_limit() {
    let db = TestDb::new();
    let store = Store::open(db.path()).await.unwrap();
    store.insert("demo", 1, "{}").await.unwrap();
    store.insert("demo", 2, "{}").await.unwrap();
    store.insert("other", 3, "{}").await.unwrap();
    let rows = store.claim_due("demo", 32).await.unwrap();
    store.mark_delivered(rows[0].id).await.unwrap();
    store.mark_failed(rows[1].id, "x", 1).await.unwrap();

    assert_eq!(store.list("all", None, 50).await.unwrap().len(), 3);
    assert_eq!(store.list("pending", None, 50).await.unwrap().len(), 1);
    assert_eq!(
        store
            .list("delivered", Some("demo"), 50)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(store.list("dead", Some("demo"), 50).await.unwrap().len(), 1);
    assert_eq!(
        store.list("dead", Some("other"), 50).await.unwrap().len(),
        0
    );
    assert_eq!(store.list("all", None, 2).await.unwrap().len(), 2);
    let all = store.list("all", None, 50).await.unwrap();
    assert_eq!(all[0].update_id, 3, "newest first");
    assert_eq!(all[0].status, "pending");
}

#[tokio::test]
async fn find_payload_returns_newest_stored_body() {
    let db = TestDb::new();
    let store = Store::open(db.path()).await.unwrap();
    assert_eq!(store.find_payload("demo", 9).await.unwrap(), None);
    store
        .insert("demo", 9, r#"{"update_id":9,"n":1}"#)
        .await
        .unwrap();
    assert_eq!(
        store.find_payload("demo", 9).await.unwrap().as_deref(),
        Some(r#"{"update_id":9,"n":1}"#)
    );
}

#[tokio::test]
async fn prune_drops_old_rows() {
    let db = TestDb::new();
    let store = Store::open(db.path()).await.unwrap();
    store.insert("demo", 1, "{}").await.unwrap();
    let rows = store.claim_due("demo", 32).await.unwrap();
    store.mark_delivered(rows[0].id).await.unwrap();

    // A cutoff in the past prunes nothing.
    store.prune(1, 1, i64::MAX).await.unwrap();
    assert_eq!(store.status_of("demo", 1).await.unwrap(), "delivered");

    // A cutoff in the future prunes it.
    store.prune(i64::MAX, i64::MAX, i64::MAX).await.unwrap();
    assert!(store.status_of("demo", 1).await.is_err());
}

#[tokio::test]
async fn prune_caps_dead_letters_per_bot_keeping_newest() {
    let db = TestDb::new();
    let store = Store::open(db.path()).await.unwrap();
    for bot in ["a", "b"] {
        for u in 1..=5 {
            store.insert(bot, u, "{}").await.unwrap();
        }
    }
    store.insert("a", 99, "{}").await.unwrap(); // stays pending
    for bot in ["a", "b"] {
        for row in store.claim_due(bot, 5).await.unwrap() {
            store.mark_dead(row.id, "boom").await.unwrap();
        }
    }

    store.prune(0, 0, 2).await.unwrap();

    for bot in ["a", "b"] {
        let dead = store.list("dead", Some(bot), 50).await.unwrap();
        let mut ids: Vec<i64> = dead.iter().map(|r| r.update_id).collect();
        ids.sort();
        assert_eq!(ids, vec![4, 5], "{bot}: newest two dead rows kept");
    }
    assert_eq!(store.status_of("a", 99).await.unwrap(), "pending");
}

#[tokio::test]
async fn wake_pending_clears_backoff() {
    let db = TestDb::new();
    let store = Store::open(db.path()).await.unwrap();
    store.insert("demo", 1, "{}").await.unwrap();
    let rows = store.claim_due("demo", 32).await.unwrap();
    store.mark_failed(rows[0].id, "x", 10).await.unwrap();
    assert!(store.claim_due("demo", 32).await.unwrap().is_empty());

    store.wake_pending("demo").await.unwrap();
    assert_eq!(store.claim_due("demo", 32).await.unwrap().len(), 1);
}

/// Stub downstream that counts every request it receives.
async fn stub_counting_downstream(
    fail_times: usize,
) -> (
    String,
    tokio::sync::mpsc::UnboundedReceiver<String>,
    Arc<AtomicU64>,
) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let counter = Arc::new(AtomicU64::new(0));
    let router = axum::Router::new().route(
        "/webhook",
        axum::routing::post({
            let counter = counter.clone();
            move |body: String| {
                let tx = tx.clone();
                let counter = counter.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    let _ = tx.send(body);
                    if counter.load(Ordering::SeqCst) <= fail_times as u64 {
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR
                    } else {
                        axum::http::StatusCode::OK
                    }
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (format!("http://{addr}/webhook"), rx, counter)
}

#[tokio::test]
async fn breaker_opens_and_fast_fails_without_burning_attempts() {
    let (downstream, _rx, hits) = stub_counting_downstream(usize::MAX).await;
    let db = TestDb::new();
    let store = Store::open(db.path()).await.unwrap();
    for id in 1..=3 {
        store.insert("demo", id, "{}").await.unwrap();
    }

    // Threshold 2, cooldown far in the future: no probe during this test.
    let d = dispatcher_with_breaker(
        store.clone(),
        test_bot(downstream),
        10,
        2,
        Duration::from_secs(3600),
    );
    assert_eq!(d.drain_once().await, 2);
    assert_eq!(hits.load(Ordering::SeqCst), 2, "third row never attempted");

    // The unattempted row is requeued as pending with its attempt counter restored.
    assert_eq!(store.status_of("demo", 3).await.unwrap(), "pending");
    let pending = store.list("pending", Some("demo"), 10).await.unwrap();
    let row3 = pending.iter().find(|r| r.update_id == 3).unwrap();
    assert_eq!(row3.attempts, 0, "requeue must restore pre-claim attempts");

    // Breaker still open: another drain attempts nothing.
    assert_eq!(d.drain_once().await, 0);
    assert_eq!(hits.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn breaker_probe_recovers_and_wakes_queue() {
    let (downstream, _rx, hits) = stub_counting_downstream(1).await;
    let db = TestDb::new();
    let store = Store::open(db.path()).await.unwrap();
    store.insert("demo", 1, "{}").await.unwrap();
    store.insert("demo", 2, "{}").await.unwrap();

    let d = dispatcher_with_breaker(
        store.clone(),
        test_bot(downstream),
        10,
        1,
        Duration::from_millis(100),
    );
    // First row fails -> breaker opens immediately; second row requeued.
    assert_eq!(d.drain_once().await, 1);
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    assert_eq!(store.status_of("demo", 2).await.unwrap(), "pending");

    // Cooldown elapses; the failed row's backoff has not, so make it due
    // the way time would.
    tokio::time::sleep(Duration::from_millis(150)).await;
    store.force_due_by_update("demo", 1).await.unwrap();

    // Probe succeeds: breaker closes, row delivered, queue woken.
    let probe_ran = d.probe().await;
    assert!(probe_ran);
    assert_eq!(hits.load(Ordering::SeqCst), 2);
    assert_eq!(store.status_of("demo", 1).await.unwrap(), "delivered");

    // The requeued row is now due without waiting out its backoff.
    assert_eq!(d.drain_once().await, 1);
    assert_eq!(hits.load(Ordering::SeqCst), 3);
    assert_eq!(store.status_of("demo", 2).await.unwrap(), "delivered");
}

#[tokio::test]
async fn dispatcher_delivers_pending_rows() {
    let (downstream, mut rx) = stub_downstream(0).await;
    let db = TestDb::new();
    let store = Store::open(db.path()).await.unwrap();
    store.insert("demo", 7, r#"{"update_id":7}"#).await.unwrap();

    let d = dispatcher(store.clone(), test_bot(downstream), 3);
    assert_eq!(d.drain_once().await, 1);

    let body = rx.recv().await.expect("downstream should be called");
    assert_eq!(body, r#"{"update_id":7}"#);
    assert_eq!(store.status_of("demo", 7).await.unwrap(), "delivered");
    // Second drain finds nothing to do.
    assert_eq!(d.drain_once().await, 0);
}

#[tokio::test]
async fn dispatcher_retries_then_succeeds() {
    let (downstream, mut rx) = stub_downstream(1).await;
    let db = TestDb::new();
    let store = Store::open(db.path()).await.unwrap();
    store.insert("demo", 7, "{}").await.unwrap();

    let d = dispatcher(store.clone(), test_bot(downstream), 3);
    assert_eq!(d.drain_once().await, 1);
    assert_eq!(store.status_of("demo", 7).await.unwrap(), "pending");
    let _ = rx.recv().await;

    // Backoff has not elapsed: nothing due yet.
    assert_eq!(d.drain_once().await, 0);

    store.force_due_by_update("demo", 7).await.unwrap();
    assert_eq!(d.drain_once().await, 1);
    assert_eq!(store.status_of("demo", 7).await.unwrap(), "delivered");
    let _ = rx.recv().await;
}

#[tokio::test]
async fn dispatcher_dead_letters_after_max_attempts() {
    let (downstream, _rx) = stub_downstream(usize::MAX).await;
    let db = TestDb::new();
    let store = Store::open(db.path()).await.unwrap();
    store.insert("demo", 7, "{}").await.unwrap();

    let d = dispatcher(store.clone(), test_bot(downstream), 2);
    d.drain_once().await;
    store.force_due_by_update("demo", 7).await.unwrap();
    d.drain_once().await;

    assert_eq!(store.status_of("demo", 7).await.unwrap(), "dead");
    let dead = store.list("dead", Some("demo"), 50).await.unwrap();
    assert_eq!(dead.len(), 1);
    assert!(dead[0].last_error.as_deref().unwrap().contains("500"));
}

#[tokio::test]
async fn dispatcher_run_parks_when_breaker_open_and_queue_empty() {
    let db = TestDb::new();
    let store = Store::open(db.path()).await.unwrap();
    let bot = test_bot("http://127.0.0.1:9/unused".to_owned());
    let d = Arc::new(dispatcher_with_breaker(
        store,
        bot,
        3,
        1,
        Duration::from_millis(50),
    ));

    // Fast-trip breaker
    d.decide(); // ensure breaker initialized
    // Simulate open breaker state
    let (tx, rx) = tokio::sync::watch::channel(false);
    let run_handle = tokio::spawn(d.clone().run(rx));

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !run_handle.is_finished(),
        "dispatcher should be running/parked"
    );

    tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_millis(500), run_handle)
        .await
        .expect("dispatcher did not hang on shutdown")
        .unwrap();
}

#[tokio::test]
async fn requeue_keeps_attempts() {
    let db = TestDb::new();
    let store = Store::open(db.path()).await.unwrap();
    store.insert("demo", 1, "{}").await.unwrap();
    let rows = store.claim_due("demo", 1).await.unwrap();
    assert_eq!(rows[0].attempts, 1);
    store.requeue(&[rows[0].id]).await.unwrap();
    let list = store.list("pending", Some("demo"), 10).await.unwrap();
    assert_eq!(
        list[0].attempts, 0,
        "requeue decrements back to pre-claim attempts"
    );
}

#[tokio::test]
async fn permanent_400_dead_letters_immediately_and_leaves_breaker_closed() {
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let counter = Arc::new(AtomicU64::new(0));
    let router = axum::Router::new().route(
        "/webhook",
        axum::routing::post({
            let counter = counter.clone();
            move |body: String| {
                let tx = tx.clone();
                let counter = counter.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    let _ = tx.send(body);
                    axum::http::StatusCode::BAD_REQUEST
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    let db = TestDb::new();
    let store = Store::open(db.path()).await.unwrap();
    store.insert("demo", 101, r#"{"bad":true}"#).await.unwrap();

    let bot = test_bot(format!("http://{addr}/webhook"));
    let d = dispatcher_with_breaker(store.clone(), bot, 5, 2, Duration::from_secs(60));

    assert_eq!(d.drain_once().await, 1);
    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "exactly one hit downstream"
    );
    assert_eq!(store.status_of("demo", 101).await.unwrap(), "dead");

    // Breaker must NOT trip or count failures for permanent error
    assert_eq!(d.decide(), Decision::Allow, "breaker remains closed");
}

#[tokio::test]
async fn retry_after_honored_on_429_or_503() {
    let router = axum::Router::new().route(
        "/webhook",
        axum::routing::post(|| async {
            let mut headers = axum::http::HeaderMap::new();
            headers.insert(axum::http::header::RETRY_AFTER, "30".parse().unwrap());
            (
                axum::http::StatusCode::TOO_MANY_REQUESTS,
                headers,
                "calm down",
            )
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    let db = TestDb::new();
    let store = Store::open(db.path()).await.unwrap();
    store.insert("demo", 201, "{}").await.unwrap();

    let bot = test_bot(format!("http://{addr}/webhook"));
    let d = dispatcher_with_breaker(store.clone(), bot, 5, 10, Duration::from_secs(60));

    assert_eq!(d.drain_once().await, 1);
    assert_eq!(store.status_of("demo", 201).await.unwrap(), "pending");

    let entries = store.list("pending", Some("demo"), 10).await.unwrap();
    let next_attempt = entries[0]
        .next_attempt_at
        .expect("must have next attempt set");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let diff = next_attempt - now;
    assert!(
        diff >= 25_000,
        "next attempt should honor 30s retry-after, diff was {diff}ms"
    );
}

#[tokio::test]
async fn transient_500_retries_and_trips_breaker() {
    let (downstream, _rx, counter) = stub_counting_downstream(10).await;
    let db = TestDb::new();
    let store = Store::open(db.path()).await.unwrap();
    store.insert("demo", 301, "{}").await.unwrap();

    let bot = test_bot(downstream);
    // Breaker trips after 1 failure
    let d = dispatcher_with_breaker(store.clone(), bot, 5, 1, Duration::from_secs(60));

    assert_eq!(d.drain_once().await, 1);
    assert_eq!(counter.load(Ordering::SeqCst), 1);
    assert_eq!(store.status_of("demo", 301).await.unwrap(), "pending");
    assert!(matches!(d.decide(), Decision::Deny { .. }));
}

#[tokio::test]
async fn transport_error_does_not_contain_downstream_url() {
    let secret_url = "http://bot-secret-token:pass@127.0.0.1:1/private";
    let bot = test_bot(secret_url.to_owned());
    let client = reqwest::Client::new();
    let err = deliver_payload(&client, &bot, 1, "{}").await.unwrap_err();
    let err_msg = err.to_string();
    assert!(
        !err_msg.contains("bot-secret-token"),
        "error string must not leak credentials from url: {err_msg}"
    );
    assert!(
        !err_msg.contains("127.0.0.1:1/private"),
        "error string must not contain url: {err_msg}"
    );
}

#[tokio::test]
async fn shutdown_signal_stops_run_without_losing_rows() {
    let db = TestDb::new();
    let store = Store::open(db.path()).await.unwrap();
    for id in 1..=5 {
        store.insert("demo", id, "{}").await.unwrap();
    }

    let (tx, rx) = tokio::sync::watch::channel(false);
    let router = axum::Router::new().route(
        "/webhook",
        axum::routing::post(|| async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            axum::http::StatusCode::OK
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    let bot = test_bot(format!("http://{addr}/webhook"));
    let d = Arc::new(dispatcher_with_breaker(
        store.clone(),
        bot,
        5,
        10,
        Duration::from_secs(60),
    ));

    let d_clone = d.clone();
    let handle = tokio::spawn(d_clone.run(rx));

    tokio::time::sleep(Duration::from_millis(20)).await;
    tx.send(true).unwrap();

    handle.await.unwrap();

    let all = store.list("all", Some("demo"), 10).await.unwrap();
    assert_eq!(all.len(), 5);
    for row in &all {
        assert!(
            row.status == "delivered" || row.status == "pending",
            "unexpected status {}",
            row.status
        );
    }
}

#[tokio::test]
async fn mark_failed_on_non_in_flight_row_is_noop() {
    let db = TestDb::new();
    let store = Store::open(db.path()).await.unwrap();
    store.insert("demo", 1, "{}").await.unwrap();

    // Row is pending, not in_flight
    let res = store.mark_failed(1, "err", 5).await.unwrap();
    assert_eq!(res, None);
    assert_eq!(store.status_of("demo", 1).await.unwrap(), "pending");

    // Claim and deliver
    let rows = store.claim_due("demo", 1).await.unwrap();
    store.mark_delivered(rows[0].id).await.unwrap();

    // Row is delivered, not in_flight
    let res2 = store.mark_failed(rows[0].id, "err", 5).await.unwrap();
    assert_eq!(res2, None);
    assert_eq!(store.status_of("demo", 1).await.unwrap(), "delivered");
}
