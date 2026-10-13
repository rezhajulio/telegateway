// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
//! Outbox storage and dispatcher behavior against a stub downstream.
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use telegateway::{
    config::BotConfig,
    outbox::{CircuitBreaker, Dispatcher, Store},
};
use tokio::sync::Notify;

fn temp_db() -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir()
        .join(format!(
            "telegateway-outbox-test-{}-{n}.db",
            std::process::id()
        ))
        .to_str()
        .unwrap()
        .to_owned()
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
    let store = Store::open(&temp_db()).await.unwrap();
    assert!(store.insert("demo", 1, "{}").await.unwrap());
    assert!(!store.insert("demo", 1, "{}").await.unwrap());
    // Same update_id for a different bot is a different row.
    assert!(store.insert("other", 1, "{}").await.unwrap());
}

#[tokio::test]
async fn claim_bumps_attempts_and_hides_claimed_rows() {
    let store = Store::open(&temp_db()).await.unwrap();
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
    let store = Store::open(&temp_db()).await.unwrap();
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
    let store = Store::open(&temp_db()).await.unwrap();
    store.insert("demo", 1, "{}").await.unwrap();
    let rows = store.claim_due("demo", 32).await.unwrap();
    store.mark_delivered(rows[0].id).await.unwrap();
    assert!(store.claim_due("demo", 32).await.unwrap().is_empty());
    assert_eq!(store.status_of("demo", 1).await.unwrap(), "delivered");
}

#[tokio::test]
async fn in_flight_rows_are_requeued_on_open() {
    let path = temp_db();
    let store = Store::open(&path).await.unwrap();
    store.insert("demo", 1, "{}").await.unwrap();
    assert_eq!(store.claim_due("demo", 32).await.unwrap().len(), 1);
    drop(store);

    // Simulate a restart: the in-flight row becomes pending again.
    let store = Store::open(&path).await.unwrap();
    let rows = store.claim_due("demo", 32).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].attempts, 2, "attempt counter survives the crash");
}

#[tokio::test]
async fn list_filters_by_status_bot_and_limit() {
    let store = Store::open(&temp_db()).await.unwrap();
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
    let store = Store::open(&temp_db()).await.unwrap();
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
    let store = Store::open(&temp_db()).await.unwrap();
    store.insert("demo", 1, "{}").await.unwrap();
    let rows = store.claim_due("demo", 32).await.unwrap();
    store.mark_delivered(rows[0].id).await.unwrap();

    // A cutoff in the past prunes nothing.
    store.prune(1, 1).await.unwrap();
    assert_eq!(store.status_of("demo", 1).await.unwrap(), "delivered");

    // A cutoff in the future prunes it.
    store.prune(i64::MAX, i64::MAX).await.unwrap();
    assert!(store.status_of("demo", 1).await.is_err());
}

#[tokio::test]
async fn wake_pending_clears_backoff() {
    let store = Store::open(&temp_db()).await.unwrap();
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
    let store = Store::open(&temp_db()).await.unwrap();
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

    // The unattempted row is requeued as pending with its single claim
    // counted, not dead-lettered or lost.
    assert_eq!(store.status_of("demo", 3).await.unwrap(), "pending");
    // Breaker still open: another drain attempts nothing.
    assert_eq!(d.drain_once().await, 0);
    assert_eq!(hits.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn breaker_probe_recovers_and_wakes_queue() {
    let (downstream, _rx, hits) = stub_counting_downstream(1).await;
    let store = Store::open(&temp_db()).await.unwrap();
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
    d.probe().await;
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
    let store = Store::open(&temp_db()).await.unwrap();
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
    let store = Store::open(&temp_db()).await.unwrap();
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
    let store = Store::open(&temp_db()).await.unwrap();
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
