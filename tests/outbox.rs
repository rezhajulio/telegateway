// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
//! Outbox storage and dispatcher behavior against a stub downstream.
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use telegateway::{
    config::BotConfig,
    outbox::{Dispatcher, Store},
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

fn dispatcher(store: Store, bot: BotConfig, max_attempts: u32) -> Dispatcher {
    Dispatcher::new(
        store,
        reqwest::Client::new(),
        bot,
        max_attempts,
        Duration::from_millis(50),
        Arc::new(Notify::new()),
    )
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

    let dead = store.list_dead(None).await.unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].update_id, 1);
    assert_eq!(dead[0].attempts, 2);
    assert_eq!(dead[0].last_error.as_deref(), Some("boom again"));

    let filtered = store.list_dead(Some("other")).await.unwrap();
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
    let dead = store.list_dead(Some("demo")).await.unwrap();
    assert_eq!(dead.len(), 1);
    assert!(dead[0].last_error.as_deref().unwrap().contains("500"));
}
