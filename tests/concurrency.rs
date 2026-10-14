// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
//! Integration tests for bounded concurrent outbox delivery.
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use telegateway::{
    config::BotConfig,
    outbox::{CircuitBreaker, Dispatcher, Store},
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

fn dispatcher(store: Store, bot: BotConfig, concurrency: usize) -> Dispatcher {
    Dispatcher::new(
        store,
        reqwest::Client::new(),
        bot.clone(),
        10,
        Duration::from_millis(50),
        Arc::new(Notify::new()),
        CircuitBreaker::new(
            bot.id.clone(),
            10_000,
            Duration::from_secs(3600),
            Duration::from_secs(3600),
        ),
    )
    .with_concurrency(concurrency)
}

fn dispatcher_with_breaker(
    store: Store,
    bot: BotConfig,
    failure_threshold: u32,
    cooldown: Duration,
    concurrency: usize,
) -> Dispatcher {
    Dispatcher::new(
        store,
        reqwest::Client::new(),
        bot.clone(),
        10,
        Duration::from_millis(50),
        Arc::new(Notify::new()),
        CircuitBreaker::new(bot.id.clone(), failure_threshold, cooldown, cooldown * 4),
    )
    .with_concurrency(concurrency)
}

/// Stub downstream bot: sleeps for `delay`, returns 500 if body contains `"FAIL"`,
/// else 200. Sends each received body to an unbounded mpsc channel.
async fn stub_server(delay: Duration) -> (String, tokio::sync::mpsc::UnboundedReceiver<String>) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let router = axum::Router::new().route(
        "/webhook",
        axum::routing::post(move |body: String| {
            let tx = tx.clone();
            async move {
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                let status = if body.contains("FAIL") {
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR
                } else {
                    axum::http::StatusCode::OK
                };
                let _ = tx.send(body);
                status
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
async fn cross_chat_concurrency_speeds_up_delivery() {
    let delay = Duration::from_millis(200);
    let (downstream, mut rx) = stub_server(delay).await;

    // Concurrency 4 across 4 different chats
    let db = TestDb::new();
    let store = Store::open(db.path()).await.unwrap();
    for chat_id in 1..=4 {
        store
            .insert(
                "demo",
                chat_id,
                Some(chat_id),
                &format!(r#"{{"chat":{chat_id}}}"#),
            )
            .await
            .unwrap();
    }

    let d4 = dispatcher(store.clone(), test_bot(downstream.clone()), 4);
    let start = Instant::now();
    assert_eq!(d4.drain_once().await, 4);
    let elapsed4 = start.elapsed();

    for _ in 0..4 {
        rx.recv().await.unwrap();
    }

    // 4 sequential rows would take ~800ms. Concurrent 4 should finish in < 0.6 * 800ms = 480ms.
    assert!(
        elapsed4 < Duration::from_millis(480),
        "concurrency 4 took too long: {elapsed4:?}"
    );

    // Concurrency 1 across 4 different chats
    let db1 = TestDb::new();
    let store1 = Store::open(db1.path()).await.unwrap();
    for chat_id in 1..=4 {
        store1
            .insert(
                "demo",
                chat_id,
                Some(chat_id),
                &format!(r#"{{"chat":{chat_id}}}"#),
            )
            .await
            .unwrap();
    }

    let d1 = dispatcher(store1.clone(), test_bot(downstream), 1);
    let start1 = Instant::now();
    assert_eq!(d1.drain_once().await, 4);
    let elapsed1 = start1.elapsed();

    for _ in 0..4 {
        rx.recv().await.unwrap();
    }

    // Concurrency 1 should take at least 0.8 * 800ms = 640ms.
    assert!(
        elapsed1 >= Duration::from_millis(640),
        "concurrency 1 finished too quickly: {elapsed1:?}"
    );
}

#[tokio::test]
async fn same_chat_rows_delivered_in_update_id_order() {
    let (downstream, mut rx) = stub_server(Duration::from_millis(20)).await;
    let db = TestDb::new();
    let store = Store::open(db.path()).await.unwrap();

    // 6 updates for the same chat, inserted in order
    for update_id in 1..=6 {
        store
            .insert(
                "demo",
                update_id,
                Some(100),
                &format!(r#"{{"update_id":{update_id}}}"#),
            )
            .await
            .unwrap();
    }

    // Even with concurrency 8, rows for chat 100 must be delivered strictly sequentially
    let d = dispatcher(store.clone(), test_bot(downstream), 8);
    assert_eq!(d.drain_once().await, 6);

    for expected_id in 1..=6 {
        let body = rx.recv().await.unwrap();
        assert_eq!(body, format!(r#"{{"update_id":{expected_id}}}"#));
    }
}

#[tokio::test]
async fn failing_row_stops_own_chat_without_affecting_other_chats() {
    let (downstream, mut rx) = stub_server(Duration::ZERO).await;
    let db = TestDb::new();
    let store = Store::open(db.path()).await.unwrap();

    // Chat 1 has 3 updates, second update fails
    store
        .insert("demo", 101, Some(1), r#"{"msg":"c1-1"}"#)
        .await
        .unwrap();
    store
        .insert("demo", 102, Some(1), r#"{"msg":"c1-2-FAIL"}"#)
        .await
        .unwrap();
    store
        .insert("demo", 103, Some(1), r#"{"msg":"c1-3"}"#)
        .await
        .unwrap();

    // Chat 2 has 2 updates, all succeed
    store
        .insert("demo", 201, Some(2), r#"{"msg":"c2-1"}"#)
        .await
        .unwrap();
    store
        .insert("demo", 202, Some(2), r#"{"msg":"c2-2"}"#)
        .await
        .unwrap();

    let d = dispatcher(store.clone(), test_bot(downstream), 4);
    // Attempted: c1-1 (ok), c1-2 (fail -> c1-3 stopped and requeued), c2-1 (ok), c2-2 (ok) = 4 total attempts
    assert_eq!(d.drain_once().await, 4);

    // Collect all bodies received by downstream
    let mut received = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        received.push(msg);
    }
    assert_eq!(received.len(), 4);
    assert!(received.contains(&r#"{"msg":"c1-1"}"#.to_string()));
    assert!(received.contains(&r#"{"msg":"c1-2-FAIL"}"#.to_string()));
    assert!(received.contains(&r#"{"msg":"c2-1"}"#.to_string()));
    assert!(received.contains(&r#"{"msg":"c2-2"}"#.to_string()));
    assert!(!received.contains(&r#"{"msg":"c1-3"}"#.to_string()));

    // Check status in store: c1-3 was NOT sent, stayed pending with attempts = 0
    let pending = store.list("pending", Some("demo"), 10).await.unwrap();
    let row103 = pending.iter().find(|r| r.update_id == 103).unwrap();
    assert_eq!(
        row103.attempts, 0,
        "c1-3 must preserve pre-claim attempts 0"
    );

    // c1-2 failed, attempts is 1
    let row102 = pending.iter().find(|r| r.update_id == 102).unwrap();
    assert_eq!(row102.attempts, 1);

    // c2 updates delivered
    assert_eq!(store.status_of("demo", 201).await.unwrap(), "delivered");
    assert_eq!(store.status_of("demo", 202).await.unwrap(), "delivered");
}

#[tokio::test]
async fn none_chat_key_rows_run_concurrently() {
    let delay = Duration::from_millis(200);
    let (downstream, mut rx) = stub_server(delay).await;
    let db = TestDb::new();
    let store = Store::open(db.path()).await.unwrap();

    // 4 rows with None chat_key
    for id in 1..=4 {
        store
            .insert("demo", id, None, &format!(r#"{{"none":{id}}}"#))
            .await
            .unwrap();
    }

    let d = dispatcher(store.clone(), test_bot(downstream), 4);
    let start = Instant::now();
    assert_eq!(d.drain_once().await, 4);
    let elapsed = start.elapsed();

    for _ in 0..4 {
        rx.recv().await.unwrap();
    }

    // Concurrent 4 finishes in < 0.6 * 800ms = 480ms
    assert!(
        elapsed < Duration::from_millis(480),
        "None chat_key rows did not run concurrently: {elapsed:?}"
    );
}

#[tokio::test]
async fn shutdown_and_breaker_mid_batch_leaves_no_in_flight_rows() {
    let (downstream, _rx) = stub_server(Duration::from_millis(50)).await;
    let db = TestDb::new();
    let store = Store::open(db.path()).await.unwrap();

    // Insert 6 rows across 6 chats
    for id in 1..=6 {
        store
            .insert("demo", id, Some(id), &format!(r#"{{"id":{id}}}"#))
            .await
            .unwrap();
    }

    // Concurrency 1 so shutdown or breaker can take effect while rows remain in the batch
    let (tx, rx) = tokio::sync::watch::channel(false);
    let d = dispatcher(store.clone(), test_bot(downstream), 1);

    // Trigger shutdown right after drain begins
    let d_task = tokio::spawn(async move { d.drain_once_with_shutdown(&rx).await });
    tokio::time::sleep(Duration::from_millis(60)).await;
    tx.send(true).unwrap();

    let handled = d_task.await.unwrap();
    assert!(handled > 0 && handled < 6, "handled: {handled}");

    // All rows in database must be either 'delivered' or 'pending', NEVER 'in_flight'
    let all = store.list("all", Some("demo"), 20).await.unwrap();
    assert_eq!(all.len(), 6);
    for row in all {
        assert!(
            row.status == "delivered" || row.status == "pending",
            "row {} had status {}",
            row.update_id,
            row.status
        );
    }

    // Breaker open test: threshold 1 failure trips breaker immediately
    let (bad_downstream, _bad_rx) = stub_server(Duration::ZERO).await;
    let db2 = TestDb::new();
    let store2 = Store::open(db2.path()).await.unwrap();

    store2
        .insert("demo", 1, Some(1), r#"{"msg":"FAIL"}"#)
        .await
        .unwrap();
    for id in 2..=5 {
        store2
            .insert("demo", id, Some(id), &format!(r#"{{"id":{id}}}"#))
            .await
            .unwrap();
    }

    let d_breaker = dispatcher_with_breaker(
        store2.clone(),
        test_bot(bad_downstream),
        1,
        Duration::from_secs(60),
        1,
    );
    let handled_breaker = d_breaker.drain_once().await;
    assert_eq!(handled_breaker, 1);

    // All rows must be either 'pending' or 'dead', none 'in_flight'
    let all2 = store2.list("all", Some("demo"), 20).await.unwrap();
    assert_eq!(all2.len(), 5);
    for row in all2 {
        assert!(
            row.status == "delivered" || row.status == "pending" || row.status == "dead",
            "row {} had status {}",
            row.update_id,
            row.status
        );
    }
}
