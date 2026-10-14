// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
//! Webhook ingress: routing, secret validation, rate limiting, and
//! outbox persistence, through the real router.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::json;
use std::net::SocketAddr;
use telegateway::{app, config::Config};
use tower::ServiceExt;

const SECRET: &str = "test-secret";
const ADMIN_SECRET: &str = "admin-secret-0123456789-abcdefghijklmnop";

fn test_config(downstream_url: String, rate_limit_per_minute: u32) -> (tempfile::TempDir, Config) {
    test_config_full(downstream_url, rate_limit_per_minute, false)
}

fn test_config_full(
    downstream_url: String,
    rate_limit_per_minute: u32,
    with_admin: bool,
) -> (tempfile::TempDir, Config) {
    test_config_with_admin(
        downstream_url,
        rate_limit_per_minute,
        with_admin,
        None,
        10_000,
    )
}

fn test_config_with_admin(
    downstream_url: String,
    rate_limit_per_minute: u32,
    with_admin: bool,
    admin_listen: Option<&str>,
    max_pending: u64,
) -> (tempfile::TempDir, Config) {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let db = tmp.path().join("outbox.db").to_str().unwrap().to_owned();
    let admin = if with_admin {
        if let Some(listen) = admin_listen {
            format!("[admin]\ntoken = \"{ADMIN_SECRET}\"\nlisten = \"{listen}\"\n")
        } else {
            format!("[admin]\ntoken = \"{ADMIN_SECRET}\"\n")
        }
    } else {
        String::new()
    };
    let toml = format!(
        r#"
listen = "127.0.0.1:0"

[outbox]
path = "{db}"
max_pending = {max_pending}

{admin}
[[bots]]
id = "demo"
downstream_url = "{downstream_url}"
webhook_secret = "{SECRET}"
rate_limit_per_minute = {rate_limit_per_minute}
"#
    );
    let cfg = telegateway::config::load_from_str(&toml).unwrap();
    (tmp, cfg)
}

fn authed(path: &str, body: serde_json::Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json")
        .header("x-telegram-bot-api-secret-token", SECRET)
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn update(id: i64, chat: i64) -> serde_json::Value {
    json!({
        "update_id": id,
        "message": {
            "message_id": 1,
            "chat": { "id": chat, "type": "private" },
            "text": "hello"
        }
    })
}

/// Spin up a stub downstream bot that records bodies and answers 200.
async fn stub_downstream() -> (String, tokio::sync::mpsc::UnboundedReceiver<String>) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let router = axum::Router::new().route(
        "/webhook",
        axum::routing::post(move |body: String| {
            let tx = tx.clone();
            async move {
                let _ = tx.send(body);
                "ok"
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
async fn health_endpoints_answer_ok() {
    let (downstream, _rx) = stub_downstream().await;
    let (_tmp, config) = test_config(downstream, 30);
    let built = app::build_router(config).await.unwrap();

    for path in ["/healthz", "/readyz"] {
        let resp = built
            .router
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "path {path}");
    }
}

#[tokio::test]
async fn unknown_bot_is_404() {
    let (downstream, _rx) = stub_downstream().await;
    let (_tmp, config) = test_config(downstream, 30);
    let built = app::build_router(config).await.unwrap();

    let resp = built
        .router
        .oneshot(authed("/webhooks/nope", update(1, 7)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn bad_secret_is_401() {
    let (downstream, _rx) = stub_downstream().await;
    let (_tmp, config) = test_config(downstream, 30);
    let built = app::build_router(config).await.unwrap();

    let req = Request::builder()
        .method("POST")
        .uri("/webhooks/demo")
        .header("content-type", "application/json")
        .header("x-telegram-bot-api-secret-token", "wrong")
        .body(Body::from(update(1, 7).to_string()))
        .unwrap();
    let resp = built.router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn accepted_update_is_persisted_then_forwarded() {
    let (downstream, mut rx) = stub_downstream().await;
    let (_tmp, config) = test_config(downstream, 30);
    let built = app::build_router(config).await.unwrap();

    // Ingest answers 200 once the update is persisted, without waiting
    // for downstream.
    let resp = built
        .router
        .clone()
        .oneshot(authed("/webhooks/demo", update(99, 7)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(rx.try_recv().is_err(), "delivery is asynchronous");

    built.dispatchers[0].drain_once().await;
    let forwarded = rx.recv().await.expect("downstream should be called");
    let body: serde_json::Value = serde_json::from_str(&forwarded).unwrap();
    assert_eq!(body["update_id"], 99);
    assert_eq!(
        built.store.status_of("demo", 99).await.unwrap(),
        "delivered"
    );
}

#[tokio::test]
async fn duplicate_update_is_accepted_once() {
    let (downstream, mut rx) = stub_downstream().await;
    let (_tmp, config) = test_config(downstream, 30);
    let built = app::build_router(config).await.unwrap();

    for _ in 0..2 {
        let resp = built
            .router
            .clone()
            .oneshot(authed("/webhooks/demo", update(5, 7)))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    built.dispatchers[0].drain_once().await;
    assert!(rx.recv().await.is_some());
    assert!(
        rx.try_recv().is_err(),
        "duplicate must not be delivered twice"
    );
}

#[tokio::test]
async fn webhook_stores_chat_key_for_message_update() {
    let (downstream, _rx) = stub_downstream().await;
    let (_tmp, config) = test_config(downstream, 30);
    let built = app::build_router(config).await.unwrap();

    let resp = built
        .router
        .clone()
        .oneshot(authed("/webhooks/demo", update(1001, 77)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let rows = built.store.claim_due("demo", 10).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].update_id, 1001);
    assert_eq!(rows[0].chat_key, Some(77));
}

#[tokio::test]
async fn rate_limited_updates_answer_200_without_forwarding() {
    let (downstream, mut rx) = stub_downstream().await;
    // Quota of 1 per minute: the second update from the same chat is rejected.
    let (_tmp, config) = test_config(downstream, 1);
    let built = app::build_router(config).await.unwrap();

    let first = built
        .router
        .clone()
        .oneshot(authed("/webhooks/demo", update(1, 7)))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    built.dispatchers[0].drain_once().await;
    assert!(rx.recv().await.is_some());

    let second = built
        .router
        .oneshot(authed("/webhooks/demo", update(2, 7)))
        .await
        .unwrap();
    // 200, not 429: Telegram must not retry a policy rejection.
    assert_eq!(second.status(), StatusCode::OK);
    built.dispatchers[0].drain_once().await;
    assert!(
        rx.try_recv().is_err(),
        "rate-limited update must not reach downstream"
    );
}

#[tokio::test]
async fn downstream_outage_is_accepted_for_retry() {
    // Nothing listens on this port.
    let (_tmp, config) = test_config("http://127.0.0.1:1/webhook".to_owned(), 30);
    let built = app::build_router(config).await.unwrap();

    let resp = built
        .router
        .oneshot(authed("/webhooks/demo", update(1, 7)))
        .await
        .unwrap();
    // 200, not 502: the update is persisted and retried in the background.
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(built.store.status_of("demo", 1).await.unwrap(), "pending");
}

fn admin_get(path: &str, token: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder().method("GET").uri(path);
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    builder.body(Body::empty()).unwrap()
}

fn admin_post(path: &str, token: Option<&str>, body: serde_json::Value) -> Request<Body> {
    let mut builder = Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json");
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    builder.body(Body::from(body.to_string())).unwrap()
}

async fn read_json(router: axum::Router, req: Request<Body>) -> (StatusCode, serde_json::Value) {
    let resp = router.oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let value = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, value)
}

#[tokio::test]
async fn outbox_list_needs_admin_token() {
    let (downstream, _rx) = stub_downstream().await;
    let (_tmp, config) = test_config_full(downstream, 30, true);
    let built = app::build_router(config).await.unwrap();

    let no_token = built
        .router
        .clone()
        .oneshot(admin_get("/outbox", None))
        .await
        .unwrap();
    assert_eq!(no_token.status(), StatusCode::UNAUTHORIZED);

    let wrong = built
        .router
        .clone()
        .oneshot(admin_get("/outbox", Some("wrong")))
        .await
        .unwrap();
    assert_eq!(wrong.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn outbox_list_filters_and_validates() {
    let (downstream, _rx) = stub_downstream().await;
    let (_tmp, config) = test_config_full(downstream, 30, true);
    let built = app::build_router(config).await.unwrap();

    // Seed one pending row via the webhook.
    let resp = built
        .router
        .clone()
        .oneshot(authed("/webhooks/demo", update(11, 7)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Default status is dead: nothing there.
    let (status, rows) = read_json(
        built.router.clone(),
        admin_get("/outbox", Some(ADMIN_SECRET)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(rows, json!([]));

    let (status, rows) = read_json(
        built.router.clone(),
        admin_get("/outbox?status=pending&bot_id=demo", Some(ADMIN_SECRET)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(rows.as_array().unwrap().len(), 1);
    assert_eq!(rows[0]["update_id"], 11);
    assert_eq!(rows[0]["status"], "pending");

    let resp = built
        .router
        .oneshot(admin_get("/outbox?status=bogus", Some(ADMIN_SECRET)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn admin_routes_missing_without_token_config() {
    let (downstream, _rx) = stub_downstream().await;
    let (_tmp, config) = test_config(downstream, 30);
    let built = app::build_router(config).await.unwrap();

    for req in [
        admin_get("/outbox", Some(ADMIN_SECRET)),
        admin_post(
            "/outbox/replay",
            Some(ADMIN_SECRET),
            json!({"bot_id": "demo", "update_id": 1}),
        ),
    ] {
        let resp = built.router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}

#[tokio::test]
async fn replay_delivers_stored_update() {
    let (downstream, mut rx) = stub_downstream().await;
    let (_tmp, config) = test_config_full(downstream, 30, true);
    let built = app::build_router(config).await.unwrap();

    built
        .router
        .clone()
        .oneshot(authed("/webhooks/demo", update(21, 7)))
        .await
        .unwrap();

    let (status, replayed) = read_json(
        built.router,
        admin_post(
            "/outbox/replay",
            Some(ADMIN_SECRET),
            json!({"bot_id": "demo", "update_id": 21}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(replayed["ok"], true);
    assert_eq!(replayed["update_id"], 21);
    assert_eq!(replayed["downstream_status"], 200);

    let forwarded = rx.recv().await.expect("downstream should be called");
    let body: serde_json::Value = serde_json::from_str(&forwarded).unwrap();
    assert_eq!(body["update_id"], 21);
}

#[tokio::test]
async fn replay_accepts_adhoc_payload() {
    let (downstream, mut rx) = stub_downstream().await;
    let (_tmp, config) = test_config_full(downstream, 30, true);
    let built = app::build_router(config).await.unwrap();

    let (status, _) = read_json(
        built.router,
        admin_post(
            "/outbox/replay",
            Some(ADMIN_SECRET),
            json!({"bot_id": "demo", "payload": update(22, 7)}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let forwarded = rx.recv().await.expect("downstream should be called");
    let body: serde_json::Value = serde_json::from_str(&forwarded).unwrap();
    assert_eq!(body["update_id"], 22);
}

#[tokio::test]
async fn replay_rejects_bad_requests() {
    let (downstream, _rx) = stub_downstream().await;
    let (_tmp, config) = test_config_full(downstream, 30, true);
    let built = app::build_router(config).await.unwrap();

    let post = async |body: serde_json::Value, token: Option<&str>| {
        built
            .router
            .clone()
            .oneshot(admin_post("/outbox/replay", token, body))
            .await
            .unwrap()
            .status()
    };

    // Unknown stored update.
    assert_eq!(
        post(
            json!({"bot_id": "demo", "update_id": 999}),
            Some(ADMIN_SECRET)
        )
        .await,
        StatusCode::NOT_FOUND
    );
    // Unknown bot.
    assert_eq!(
        post(
            json!({"bot_id": "nope", "update_id": 1}),
            Some(ADMIN_SECRET)
        )
        .await,
        StatusCode::NOT_FOUND
    );
    // Both identifiers.
    assert_eq!(
        post(
            json!({"bot_id": "demo", "update_id": 1, "payload": update(1, 7)}),
            Some(ADMIN_SECRET)
        )
        .await,
        StatusCode::BAD_REQUEST
    );
    // Neither identifier.
    assert_eq!(
        post(json!({"bot_id": "demo"}), Some(ADMIN_SECRET)).await,
        StatusCode::BAD_REQUEST
    );
    // Payload without update_id.
    assert_eq!(
        post(
            json!({"bot_id": "demo", "payload": {"nope": true}}),
            Some(ADMIN_SECRET)
        )
        .await,
        StatusCode::BAD_REQUEST
    );
    // Bad token.
    assert_eq!(
        post(json!({"bot_id": "demo", "update_id": 1}), Some("wrong")).await,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn replay_surfaces_downstream_failure() {
    // Nothing listens on this port.
    let (_tmp, config) = test_config_full("http://127.0.0.1:1/webhook".to_owned(), 30, true);
    let built = app::build_router(config).await.unwrap();
    let (status, replayed) = read_json(
        built.router,
        admin_post(
            "/outbox/replay",
            Some(ADMIN_SECRET),
            json!({"bot_id": "demo", "payload": update(23, 7)}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(replayed["ok"], false);
    assert!(replayed["error"].is_string());
}

#[tokio::test]
async fn unauthenticated_non_json_body_is_401() {
    let (downstream, _rx) = stub_downstream().await;
    let (_tmp, config) = test_config(downstream, 30);
    let built = app::build_router(config).await.unwrap();

    let req = Request::builder()
        .method("POST")
        .uri("/webhooks/demo")
        .header("content-type", "application/json")
        .header("x-telegram-bot-api-secret-token", "wrong-secret")
        .body(Body::from("this is definitely not json"))
        .unwrap();
    let resp = built.router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn authenticated_invalid_json_is_400() {
    let (downstream, _rx) = stub_downstream().await;
    let (_tmp, config) = test_config(downstream, 30);
    let built = app::build_router(config).await.unwrap();

    let req = Request::builder()
        .method("POST")
        .uri("/webhooks/demo")
        .header("content-type", "application/json")
        .header("x-telegram-bot-api-secret-token", SECRET)
        .body(Body::from("not json at all"))
        .unwrap();
    let resp = built.router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn stored_payload_preserves_exact_bytes() {
    let (downstream, _rx) = stub_downstream().await;
    let (_tmp, config) = test_config(downstream, 30);
    let built = app::build_router(config).await.unwrap();

    let raw_payload =
        r#"{"z":1,"update_id":9007199254740999,"a":2,"message":{"chat":{"id":7},"text":"hi"}}"#;
    let req = Request::builder()
        .method("POST")
        .uri("/webhooks/demo")
        .header("content-type", "application/json")
        .header("x-telegram-bot-api-secret-token", SECRET)
        .body(Body::from(raw_payload))
        .unwrap();
    let resp = built.router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let stored = built
        .store
        .find_payload("demo", 9007199254740999)
        .await
        .unwrap()
        .expect("payload should be found in outbox");
    assert_eq!(stored, raw_payload);
}

#[tokio::test]
async fn duplicate_does_not_consume_rate_limit_quota() {
    let (downstream, mut rx) = stub_downstream().await;
    // Rate limit 1 per minute: update A is accepted, duplicate A is accepted without
    // consuming extra quota or being blocked, and new update B is rate-limited.
    let (_tmp, config) = test_config(downstream, 1);
    let built = app::build_router(config).await.unwrap();

    // 1. Initial update A is accepted.
    let resp_a1 = built
        .router
        .clone()
        .oneshot(authed("/webhooks/demo", update(10, 7)))
        .await
        .unwrap();
    assert_eq!(resp_a1.status(), StatusCode::OK);

    // 2. Duplicate update A is sent again: must be accepted as duplicate (status 200),
    // not rejected by rate limiting.
    let resp_a2 = built
        .router
        .clone()
        .oneshot(authed("/webhooks/demo", update(10, 7)))
        .await
        .unwrap();
    assert_eq!(resp_a2.status(), StatusCode::OK);

    // 3. New update B for same chat is rate-limited (quota of 1 was consumed by A1).
    let resp_b = built
        .router
        .clone()
        .oneshot(authed("/webhooks/demo", update(11, 7)))
        .await
        .unwrap();
    assert_eq!(resp_b.status(), StatusCode::OK);

    built.dispatchers[0].drain_once().await;
    // Only update A should have been delivered to downstream.
    let forwarded = rx.recv().await.expect("update 10 delivered");
    let body: serde_json::Value = serde_json::from_str(&forwarded).unwrap();
    assert_eq!(body["update_id"], 10);
    assert!(
        rx.try_recv().is_err(),
        "neither duplicate nor rate-limited update reaches downstream"
    );
}

#[tokio::test]
async fn duplicate_does_not_burn_remaining_quota() {
    let (downstream, mut rx) = stub_downstream().await;
    // Rate limit 2 per minute: update A (uses 1), duplicate A (uses 0), update B (uses 1), update C (rate-limited).
    let (_tmp, config) = test_config(downstream, 2);
    let built = app::build_router(config).await.unwrap();

    let resp_a1 = built
        .router
        .clone()
        .oneshot(authed("/webhooks/demo", update(30, 7)))
        .await
        .unwrap();
    assert_eq!(resp_a1.status(), StatusCode::OK);

    let resp_a2 = built
        .router
        .clone()
        .oneshot(authed("/webhooks/demo", update(30, 7)))
        .await
        .unwrap();
    assert_eq!(resp_a2.status(), StatusCode::OK);

    // If duplicate A burned quota, B would be rejected. With duplicate dedupe first, B is admitted.
    let resp_b = built
        .router
        .clone()
        .oneshot(authed("/webhooks/demo", update(31, 7)))
        .await
        .unwrap();
    assert_eq!(resp_b.status(), StatusCode::OK);

    // Third unique update is rejected by rate limit.
    let resp_c = built
        .router
        .clone()
        .oneshot(authed("/webhooks/demo", update(32, 7)))
        .await
        .unwrap();
    assert_eq!(resp_c.status(), StatusCode::OK);

    built.dispatchers[0].drain_once().await;
    let f1 = rx.recv().await.unwrap();
    let body1: serde_json::Value = serde_json::from_str(&f1).unwrap();
    assert_eq!(body1["update_id"], 30);
    let f2 = rx.recv().await.unwrap();
    let body2: serde_json::Value = serde_json::from_str(&f2).unwrap();
    assert_eq!(body2["update_id"], 31);
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn rate_limited_update_is_not_persisted() {
    let (downstream, _rx) = stub_downstream().await;
    let (_tmp, config) = test_config(downstream, 1);
    let built = app::build_router(config).await.unwrap();

    let resp1 = built
        .router
        .clone()
        .oneshot(authed("/webhooks/demo", update(100, 7)))
        .await
        .unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);
    assert!(
        built
            .store
            .find_payload("demo", 100)
            .await
            .unwrap()
            .is_some()
    );

    let resp2 = built
        .router
        .clone()
        .oneshot(authed("/webhooks/demo", update(101, 7)))
        .await
        .unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);

    let stored = built.store.find_payload("demo", 101).await.unwrap();
    assert_eq!(stored, None, "rate-limited update must not be persisted");
}

#[tokio::test]
async fn queue_cap_returns_503_when_max_pending_reached() {
    let (downstream, _rx) = stub_downstream().await;
    let (_tmp, config) = test_config_with_admin(downstream, 30, false, None, 1);
    let built = app::build_router(config).await.unwrap();

    let resp1 = built
        .router
        .clone()
        .oneshot(authed("/webhooks/demo", update(201, 7)))
        .await
        .unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);

    let resp2 = built
        .router
        .clone()
        .oneshot(authed("/webhooks/demo", update(202, 8)))
        .await
        .unwrap();
    assert_eq!(resp2.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn admin_replay_without_token_returns_401_before_body_parsing() {
    let (downstream, _rx) = stub_downstream().await;
    let (_tmp, config) = test_config_full(downstream, 30, true);
    let built = app::build_router(config).await.unwrap();

    let req = Request::builder()
        .method("POST")
        .uri("/outbox/replay")
        .header("content-type", "application/json")
        .body(Body::from("this is definitely not valid json"))
        .unwrap();
    let resp = built.router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn admin_on_separate_router_when_admin_listen_set() {
    let (downstream, _rx) = stub_downstream().await;
    let (_tmp, config) =
        test_config_with_admin(downstream, 30, true, Some("127.0.0.1:8089"), 10_000);
    let built = app::build_router(config).await.unwrap();

    assert!(built.admin_router.is_some());
    let admin_router = built.admin_router.unwrap();

    let resp_pub = built
        .router
        .oneshot(admin_get("/outbox", Some(ADMIN_SECRET)))
        .await
        .unwrap();
    assert_eq!(resp_pub.status(), StatusCode::NOT_FOUND);

    let resp_admin = admin_router
        .oneshot(admin_get("/outbox", Some(ADMIN_SECRET)))
        .await
        .unwrap();
    assert_eq!(resp_admin.status(), StatusCode::OK);
}
