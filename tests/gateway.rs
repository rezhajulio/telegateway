// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::json;
use std::net::SocketAddr;
use telegateway::{app, config::Config};
use tower::ServiceExt;

const SECRET: &str = "test-secret";

fn test_config(downstream_url: String, rate_limit_per_minute: u32) -> Config {
    let toml = format!(
        r#"
listen = "127.0.0.1:0"

[[bots]]
id = "demo"
downstream_url = "{downstream_url}"
webhook_secret = "{SECRET}"
rate_limit_per_minute = {rate_limit_per_minute}
"#
    );
    let config: Config = toml::from_str(&toml).unwrap();
    // Reuse the same validation the binary applies.
    telegateway::config::load_from_str(&toml).unwrap();
    config
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
    let router = app::build_router(test_config(downstream, 30));

    for path in ["/healthz", "/readyz"] {
        let resp = router
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
    let router = app::build_router(test_config(downstream, 30));

    let resp = router
        .oneshot(authed("/webhooks/nope", update(1, 7)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn bad_secret_is_401() {
    let (downstream, _rx) = stub_downstream().await;
    let router = app::build_router(test_config(downstream, 30));

    let req = Request::builder()
        .method("POST")
        .uri("/webhooks/demo")
        .header("content-type", "application/json")
        .header("x-telegram-bot-api-secret-token", "wrong")
        .body(Body::from(update(1, 7).to_string()))
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn forwards_update_to_downstream() {
    let (downstream, mut rx) = stub_downstream().await;
    let router = app::build_router(test_config(downstream, 30));

    let resp = router
        .oneshot(authed("/webhooks/demo", update(99, 7)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let forwarded = rx.recv().await.expect("downstream should be called");
    let body: serde_json::Value = serde_json::from_str(&forwarded).unwrap();
    assert_eq!(body["update_id"], 99);
}

#[tokio::test]
async fn rate_limited_updates_answer_200_without_forwarding() {
    let (downstream, mut rx) = stub_downstream().await;
    // Quota of 1 per minute: the second update from the same chat is rejected.
    let router = app::build_router(test_config(downstream, 1));

    let first = router
        .clone()
        .oneshot(authed("/webhooks/demo", update(1, 7)))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    assert!(rx.recv().await.is_some());

    let second = router
        .oneshot(authed("/webhooks/demo", update(2, 7)))
        .await
        .unwrap();
    // 200, not 429: Telegram must not retry a policy rejection.
    assert_eq!(second.status(), StatusCode::OK);
    assert!(
        rx.try_recv().is_err(),
        "rate-limited update must not reach downstream"
    );
}

#[tokio::test]
async fn downstream_outage_is_502() {
    // Nothing listens on this port.
    let config = test_config("http://127.0.0.1:1/webhook".to_owned(), 30);
    let router = app::build_router(config);

    let resp = router
        .oneshot(authed("/webhooks/demo", update(1, 7)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
}
