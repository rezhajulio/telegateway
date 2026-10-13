// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
//! Webhook self-registration against a stub Telegram API.
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::{Json, Router, extract::Path, http::StatusCode, routing::post};
use serde_json::Value;
use telegateway::{
    config::{BotConfig, Config},
    register::{
        RegisterError, default_client, register_all_with_delay, register_bot,
        register_bot_with_backoff,
    },
};
use tokio::sync::mpsc;

/// Stub Telegram Bot API. Records `(token, body)` of each `setWebhook` call
/// and answers ok true/false depending on the flag.
async fn stub_telegram(ok: bool) -> (String, mpsc::UnboundedReceiver<(String, Value)>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let router = Router::new().route(
        "/bot{token}/setWebhook",
        post(
            move |Path(token): Path<String>, Json(body): Json<Value>| async move {
                let _ = tx.send((token, body));
                Json(serde_json::json!({
                    "ok": ok,
                    "result": true,
                    "description": if ok { "Webhook was set" } else { "Bad Request: bad webhook" },
                }))
            },
        ),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (base, rx)
}

fn test_bot() -> BotConfig {
    BotConfig {
        id: "echo-python".to_owned(),
        bot_token: Some("TOKEN".to_owned()),
        downstream_url: "http://127.0.0.1:1/webhook".to_owned(),
        webhook_secret: "s3cret".to_owned(),
        rate_limit_per_minute: 30,
    }
}

#[tokio::test]
async fn sends_set_webhook_with_url_and_secret() {
    let (base, mut rx) = stub_telegram(true).await;
    let client = default_client();
    register_bot(&client, &base, &test_bot(), "https://gw.example.com/")
        .await
        .unwrap();

    let (token, body) = rx.recv().await.unwrap();
    assert_eq!(token, "TOKEN");
    assert_eq!(body["url"], "https://gw.example.com/webhooks/echo-python");
    assert_eq!(body["secret_token"], "s3cret");
}

#[tokio::test]
async fn surfaces_telegram_refusal() {
    let (base, _rx) = stub_telegram(false).await;
    let client = default_client();
    let err = register_bot(&client, &base, &test_bot(), "https://gw.example.com")
        .await
        .unwrap_err();
    assert!(matches!(err, RegisterError::Refused(_)));
}

#[tokio::test]
async fn register_all_skips_bots_without_token() {
    let (base, mut rx) = stub_telegram(true).await;
    let no_token = BotConfig {
        bot_token: None,
        ..test_bot()
    };
    let config = Config {
        listen: "127.0.0.1:0".to_owned(),
        public_url: Some("https://gw.example.com/".to_owned()),
        auto_register: true,
        bots: vec![test_bot(), no_token],
        tls: None,
        outbox: Default::default(),
        circuit_breaker: Default::default(),
        admin: None,
        otel: None,
    };
    register_all_with_delay(&config, &base, Duration::from_millis(1)).await;

    // Only the bot with a token was registered.
    let (token, body) = rx.recv().await.unwrap();
    assert_eq!(token, "TOKEN");
    assert_eq!(body["url"], "https://gw.example.com/webhooks/echo-python");
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn error_display_does_not_leak_bot_token() {
    // Point at a closed port
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let closed_port = listener.local_addr().unwrap().port();
    drop(listener);

    let base = format!("http://127.0.0.1:{closed_port}");
    let client = default_client();
    let bot = BotConfig {
        bot_token: Some("SECRET_BOT_TOKEN_12345".to_owned()),
        ..test_bot()
    };

    let err = register_bot(&client, &base, &bot, "https://gw.example.com")
        .await
        .unwrap_err();

    let display = format!("{err}");
    let debug = format!("{err:?}");
    assert!(
        !display.contains("SECRET_BOT_TOKEN_12345"),
        "token leaked in Display: {display}"
    );
    assert!(
        !debug.contains("SECRET_BOT_TOKEN_12345"),
        "token leaked in Debug: {debug}"
    );
}

#[tokio::test]
async fn retries_transient_failures_then_succeeds() {
    let call_count = Arc::new(AtomicUsize::new(0));
    let count_clone = call_count.clone();

    let router = Router::new().route(
        "/bot{token}/setWebhook",
        post(move || {
            let count = count_clone.clone();
            async move {
                let c = count.fetch_add(1, Ordering::SeqCst);
                if c == 0 {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({
                            "ok": false,
                            "description": "Internal Server Error"
                        })),
                    )
                } else {
                    (
                        StatusCode::OK,
                        Json(serde_json::json!({
                            "ok": true,
                            "result": true
                        })),
                    )
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

    let client = default_client();
    let res = register_bot_with_backoff(
        &client,
        &base,
        &test_bot(),
        "https://gw.example.com",
        Duration::from_millis(5),
    )
    .await;

    assert!(res.is_ok());
    assert_eq!(call_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn does_not_retry_refusal() {
    let call_count = Arc::new(AtomicUsize::new(0));
    let count_clone = call_count.clone();

    let router = Router::new().route(
        "/bot{token}/setWebhook",
        post(move || {
            let count = count_clone.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                (
                    StatusCode::OK,
                    Json(serde_json::json!({
                        "ok": false,
                        "description": "Unauthorized: invalid token"
                    })),
                )
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

    let client = default_client();
    let res = register_bot_with_backoff(
        &client,
        &base,
        &test_bot(),
        "https://gw.example.com",
        Duration::from_millis(5),
    )
    .await;

    assert!(matches!(res, Err(RegisterError::Refused(_))));
    assert_eq!(call_count.load(Ordering::SeqCst), 1);
}
