// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
use axum::{Json, Router, extract::Path, routing::post};
use serde_json::Value;
use telegateway::{
    config::{BotConfig, Config},
    register::{RegisterError, register_all, register_bot},
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
    let client = reqwest::Client::new();
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
    let client = reqwest::Client::new();
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

    register_all(&config, &base).await;

    // Only the bot with a token was registered.
    let (token, body) = rx.recv().await.unwrap();
    assert_eq!(token, "TOKEN");
    assert_eq!(body["url"], "https://gw.example.com/webhooks/echo-python");
    assert!(rx.try_recv().is_err());
}
