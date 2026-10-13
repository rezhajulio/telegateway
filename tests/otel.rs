// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
//! OpenTelemetry configuration, initialization, and the instruments the
//! pipeline records on.
use std::sync::atomic::{AtomicU64, Ordering};

use telegateway::{config, otel, outbox::Store};

fn temp_db() -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir()
        .join(format!(
            "telegateway-otel-test-{}-{n}.db",
            std::process::id()
        ))
        .to_str()
        .unwrap()
        .to_owned()
}

fn base_toml(extra: &str) -> String {
    format!(
        r#"
listen = "127.0.0.1:0"

{extra}
[[bots]]
id = "demo"
downstream_url = "http://127.0.0.1:9/"
webhook_secret = "secret"
"#
    )
}

#[test]
fn otel_config_parses_with_defaults() {
    let cfg = config::load_from_str(&base_toml("[otel]\nendpoint = \"http://localhost:4318\"\n"))
        .unwrap();
    let otel = cfg.otel.expect("otel should be configured");
    assert_eq!(otel.endpoint, "http://localhost:4318");
    assert_eq!(otel.service_name, "telegateway");
}

#[test]
fn otel_config_accepts_custom_service_name() {
    let cfg = config::load_from_str(&base_toml(
        "[otel]\nendpoint = \"http://localhost:4318\"\nservice_name = \"gw-prod\"\n",
    ))
    .unwrap();
    assert_eq!(cfg.otel.unwrap().service_name, "gw-prod");
}

#[test]
fn otel_absent_by_default() {
    let cfg = config::load_from_str(&base_toml("")).unwrap();
    assert!(cfg.otel.is_none());
}

#[test]
fn otel_endpoint_must_be_a_url() {
    let err = config::load_from_str(&base_toml("[otel]\nendpoint = \"not a url\"\n"))
        .expect_err("invalid endpoint should fail validation");
    assert!(err.to_string().contains("otel.endpoint"), "{err}");
}

#[test]
fn otel_init_without_config_is_noop() {
    let mut otel = otel::init(None).unwrap();
    assert!(otel.take_tracing_layer().is_none());
    // Recording on no-op instruments must never panic.
    let m = otel::metrics();
    let attrs = otel::bot_attrs("demo");
    m.updates_accepted.add(1, &attrs);
    m.updates_delivered.add(1, &attrs);
    m.updates_failed.add(1, &attrs);
    m.updates_dead_lettered.add(1, &attrs);
    m.updates_replayed.add(1, &attrs);
    m.delivery_latency_ms.record(12.5, &attrs);
    m.outbox_pending.record(3, &attrs);
    m.breaker_state.record(0, &attrs);
}

#[test]
fn otel_init_with_dead_endpoint_still_succeeds() {
    // Telemetry must never break the gateway: exporter setup is lazy, so
    // an unreachable collector is fine at startup.
    let cfg = config::OtelConfig {
        endpoint: "http://127.0.0.1:1".to_owned(),
        service_name: "telegateway".to_owned(),
    };
    let mut otel = otel::init(Some(&cfg)).unwrap();
    assert!(otel.take_tracing_layer().is_some());
    otel.shutdown();
}

#[tokio::test]
async fn pending_counts_groups_by_bot() {
    let store = Store::open(&temp_db()).await.unwrap();
    store.insert("bot-a", 1, "{}").await.unwrap();
    store.insert("bot-a", 2, "{}").await.unwrap();
    store.insert("bot-b", 3, "{}").await.unwrap();
    store.insert("bot-b", 3, "{}").await.unwrap(); // duplicate, ignored

    let mut counts = store.pending_counts().await.unwrap();
    counts.sort();
    assert_eq!(
        counts,
        vec![("bot-a".to_owned(), 2), ("bot-b".to_owned(), 1)]
    );
}

#[tokio::test]
async fn pending_counts_empty_when_nothing_pending() {
    let store = Store::open(&temp_db()).await.unwrap();
    assert!(store.pending_counts().await.unwrap().is_empty());
}
