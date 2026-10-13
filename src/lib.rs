// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
//! Reverse-proxy gateway for Telegram bots.
//!
//! Telegram delivers webhook updates to the gateway; the gateway validates
//! the webhook secret, applies per-chat rate limiting, persists the update
//! to a durable outbox, and forwards it to the right internal bot with
//! retries, per-bot circuit breakers, and optional OpenTelemetry export.
pub mod app;
pub mod config;
pub mod otel;
pub mod outbox;
pub mod policy;
pub mod register;
pub mod routes;
pub mod telegram;
