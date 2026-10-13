# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-10-13

First release.

### Added

- Webhook ingress per bot (`POST /webhooks/{bot_id}`) with constant-time
  webhook-secret validation.
- Per-chat token-bucket rate limiting; rejections answer `200` so abusive
  traffic is not amplified by Telegram retries.
- Forwarding to private downstream bots with gateway headers
  (`x-telegateway-bot`, `x-telegateway-update-id`) and W3C `traceparent`
  propagation.
- Optional TLS termination via `[tls]` (Rustls); plain HTTP otherwise.
- Telegram webhook auto-registration (`auto_register` + `public_url` +
  per-bot `bot_token`); failures are logged, never fatal.
- Durable outbox: accepted updates are persisted to SQLite (WAL) before
  the gateway answers `200`, then delivered asynchronously with
  exponential backoff, 25 max attempts, and dead-lettering. `503` on
  persistence failure so Telegram retries.
- Outbox listing (`GET /outbox`) and replay API (`POST /outbox/replay`),
  guarded by the `[admin]` bearer token. See `docs/API.md`.
- Per-bot circuit breaker: fast-fail while open, half-open probe after
  the cooldown, doubling cooldown capped at `max_cooldown_secs`.
- OpenTelemetry export via `[otel]`: traces and per-bot metrics over
  OTLP/HTTP. Absent `[otel]`, every instrument is a no-op.
- `example/` with echo bots in Python, Go, and Rust behind one gateway
  config.
- JSON structured logs; `/healthz` and `/readyz` probes
  (`/readyz` checks the database).
- Release automation: `cargo publish` to crates.io and release binaries
  (Linux x86_64, macOS ARM64) on `v*` tags. See `docs/RELEASING.md`.

### Fixed

- `.gitignore` only ignores the root `config.toml`, so
  `example/config.toml` is committed.

[Unreleased]: https://github.com/rezhajulio/telegateway/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/rezhajulio/telegateway/releases/tag/v0.1.0
