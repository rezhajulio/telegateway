# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Config: `outbox.max_pending` bounds queued updates per bot (sheds with 503 when reached).
- Config: `[admin] listen` serves admin routes on a dedicated socket address, removing them from the public listener.
- Config: secrets (`bot_token`, `webhook_secret`, `admin.token`) are redacted as `[REDACTED]` in `Debug` output.
- Ingress: load shedding with 503 when concurrency limit is saturated.
- Ingress: rate limiting keys on chat ID or sender user ID when chat is omitted.
- Downstream: honor `Retry-After` response headers on 429 and 503 responses.
- Downstream: delivery ordering is per-bot sequential and best-effort (at-least-once; downstreams dedupe on `update_id`).
- Runtime: SIGHUP reloads TLS certificate and private key.
- Runtime: container image includes a `HEALTHCHECK` probing `/healthz`.
- Database: migration 002 adds a status `CHECK` constraint and an optimized claim index.
- Telemetry: outbox depth gauge reports 0 for idle bots.

### Changed

- Ingress: verifies bot existence and webhook secret in constant time before parsing the body.
- Ingress: stores and forwards original raw bytes received from Telegram without re-serialization.
- Ingress: checks duplicate updates before consuming token-bucket rate limit quota.
- Ingress: rate limiter in-memory state is pruned hourly.
- Downstream: 4xx responses (except 408, 425, 429) dead-letter immediately and do not trip the circuit breaker.
- Downstream: HTTP client disables redirect following and enforces a 5s connect timeout; timeouts produce status 504.
- Downstream: requeuing rows while the circuit breaker is open no longer increments attempt counters.
- Downstream: recovery from process crashes retains existing retry backoff intervals.
- Downstream: delivery error messages strip downstream and Telegram URLs to prevent credential leakage.
- Lifecycle: graceful shutdown lets dispatchers finish in-flight delivery before stopping.
- Lifecycle: unexpected dispatcher exit terminates the process with non-zero exit code for orchestrator restarts.
- Lifecycle: webhook auto-registration retries on failure with exponential backoff.
- Validation: enforced minimum 32 characters for `admin.token`.
- Validation: enforced `[A-Za-z0-9_-]{1,64}` for bot IDs and `[A-Za-z0-9_-]{1,256}` for webhook secrets.
- Validation: enforced HTTP/HTTPS schemes and valid hosts for downstream URLs and OTEL endpoint.
- Validation: enforced HTTPS scheme and host for `public_url`.
- Validation: enforced non-empty `bot_token` without whitespace, `/`, `?`, or `#`.
- Validation: enforced `poll_interval_ms >= 1`, `delivered_retention_days` in `1..=36500`, and `max_cooldown_secs >= cooldown_secs`.
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
