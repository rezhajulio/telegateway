# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.3] - 2026-11-09

### Changed

- Upgrade `sqlx` and `sqlx-sqlite` dependencies from 0.8 to 0.9.0.
- Bump MSRV (`rust-version`) to 1.94.
- Adapt `Store::list` dynamic SQL query construction to `sqlx::QueryBuilder` for `SqlSafeStr` compatibility.
- Remove obsolete `RUSTSEC-2023-0071` ignore from `.cargo/audit.toml` as `rsa` dependency was dropped.

### Fixed

- Collapse nested `if` statements in outbox dispatcher requeue logic.

## [0.1.2] - 2026-11-09

### Added

- `bots[].allowed_updates` config: the list of update types passed to
  Telegram `setWebhook` during auto-registration. When unset, Telegram
  keeps its previous setting; pass `[]` for Telegram defaults. Invalid
  update types are rejected at config validation.

### Fixed

- Ignore `RUSTSEC-2026-0459` (libsqlite3-sys SQLite WAL page size
  mismatch) in `.cargo/audit.toml`; the database files live on a trusted
  host volume and the fix requires the sqlx 0.9 upgrade.

## [0.1.1] - 2026-11-07

### Fixed

- Dependency updates resolving 7 of 8 flagged CVEs: rustls 0.23.45 ->
  0.23.46 (4 GHSAs), rustls-webpki 0.103.15 -> 0.103.16, webpki-roots
  1.0.9 -> 1.0.11 (CVE-2026-111943), OpenTelemetry family 0.28 -> 0.32
  (sdk 0.32.1 fixes CVE-2026-48504; tracing-opentelemetry 0.29 -> 0.33).
  Same rustls updates in `example/echo-rust`. The remaining advisory,
  RUSTSEC-2023-0071 (`rsa`), has no fixed release, is not compiled into
  any telegateway target, and is ignored in `.cargo/audit.toml`.

## [0.1.0] - 2026-10-14

First release.

### Added

- Bounded per-chat concurrent delivery: `outbox.delivery_concurrency`
  (default 1, 1..=64). Different chats are delivered in parallel; one chat
  is always delivered in order, and a failed row blocks that chat's later
  rows until it is retried. Rows carry a `chat_key` column.
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
- Bounded queues: `outbox.max_pending` sheds with `503` per bot;
  `outbox.max_dead_letters` caps dead letters per bot (plus 30-day age limit).
- Separate admin listener (`[admin] listen`) to keep admin routes off the
  public socket; admin token must be at least 32 characters.
- Load shedding (`503`) when the concurrency limit is saturated; request
  timeouts answer `504`.
- Webhook authentication happens before the body is parsed; the original
  bytes Telegram sent are stored and forwarded unchanged. Rate limiting
  keys on chat id, falling back to the sending user; duplicates do not
  consume quota.
- Downstream handling: 4xx (except 408/425/429) dead-letter immediately
  without tripping the breaker; `Retry-After` honored on 429/503;
  redirects are not followed; 5s connect timeout.
- Secrets (`bot_token`, `webhook_secret`, admin token) are redacted in
  `Debug`, and URLs are stripped from delivery and registration errors.
- Strict config validation (id/secret charsets, http/https URLs, https
  `public_url`, positive intervals, retention bounds).
- Supervised workers and graceful shutdown: dispatchers finish the
  in-flight delivery; a dead worker exits the process non-zero.
- SIGHUP reloads the TLS certificate and key.
- Webhook auto-registration retries with backoff.
- Container image `HEALTHCHECK` on `/healthz`.
- Delivery is at-least-once, per-bot sequential, best-effort ordered;
  downstream bots must dedupe on `update_id`.
- Release automation: `cargo publish` to crates.io and release binaries
  (Linux x86_64 and aarch64, macOS x86_64 and ARM64) on `v*` tags. See `docs/RELEASING.md`.

### Fixed

- `.gitignore` only ignores the root `config.toml`, so
  `example/config.toml` is committed.

[Unreleased]: https://github.com/rezhajulio/telegateway/compare/v0.1.3...HEAD
[0.1.3]: https://github.com/rezhajulio/telegateway/releases/tag/v0.1.3
[0.1.2]: https://github.com/rezhajulio/telegateway/releases/tag/v0.1.2
[0.1.1]: https://github.com/rezhajulio/telegateway/releases/tag/v0.1.1
[0.1.0]: https://github.com/rezhajulio/telegateway/releases/tag/v0.1.0
