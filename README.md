# telegateway

[![CI](https://github.com/rezhajulio/telegateway/actions/workflows/ci.yml/badge.svg)](https://github.com/rezhajulio/telegateway/actions/workflows/ci.yml)
[![GitHub release](https://img.shields.io/github/v/release/rezhajulio/telegateway)](https://github.com/rezhajulio/telegateway/releases)
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](LICENSE)

🛰️ A reverse-proxy gateway for Telegram bots — webhook ingress, per-chat rate limiting, and structured observability in a single static binary.

`telegateway` sits between Telegram and your bots. Telegram delivers webhook updates to it; it validates the webhook secret, applies per-chat admission policy, forwards the update to the right internal bot, and emits structured logs for every decision. Your bots stay on the private network and never see the public internet.

## Why telegateway?

- **One ingress for every bot.** `POST /webhooks/:bot_id` routes to any number of downstream bots, each with its own secret and policy. A new bot is five lines of TOML.
- **Accepted means kept.** Updates are persisted to a local SQLite outbox before Telegram gets its `200`, then delivered asynchronously with retries. A downed bot loses nothing.
- **Abuse protection before the expensive work.** Per-chat token buckets reject flooding chats before a single download starts. Policy rejections answer `200` so Telegram doesn't retry them.
- **Bots stay private.** The gateway is the only internet-facing component. Downstream bots listen on the private network, so a forged update can't bypass admission.
- **Observable by default.** JSON structured logs carry `bot_id`, `update_id`, kind, chat, downstream status, and latency on every forwarded update. `RUST_LOG=debug` when you need the details.
- **Small and boring.** One static binary with an embedded SQLite outbox, no external database, no control plane. Around 6 MB RSS at idle.

## Quick start

1. Copy the example config and fill in your bots:

```sh
cp config.example.toml config.toml
$EDITOR config.toml
```

2. Run it:

```sh
./telegateway --config config.toml
# {"level":"INFO","fields":{"addr":"127.0.0.1:8080","bots":1},"message":"telegateway listening"}
```

3. Let the gateway register the webhooks itself. Add to `config.toml`:

```toml
public_url = "https://gateway.example.com"
auto_register = true
```

and put each bot's token from [@BotFather](https://t.me/BotFather) in its
`[[bots]]` section as `bot_token`. On startup the gateway calls `setWebhook`
for every bot that has a token and logs the result:

```sh
./telegateway --config config.toml
# {"level":"INFO","fields":{"bot_id":"mediadownloadbot","url":"https://gateway.example.com/webhooks/mediadownloadbot"},"message":"webhook registered"}
```

Telegram now delivers updates to the gateway, which forwards them to your
bot's internal endpoint. `config.toml` is gitignored, so tokens stay on your
machine. Bots without a token are skipped.

## Routes

| Method | Path                | Description                                 |
| ------ | ------------------- | ------------------------------------------- |
| `POST` | `/webhooks/:bot_id` | Telegram webhook ingress for bot `:bot_id`. |
| `GET`  | `/healthz`          | Liveness probe.                             |
| `GET`  | `/readyz`           | Readiness probe (checks the outbox database). |
| `GET`  | `/outbox/dead`      | Dead-lettered updates (admin token only).   |

### Webhook response contract

| Status | Meaning                                                        |
| ------ | -------------------------------------------------------------- |
| `200`  | Update accepted into the outbox, duplicate, or rejected by admission policy. |
| `400`  | Malformed update (missing `update_id`).                        |
| `401`  | Wrong or missing webhook secret.                               |
| `404`  | Unknown bot id.                                                |
| `503`  | Outbox unavailable (Telegram will retry).                      |

Accepted updates are delivered to the downstream bot asynchronously with
retries; a downed bot no longer produces a `502`. Forwarded requests carry
`X-Telegateway-Bot` and `X-Telegateway-Update-Id` headers so the downstream
bot can correlate and deduplicate (`update_id` is unique per bot, and
delivery is at-least-once).

### Dead letters

Updates that exhaust their retries land in the dead-letter trail, listed
via the admin API. See [docs/API.md](docs/API.md) for the full API
reference: webhook ingress, probes, outbox listing, and replay.

## Configuration

`telegateway --config config.toml` (default: `./config.toml`):

```toml
listen = "127.0.0.1:8080"

[outbox]
path = "outbox.db"          # SQLite file holding queued updates
max_attempts = 25           # retries before dead-lettering

# [admin]
# token = "a-long-random-value"  # guards GET /outbox/dead

[[bots]]
id = "mediadownloadbot"
downstream_url = "http://127.0.0.1:8081/webhook"
webhook_secret = "a-long-random-value"
rate_limit_per_minute = 30
```

| Key                          | Description                                                 |
| ---------------------------- | ----------------------------------------------------------- |
| `listen`                     | Socket address to bind (default `127.0.0.1:8080`).           |
| `outbox.path`                | SQLite file for the durable outbox (default `outbox.db`).   |
| `outbox.max_attempts`         | Delivery retries before dead-lettering (default `25`).      |
| `outbox.poll_interval_ms`    | Retry sweep interval (default `1000`).                      |
| `outbox.delivered_retention_days` | Prune delivered rows after N days (default `7`; dead letters after 30). |
| `circuit_breaker.failure_threshold` | Consecutive failures that trip the breaker (default `5`). |
| `circuit_breaker.cooldown_secs` | Pause before a recovery probe (default `60`). |
| `circuit_breaker.max_cooldown_secs` | Cap for the doubling cooldown (default `600`). |
| `admin.token`                | Bearer token for `/outbox/*`. Route absent when unset.       |
| `otel.endpoint`              | OTLP/HTTP collector base URL for traces and metrics. Export off when unset. |
| `otel.service_name`          | `service.name` resource attribute (default `telegateway`).  |
| `bots[].id`                  | Public id used in the webhook route. Must be unique.         |
| `bots[].downstream_url`      | Internal bot endpoint receiving the forwarded update.       |
| `bots[].webhook_secret`      | Compared in constant time against Telegram's secret header. |
| `bots[].rate_limit_per_minute` | Per-chat token-bucket quota (default `30`).                |

Set `RUST_LOG` to control log verbosity, e.g. `RUST_LOG=telegateway=debug`.

### OpenTelemetry

Point the gateway at an OTLP/HTTP collector to export traces and metrics:

```toml
[otel]
endpoint = "http://localhost:4318"
# service_name = "telegateway"
```

Spans cover webhook ingress, outbox deliveries, breaker probes, replays,
and Telegram registrations, with `bot_id` and `update_id` as attributes.
Metrics include per-bot counters (accepted, delivered, failed,
dead-lettered, replayed), delivery latency, pending outbox depth, and
breaker state. Outbound requests carry W3C `traceparent` headers so your
bots can continue the trace. Without `[otel]`, nothing leaves the process
and every instrument is a no-op. A dead collector only drops telemetry —
it never breaks the gateway.

### TLS

If no reverse proxy sits in front, telegateway can terminate TLS itself:

```toml
[tls]
cert_path = "/etc/telegateway/fullchain.pem"
key_path = "/etc/telegateway/privkey.pem"
```

With `[tls]` present, `listen` serves HTTPS instead of plain HTTP. Without
it, the gateway speaks plain HTTP and expects TLS to be terminated upstream.
Certificate issuance and renewal stay your job (certbot, Caddy, your CA);
restart the process to pick up renewed certificates.

## Install from source

You need the [Rust toolchain](https://rustup.rs) (1.85+):

```sh
git clone https://github.com/rezhajulio/telegateway.git
cd telegateway
cargo build --release
./target/release/telegateway --config config.toml
```

## How it works

```
Telegram ──POST /webhooks/:bot_id──▶ telegateway ──POST──▶ your bot (private net)
                                    │
                                    ├─ validate webhook secret (constant-time)
                                    ├─ extract update envelope (update_id, kind, chat)
                                    ├─ per-chat token bucket (rejections answer 200)
                                    ├─ forward with 25s timeout + gateway headers
                                    └─ JSON log: bot_id, update_id, kind, chat, status, latency
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for the design: component breakdown, design patterns, security model, and failure handling.

## Examples

`example/` has three echo bots (Python, Go, Rust) and one gateway config
routing to all three. See [example/README.md](example/README.md).

## Roadmap

- Durable outbox: persist accepted updates and deliver asynchronously with retries and a dead-letter trail.
- Replay API: re-deliver a past update to a bot for debugging.
- OpenTelemetry export for traces and metrics.
- Circuit breaker per downstream bot.

## License

AGPL-3.0-or-later — see [LICENSE](LICENSE).

If you need a commercial license without AGPL obligations: contact@rezhajulio.id.
