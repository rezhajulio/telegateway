# telegateway

[![CI](https://github.com/rezhajulio/telegateway/actions/workflows/ci.yml/badge.svg)](https://github.com/rezhajulio/telegateway/actions/workflows/ci.yml)
[![GitHub release](https://img.shields.io/github/v/release/rezhajulio/telegateway)](https://github.com/rezhajulio/telegateway/releases)
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](LICENSE)

🛰️ A reverse-proxy gateway for Telegram bots — webhook ingress, per-chat rate limiting, and structured observability in a single static binary.

`telegateway` sits between Telegram and your bots. Telegram delivers webhook updates to it; it validates the webhook secret, applies per-chat admission policy, forwards the update to the right internal bot, and emits structured logs for every decision. Your bots stay on the private network and never see the public internet.

## Why telegateway?

- **One ingress for every bot.** `POST /webhooks/:bot_id` routes to any number of downstream bots, each with its own secret and policy. A new bot is five lines of TOML.
- **Abuse protection before the expensive work.** Per-chat token buckets reject flooding chats before a single download starts. Policy rejections answer `200` so Telegram doesn't retry them.
- **Bots stay private.** The gateway is the only internet-facing component. Downstream bots listen on the private network, so a forged update can't bypass admission.
- **Observable by default.** JSON structured logs carry `bot_id`, `update_id`, kind, chat, downstream status, and latency on every forwarded update. `RUST_LOG=debug` when you need the details.
- **Small and boring.** One static binary, no database, no control plane. Around 6 MB RSS at idle.

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

3. Point Telegram at it with a webhook secret:

```sh
curl -s "https://api.telegram.org/bot<token>/setWebhook" \
  -d "url=https://gateway.example.com/webhooks/mediadownloadbot" \
  -d "secret_token=<the webhook_secret from config.toml>"
```

Telegram now delivers updates to the gateway, which forwards them to your bot's internal endpoint.

## Routes

| Method | Path                | Description                                 |
| ------ | ------------------- | ------------------------------------------- |
| `POST` | `/webhooks/:bot_id` | Telegram webhook ingress for bot `:bot_id`. |
| `GET`  | `/healthz`          | Liveness probe.                             |
| `GET`  | `/readyz`           | Readiness probe.                            |

### Webhook response contract

| Status | Meaning                                                        |
| ------ | -------------------------------------------------------------- |
| `200`  | Update accepted and forwarded, or rejected by admission policy. |
| `400`  | Malformed update (missing `update_id`).                        |
| `401`  | Wrong or missing webhook secret.                               |
| `404`  | Unknown bot id.                                                |
| `502`  | Downstream bot unreachable or returned an error (Telegram will retry). |

Forwarded requests carry `X-Telegateway-Bot` and `X-Telegateway-Update-Id` headers so the downstream bot can correlate.

## Configuration

`telegateway --config config.toml` (default: `./config.toml`):

```toml
listen = "127.0.0.1:8080"

[[bots]]
id = "mediadownloadbot"
downstream_url = "http://127.0.0.1:8081/webhook"
webhook_secret = "a-long-random-value"
rate_limit_per_minute = 30
```

| Key                          | Description                                                 |
| ---------------------------- | ----------------------------------------------------------- |
| `listen`                     | Socket address to bind (default `127.0.0.1:8080`).           |
| `bots[].id`                  | Public id used in the webhook route. Must be unique.         |
| `bots[].downstream_url`      | Internal bot endpoint receiving the forwarded update.       |
| `bots[].webhook_secret`      | Compared in constant time against Telegram's secret header. |
| `bots[].rate_limit_per_minute` | Per-chat token-bucket quota (default `30`).                |

Set `RUST_LOG` to control log verbosity, e.g. `RUST_LOG=telegateway=debug`.

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

## Roadmap

- Durable outbox: persist accepted updates and deliver asynchronously with retries and a dead-letter trail.
- Replay API: re-deliver a past update to a bot for debugging.
- OpenTelemetry export for traces and metrics.
- Circuit breaker per downstream bot.

## License

AGPL-3.0-or-later — see [LICENSE](LICENSE).

If you need a commercial license without AGPL obligations: contact@rezhajulio.id.
