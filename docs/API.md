# telegateway API

How to talk to the gateway: webhook ingress, probes, and the admin API.
Base URL is wherever the gateway listens (default `http://127.0.0.1:8080`),
or `https://` when `[tls]` is configured.

## Webhook ingress

`POST /webhooks/{bot_id}`: Telegram calls this; you can call it too when
testing. Authenticate with the bot's webhook secret:

```sh
curl -X POST localhost:8080/webhooks/echo-python \
  -H 'content-type: application/json' \
  -H 'x-telegram-bot-api-secret-token: python-secret' \
  -d '{"update_id":1,"message":{"message_id":1,"chat":{"id":123},"text":"hello"}}'
```

| Status | Meaning |
| ------ | ------- |
| `200`  | Accepted into the outbox (or duplicate / rate-limited). |
| `400`  | Malformed update (missing `update_id`). |
| `401`  | Wrong or missing webhook secret. |
| `404`  | Unknown bot id. |
| `503`  | Outbox unavailable; Telegram retries. |

Accepted updates are delivered to the downstream bot asynchronously with
retries. The `200` means "durable", not "delivered". Forwarded requests
carry `X-Telegateway-Bot` and `X-Telegateway-Update-Id` headers. Delivery
is at-least-once: downstream bots dedupe on `update_id`.

## Probes

```sh
curl localhost:8080/healthz  # liveness: the process is running
curl localhost:8080/readyz   # readiness: also checks the outbox database
```

## Admin API

Guarded by the `[admin]` token as a bearer token. The routes are not
registered at all when `[admin]` is absent.

```toml
[admin]
token = "a-long-random-value"
```

### List outbox rows

`GET /outbox`: newest first.

```sh
curl -H "Authorization: Bearer <token>" \
  'localhost:8080/outbox?status=dead&bot_id=demo&limit=20'
```

| Param    | Values                                              | Default |
| -------- | --------------------------------------------------- | ------- |
| `status` | `pending`, `in_flight`, `delivered`, `dead`, `all`   | `dead`  |
| `bot_id` | bot id                                              | (all)   |
| `limit`  | 1–500                                               | `50`    |

```json
[
  {
    "id": 42,
    "bot_id": "demo",
    "update_id": 123,
    "status": "dead",
    "attempts": 25,
    "next_attempt_at": null,
    "last_error": "downstream answered 500 Internal Server Error",
    "created_at": 1791861980547
  }
]
```

### Replay an update

`POST /outbox/replay`: re-deliver a past update to a bot's downstream,
synchronously, for debugging. The downstream sees the original
`update_id`.

By stored update:

```sh
curl -X POST localhost:8080/outbox/replay \
  -H "Authorization: Bearer <token>" \
  -H 'content-type: application/json' \
  -d '{"bot_id":"demo","update_id":123}'
```

Or with an ad-hoc payload (validated as a Telegram update first):

```sh
curl -X POST localhost:8080/outbox/replay \
  -H "Authorization: Bearer <token>" \
  -H 'content-type: application/json' \
  -d '{"bot_id":"demo","payload":{"update_id":123,"message":{"message_id":1,"chat":{"id":7},"text":"hi"}}}'
```

Exactly one of `update_id` and `payload` is required.

| Status | Meaning |
| ------ | ------- |
| `200`  | Delivered: `{"ok":true,"bot_id":"demo","update_id":123,"downstream_status":200}` |
| `400`  | Bad request (identifier missing/duplicated, invalid envelope, unknown `status`). |
| `401`  | Bad admin token. |
| `404`  | Unknown bot, or no stored update for the id. |
| `502`  | Downstream failed: `{"ok":false,"bot_id":"demo","update_id":123,"error":"..."}` |

Only stored updates within retention can be replayed by id: delivered
rows are pruned after `outbox.delivered_retention_days` (default 7),
dead letters after 30 days.
