# telegateway examples

Three echo bots in three languages behind one telegateway, routed by bot id:

| Bot          | Language | Deps      | Listens on        | Gateway route                |
| ------------ | -------- | --------- | ----------------- | ---------------------------- |
| `echo-python` | Python   | stdlib only | `127.0.0.1:8081` | `POST /webhooks/echo-python` |
| `echo-go`     | Go       | stdlib only | `127.0.0.1:8082` | `POST /webhooks/echo-go`     |
| `echo-rust`   | Rust     | axum      | `127.0.0.1:8083` | `POST /webhooks/echo-rust`   |

Each bot echoes every message back via `sendMessage`. Set `TELEGRAM_BOT_TOKEN`
to send for real; without it the bots log what they would send (dry-run).

## Run

Start the bots (one terminal each):

```sh
python3 example/echo-python/bot.py
```

```sh
cd example/echo-go && go run .
```

```sh
cd example/echo-rust && cargo run
```

Then the gateway, from the repo root:

```sh
cargo run -- --config example/config.toml
```

## Try it

Simulate a Telegram update through the gateway (no Telegram needed):

```sh
curl -X POST localhost:8080/webhooks/echo-python \
  -H 'content-type: application/json' \
  -H 'x-telegram-bot-api-secret-token: python-secret' \
  -d '{"update_id":1,"message":{"message_id":1,"chat":{"id":123},"text":"hello"}}'
```

The gateway answers `200` and the bot logs the dry-run echo. Swap
`echo-python`/`python-secret` for `echo-go`/`go-secret` or
`echo-rust`/`rust-secret` to hit the other bots. A wrong secret gets `401`,
an unknown bot id gets `404`.

## With real Telegram

Expose the gateway publicly (it must reach Telegram's servers), then register
one webhook per bot with its matching secret:

```sh
curl -s "https://api.telegram.org/bot<token>/setWebhook" \
  -d "url=https://gateway.example.com/webhooks/echo-python" \
  -d "secret_token=python-secret"
```

Message the bot and it echoes back.
