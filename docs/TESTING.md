# Testing telegateway

## Running the suite

```sh
cargo test --all-targets   # everything: unit + integration + doc tests
cargo test --test gateway  # one integration target
cargo test breaker         # filter by name substring
cargo clippy --all-targets -- -D warnings
cargo fmt --all -- --check
cargo llvm-cov --all-targets --summary-only   # line coverage (cargo install cargo-llvm-cov)
```

CI runs fmt, clippy with `-D warnings`, and the tests under
`cargo llvm-cov` on every push and pull request, failing below 75% line
coverage (currently about 80%; `src/main.rs` is not covered because no
test launches the binary). Keep all of it green before pushing.

## Layout

- **Unit tests** live next to the code in `mod tests` (`src/config.rs`,
  `src/policy.rs`, `src/telegram.rs`, `src/register.rs`,
  `src/outbox/breaker.rs`, `src/outbox/mod.rs`). They cover pure logic:
  envelope parsing, token-bucket behavior, config validation, breaker
  state transitions.
- **Integration tests** live in `tests/`:
  - `gateway.rs`: webhook ingress through the real router (via
    `tower::ServiceExt::oneshot`), with a stub downstream bot.
  - `outbox.rs`: dispatcher delivery, retries, backoff, dead letters,
    and breaker behavior against stub downstream servers.
  - `concurrency.rs`: bounded concurrent delivery across chats, in-order same-chat delivery, error isolation, and breaker handling.
  - `register.rs`: `setWebhook` against a stub Telegram API.
  - `otel.rs`: OTEL config parsing/validation, no-op init, init
    against a dead endpoint, `pending_counts`.
  - `tls.rs`: HTTPS serving with a self-signed certificate.

## Conventions

- **No real network.** Every external dependency is stubbed: downstream
  bots and the Telegram API are axum servers on ephemeral ports;
  `register_bot` takes the API base as a parameter so tests can point it
  at the stub.
- **One database per test.** Integration tests that touch the outbox
  create a unique temp directory via `tempfile::tempdir()`, holding the
  `TempDir` for the test's lifetime so tests run in parallel and clean up
  deterministically.
- **Observe, don't sleep.** Stubs report received requests over
  `tokio::sync::mpsc` channels; tests assert on what arrived instead of
  guessing at timing. Where timing is inherent (backoff, cooldowns),
  tests use short configured durations.
- **OTEL tests never need a collector.** `otel::init(None)` exercises the
  no-op path; init against `http://127.0.0.1:1` proves setup succeeds
  even when the collector is unreachable. Recording on any instrument
  must never panic.
- **Config changes need a test compile check.** `Config` is constructed
  literally in some tests; adding a field without updating them fails
  the build. That's intentional.

## Manual end-to-end checks

Some behaviors are verified by hand rather than in CI, because they need
real processes and timing:

- **Full pipeline**: `example/` has three echo bots (Python, Go, Rust)
  plus one gateway config. Run them, POST a signed update to
  `/webhooks/<bot>`, and confirm the bot echoes it.
- **Outage recovery**: stop the echo bot, send updates (they queue),
  restart the bot, and confirm all updates arrive exactly once and the
  breaker closes.
- **OTEL export**: point `[otel]` at a stub collector, send traffic,
  and confirm spans/metrics arrive and downstream requests carry
  `traceparent` headers.

If a manual check catches a regression twice, promote it to an
integration test.
