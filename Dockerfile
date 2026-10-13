# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# Builder: compile the release binary. Dependencies are built in their own
# layer so rebuilds after source-only changes stay fast.
# ---------------------------------------------------------------------------
FROM rust:1-bookworm AS builder
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src \
    && printf 'fn main() {}\n' > src/main.rs \
    && : > src/lib.rs \
    && cargo build --release --locked \
    && rm -rf src

COPY src ./src
COPY migrations ./migrations
# COPY preserves host mtimes, which can predate the stub build's artifacts;
# touch so cargo rebuilds the real sources instead of reusing the stub.
RUN touch src/main.rs src/lib.rs \
    && cargo build --release --locked \
    && cp target/release/telegateway /usr/local/bin/telegateway

# ---------------------------------------------------------------------------
# Runtime: Debian slim. The binary statically bundles SQLite (sqlx
# `bundled` feature), so only glibc and CA certificates are needed at
# runtime. No compiler, no cargo registry, no build artifacts.
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --shell /usr/sbin/nologin telegateway \
    && mkdir -p /data \
    && chown telegateway:telegateway /data

COPY --from=builder /usr/local/bin/telegateway /usr/local/bin/telegateway

USER telegateway
WORKDIR /data
EXPOSE 8080

# Assumes default listen port 8080 and plain HTTP (override/disable for TLS).
HEALTHCHECK CMD curl -fsS http://127.0.0.1:8080/healthz || exit 1

ENTRYPOINT ["telegateway"]
CMD ["--config", "/data/config.toml"]
