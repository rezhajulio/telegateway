// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
/// Liveness probe: the process is running.
pub async fn healthz() -> &'static str {
    "ok"
}

/// Readiness probe: the gateway is accepting traffic.
pub async fn readyz() -> &'static str {
    "ok"
}
