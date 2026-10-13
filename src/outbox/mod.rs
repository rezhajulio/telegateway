// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
//! Durable outbox: accepted updates are persisted to SQLite before the
//! gateway answers Telegram, then delivered asynchronously with retries.
//! Delivery is at-least-once, per-bot sequential, and ordering is best-effort
//! (retries reorder). Downstream bots dedupe on `update_id`.
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub mod breaker;
pub mod dispatch;
mod store;

pub use breaker::{CircuitBreaker, Decision};
pub use dispatch::{DeliveryError, Dispatcher, deliver_payload};
pub use store::{OUTBOX_STATUSES, OutboxEntry, OutboxRow, Store};

/// Exponential backoff with jitter for a 1-based attempt number:
/// 5s, 10s, 20s, ... capped at 1h, plus or minus 20%.
pub fn backoff_delay(attempt: u32) -> Duration {
    let exp = 5u64.saturating_mul(2u64.saturating_pow(attempt.saturating_sub(1).min(12)));
    let capped = exp.min(3600);
    let jitter = 0.8 + fastrand::f64() * 0.4;
    Duration::from_secs_f64(capped as f64 * jitter)
}

pub(crate) fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_exponentially() {
        let d1 = backoff_delay(1).as_secs_f64();
        let d2 = backoff_delay(2).as_secs_f64();
        let d3 = backoff_delay(3).as_secs_f64();
        assert!((4.0..=6.0).contains(&d1), "attempt 1: {d1}");
        assert!((8.0..=12.0).contains(&d2), "attempt 2: {d2}");
        assert!((16.0..=24.0).contains(&d3), "attempt 3: {d3}");
    }

    #[test]
    fn backoff_caps_at_one_hour() {
        let d = backoff_delay(100).as_secs_f64();
        assert!((2880.0..=4320.0).contains(&d), "capped: {d}");
    }

    proptest::proptest! {
        #[test]
        fn backoff_delay_proptest(attempt: u32) {
            let d = backoff_delay(attempt);
            let secs = d.as_secs_f64();
            let base_exp = 5u64
                .saturating_mul(2u64.saturating_pow(attempt.saturating_sub(1).min(12)))
                .min(3600) as f64;
            let min_expected = base_exp * 0.8;
            let max_expected = base_exp * 1.2;
            proptest::prop_assert!(secs >= min_expected - 1e-6, "secs {secs} < min {min_expected}");
            proptest::prop_assert!(secs <= max_expected + 1e-6, "secs {secs} > max {max_expected}");
            proptest::prop_assert!(secs <= 3600.0 * 1.2 + 1e-6, "exceeds 3600 * 1.2");
        }
    }
}
