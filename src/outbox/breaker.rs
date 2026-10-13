// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
//! Circuit breaker per downstream bot. When a bot fails repeatedly, the
//! breaker opens and the dispatcher stops attempting delivery entirely
//! (fast-fail) instead of burning retry attempts against a dead endpoint.
//! After a cooldown, one probe delivery goes out: success closes the
//! breaker, failure reopens it with a doubled cooldown.
use std::time::{Duration, Instant};

/// What the dispatcher may do right now.
#[derive(Debug, PartialEq, Eq)]
pub enum Decision {
    /// Deliver normally.
    Allow,
    /// The breaker is open; try again after this delay.
    Deny { retry_after: Duration },
    /// The cooldown elapsed; one trial delivery is allowed.
    Probe,
}

enum State {
    Closed {
        consecutive_failures: u32,
    },
    Open {
        opened_at: Instant,
        cooldown: Duration,
    },
}

pub struct CircuitBreaker {
    bot_id: String,
    failure_threshold: u32,
    cooldown: Duration,
    max_cooldown: Duration,
    state: State,
}

impl CircuitBreaker {
    pub fn new(
        bot_id: String,
        failure_threshold: u32,
        cooldown: Duration,
        max_cooldown: Duration,
    ) -> Self {
        Self {
            bot_id,
            failure_threshold: failure_threshold.max(1),
            cooldown,
            max_cooldown: max_cooldown.max(cooldown),
            state: State::Closed {
                consecutive_failures: 0,
            },
        }
    }

    pub fn decide(&self, now: Instant) -> Decision {
        match &self.state {
            State::Closed { .. } => Decision::Allow,
            State::Open {
                opened_at,
                cooldown,
            } => {
                if now >= *opened_at + *cooldown {
                    Decision::Probe
                } else {
                    Decision::Deny {
                        retry_after: (*opened_at + *cooldown).saturating_duration_since(now),
                    }
                }
            }
        }
    }

    /// A delivery succeeded: close the breaker and reset the counter.
    pub fn on_success(&mut self) {
        if matches!(self.state, State::Open { .. }) {
            tracing::info!(bot_id = %self.bot_id, "circuit breaker closed");
        }
        self.state = State::Closed {
            consecutive_failures: 0,
        };
    }

    /// A delivery failed. Trips the breaker at the threshold; a failed
    /// probe reopens it with a doubled cooldown.
    pub fn on_failure(&mut self, now: Instant) {
        match &self.state {
            State::Closed {
                consecutive_failures,
            } => {
                let next = consecutive_failures + 1;
                if next >= self.failure_threshold {
                    tracing::warn!(
                        bot_id = %self.bot_id,
                        consecutive_failures = next,
                        cooldown_secs = self.cooldown.as_secs(),
                        "circuit breaker opened"
                    );
                    self.state = State::Open {
                        opened_at: now,
                        cooldown: self.cooldown,
                    };
                } else {
                    self.state = State::Closed {
                        consecutive_failures: next,
                    };
                }
            }
            State::Open { cooldown, .. } => {
                let next_cooldown = (*cooldown * 2).min(self.max_cooldown);
                tracing::warn!(
                    bot_id = %self.bot_id,
                    cooldown_secs = next_cooldown.as_secs(),
                    "circuit breaker probe failed, reopening"
                );
                self.state = State::Open {
                    opened_at: now,
                    cooldown: next_cooldown,
                };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn breaker() -> CircuitBreaker {
        CircuitBreaker::new(
            "demo".to_owned(),
            3,
            Duration::from_millis(100),
            Duration::from_millis(400),
        )
    }

    #[test]
    fn starts_closed() {
        let b = breaker();
        assert_eq!(b.decide(Instant::now()), Decision::Allow);
    }

    #[test]
    fn opens_after_threshold_consecutive_failures() {
        let mut b = breaker();
        let now = Instant::now();
        b.on_failure(now);
        b.on_failure(now);
        assert_eq!(b.decide(now), Decision::Allow);
        b.on_failure(now);
        match b.decide(now) {
            Decision::Deny { retry_after } => {
                assert!(retry_after <= Duration::from_millis(100));
            }
            d => panic!("expected Deny, got {d:?}"),
        }
    }

    #[test]
    fn success_resets_the_counter() {
        let mut b = breaker();
        let now = Instant::now();
        b.on_failure(now);
        b.on_failure(now);
        b.on_success();
        b.on_failure(now);
        b.on_failure(now);
        assert_eq!(b.decide(now), Decision::Allow);
    }

    #[test]
    fn probe_after_cooldown_then_reopen_with_doubled_cooldown() {
        let mut b = breaker();
        let t0 = Instant::now();
        for _ in 0..3 {
            b.on_failure(t0);
        }
        assert_eq!(b.decide(t0 + Duration::from_millis(150)), Decision::Probe);

        // Failed probe: reopen, cooldown doubles to 200ms.
        b.on_failure(t0 + Duration::from_millis(150));
        match b.decide(t0 + Duration::from_millis(300)) {
            Decision::Deny { .. } => {}
            d => panic!("expected Deny, got {d:?}"),
        }
        assert_eq!(b.decide(t0 + Duration::from_millis(400)), Decision::Probe);
    }

    #[test]
    fn cooldown_doubling_caps_at_max() {
        let mut b = breaker();
        let mut t = Instant::now();
        for _ in 0..3 {
            b.on_failure(t);
        }
        // Fail probes repeatedly; cooldown must not exceed 400ms.
        for _ in 0..5 {
            t += Duration::from_millis(500);
            assert_eq!(b.decide(t), Decision::Probe);
            b.on_failure(t);
            match b.decide(t) {
                Decision::Deny { retry_after } => {
                    assert!(
                        retry_after <= Duration::from_millis(400),
                        "capped: {retry_after:?}"
                    );
                }
                d => panic!("expected Deny, got {d:?}"),
            }
        }
    }

    #[test]
    fn successful_probe_closes() {
        let mut b = breaker();
        let t0 = Instant::now();
        for _ in 0..3 {
            b.on_failure(t0);
        }
        assert_eq!(b.decide(t0 + Duration::from_millis(150)), Decision::Probe);
        b.on_success();
        assert_eq!(b.decide(t0 + Duration::from_millis(150)), Decision::Allow);
    }
}
