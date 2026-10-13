// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
//! Per-chat admission policy: token-bucket rate limiting keyed by bot
//! and chat.
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;

use governor::clock::DefaultClock;
use governor::state::keyed::DashMapStateStore;
use governor::{Quota, RateLimiter};

use crate::config::BotConfig;

type KeyedLimiter = RateLimiter<String, DashMapStateStore<String>, DefaultClock>;

/// Per-bot, per-chat admission control backed by token buckets.
///
/// Each bot gets its own limiter configured from `rate_limit_per_minute`;
/// buckets are keyed by chat so one abusive chat cannot starve the others.
#[derive(Clone)]
pub struct AdmissionPolicy {
    limiters: Arc<HashMap<String, KeyedLimiter>>,
}

impl AdmissionPolicy {
    /// Build the per-bot limiters from config. A quota of 0 is treated
    /// as 1; unknown bots are never admitted.
    pub fn new(bots: &[BotConfig]) -> Self {
        let mut limiters = HashMap::with_capacity(bots.len());
        for bot in bots {
            let per_minute = NonZeroU32::new(bot.rate_limit_per_minute.max(1))
                .expect("rate_limit_per_minute validated to be >= 1");
            limiters.insert(
                bot.id.clone(),
                RateLimiter::dashmap(Quota::per_minute(per_minute)),
            );
        }
        Self {
            limiters: Arc::new(limiters),
        }
    }

    /// Returns `true` when the update is admitted, `false` when the chat's
    /// bucket is empty. Unknown bots are never admitted.
    pub fn admit(&self, bot_id: &str, chat_key: Option<i64>) -> bool {
        let key = match chat_key {
            Some(chat) => format!("{bot_id}:{chat}"),
            None => format!("{bot_id}:*"),
        };
        match self.limiters.get(bot_id) {
            Some(limiter) => limiter.check_key(&key).is_ok(),
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bots() -> Vec<BotConfig> {
        vec![BotConfig {
            id: "demo".to_owned(),
            bot_token: None,
            downstream_url: "http://127.0.0.1:8081/webhook".to_owned(),
            webhook_secret: "s3cret".to_owned(),
            rate_limit_per_minute: 2,
        }]
    }

    #[test]
    fn admits_within_quota_then_rejects() {
        let policy = AdmissionPolicy::new(&bots());
        assert!(policy.admit("demo", Some(1)));
        assert!(policy.admit("demo", Some(1)));
        assert!(!policy.admit("demo", Some(1)));
    }

    #[test]
    fn buckets_are_isolated_per_chat() {
        let policy = AdmissionPolicy::new(&bots());
        assert!(policy.admit("demo", Some(1)));
        assert!(policy.admit("demo", Some(1)));
        assert!(!policy.admit("demo", Some(1)));
        // A different chat still has its own full bucket.
        assert!(policy.admit("demo", Some(2)));
    }

    #[test]
    fn unknown_bot_is_never_admitted() {
        let policy = AdmissionPolicy::new(&bots());
        assert!(!policy.admit("nope", Some(1)));
    }
}
