// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
//! Shared handler state. Lives apart from `app` and `routes` so both can
//! depend on it without depending on each other.
use std::{collections::HashMap, sync::Arc};

use tokio::sync::Notify;

use crate::{config::BotConfig, outbox::Store, policy::AdmissionPolicy};

/// Shared state injected into every handler.
#[derive(Clone)]
pub struct AppState {
    /// Bot configs by id, for routing and forwarding.
    pub bots: Arc<Vec<BotConfig>>,
    /// Per-chat rate limiters.
    pub policy: AdmissionPolicy,
    /// Outbound HTTP client for downstream delivery.
    pub http: reqwest::Client,
    /// The durable outbox.
    pub store: Store,
    /// Wake signals for each bot's dispatcher, keyed by bot id.
    pub notifies: Arc<HashMap<String, Arc<Notify>>>,
    /// Bearer token for the admin routes; `None` means they are not
    /// registered.
    pub admin_token: Option<String>,
    /// Maximum allowed pending updates before rejecting with 503.
    pub max_pending: u64,
}

impl AppState {
    /// Look up a bot's config by id.
    pub fn bot(&self, id: &str) -> Option<&BotConfig> {
        self.bots.iter().find(|b| b.id == id)
    }
}
