// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
//! Compile-level check that `AppState` is public at `telegateway::state` and
//! satisfies the bounds axum needs to inject it into handlers.
use telegateway::state::AppState;

fn assert_handler_state<T: Clone + Send + Sync + 'static>() {}

#[test]
fn app_state_is_injectable_from_state_module() {
    assert_handler_state::<AppState>();
}
