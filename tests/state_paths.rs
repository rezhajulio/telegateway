// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
//! Compile-level check that `AppState` is reachable at both its new home and
//! the legacy `app` path, and that both name the same type.
use telegateway::{app, state};

fn same_type<T>(_: Option<T>, _: Option<T>) {}

#[test]
fn app_state_paths_name_the_same_type() {
    let legacy: Option<app::AppState> = None;
    let current: Option<state::AppState> = None;
    same_type(legacy, current);
}
