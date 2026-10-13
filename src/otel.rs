// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
//! OpenTelemetry export: traces and metrics shipped via OTLP/HTTP to a
//! collector. Entirely optional — without `[otel]` in the config, the
//! tracing layer is absent and every metric instrument is a no-op.
//! Telemetry must never break the gateway: exporters are batched and
//! async, and a dead collector only drops data.
use std::sync::{
    OnceLock,
    atomic::{AtomicBool, Ordering},
};

use anyhow::Context;
use opentelemetry::{
    KeyValue, global,
    metrics::{Counter, Gauge, Histogram, Meter, MeterProvider as _},
    propagation::Injector,
    trace::TracerProvider as _,
};
use opentelemetry_otlp::{MetricExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::{
    Resource,
    metrics::{PeriodicReader, SdkMeterProvider},
    propagation::TraceContextPropagator,
    trace::SdkTracerProvider,
};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

use crate::config::OtelConfig;

static METRICS: OnceLock<Metrics> = OnceLock::new();
static ENABLED: AtomicBool = AtomicBool::new(false);

/// All metric instruments. Created from the SDK meter when `[otel]` is
/// configured, otherwise from the global no-op meter.
#[derive(Debug, Clone)]
pub struct Metrics {
    pub updates_accepted: Counter<u64>,
    pub updates_delivered: Counter<u64>,
    pub updates_failed: Counter<u64>,
    pub updates_dead_lettered: Counter<u64>,
    pub updates_replayed: Counter<u64>,
    pub delivery_latency_ms: Histogram<f64>,
    pub outbox_pending: Gauge<u64>,
    pub breaker_state: Gauge<i64>,
}

impl Metrics {
    fn from_meter(meter: Meter) -> Self {
        Self {
            updates_accepted: meter
                .u64_counter("telegateway.updates.accepted")
                .with_description("Updates accepted into the outbox")
                .build(),
            updates_delivered: meter
                .u64_counter("telegateway.updates.delivered")
                .with_description("Updates delivered downstream")
                .build(),
            updates_failed: meter
                .u64_counter("telegateway.updates.failed")
                .with_description("Delivery attempts that failed")
                .build(),
            updates_dead_lettered: meter
                .u64_counter("telegateway.updates.dead_lettered")
                .with_description("Updates that exhausted retries")
                .build(),
            updates_replayed: meter
                .u64_counter("telegateway.updates.replayed")
                .with_description("Admin replay deliveries")
                .build(),
            delivery_latency_ms: meter
                .f64_histogram("telegateway.delivery.latency")
                .with_unit("ms")
                .with_description("Downstream delivery latency")
                .build(),
            outbox_pending: meter
                .u64_gauge("telegateway.outbox.pending")
                .with_description("Pending outbox rows per bot")
                .build(),
            breaker_state: meter
                .i64_gauge("telegateway.breaker.state")
                .with_description("Circuit breaker state per bot: 0 closed, 1 open, 2 half-open")
                .build(),
        }
    }

    fn noop() -> Self {
        Self::from_meter(global::meter("telegateway"))
    }
}

/// The process-wide instruments. Always safe to record on; no-ops when
/// `[otel]` is absent.
pub fn metrics() -> &'static Metrics {
    METRICS.get_or_init(Metrics::noop)
}

pub fn bot_attrs(bot_id: &str) -> [KeyValue; 1] {
    [KeyValue::new("bot_id", bot_id.to_owned())]
}

type OtelTracingLayer = tracing_opentelemetry::OpenTelemetryLayer<
    tracing_subscriber::Registry,
    opentelemetry_sdk::trace::Tracer,
>;

/// Initialized telemetry: an optional tracing layer plus providers to shut
/// down on exit.
pub struct Otel {
    layer: Option<OtelTracingLayer>,
    tracer_provider: Option<SdkTracerProvider>,
    meter_provider: Option<SdkMeterProvider>,
}

impl Otel {
    pub fn take_tracing_layer(&mut self) -> Option<OtelTracingLayer> {
        self.layer.take()
    }

    /// Flush pending telemetry. Best-effort; never fails startup/shutdown.
    pub fn shutdown(&self) {
        if let Some(p) = &self.tracer_provider {
            let _ = p.shutdown();
        }
        if let Some(p) = &self.meter_provider {
            let _ = p.shutdown();
        }
    }
}

/// Set up OTLP export when configured. Always populates the global
/// instruments (real or no-op) so call sites never branch.
pub fn init(config: Option<&OtelConfig>) -> anyhow::Result<Otel> {
    let Some(cfg) = config else {
        METRICS.get_or_init(Metrics::noop);
        return Ok(Otel {
            layer: None,
            tracer_provider: None,
            meter_provider: None,
        });
    };

    let resource = Resource::builder()
        .with_service_name(cfg.service_name.clone())
        .with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")))
        .build();

    let span_exporter = SpanExporter::builder()
        .with_http()
        .with_endpoint(format!("{}/v1/traces", cfg.endpoint.trim_end_matches('/')))
        .build()
        .context("failed to build OTLP span exporter")?;
    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .with_resource(resource.clone())
        .build();
    let layer =
        tracing_opentelemetry::OpenTelemetryLayer::new(tracer_provider.tracer("telegateway"));

    let metric_exporter = MetricExporter::builder()
        .with_http()
        .with_endpoint(format!("{}/v1/metrics", cfg.endpoint.trim_end_matches('/')))
        .build()
        .context("failed to build OTLP metric exporter")?;
    let reader = PeriodicReader::builder(metric_exporter).build();
    let meter_provider = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(resource)
        .build();

    global::set_meter_provider(meter_provider.clone());
    global::set_text_map_propagator(TraceContextPropagator::new());
    METRICS
        .set(Metrics::from_meter(meter_provider.meter("telegateway")))
        .ok();
    ENABLED.store(true, Ordering::SeqCst);

    Ok(Otel {
        layer: Some(layer),
        tracer_provider: Some(tracer_provider),
        meter_provider: Some(meter_provider),
    })
}

struct HeaderInjector<'a>(&'a mut HeaderMap);

impl Injector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let (Ok(name), Ok(val)) = (key.parse::<HeaderName>(), value.parse::<HeaderValue>()) {
            self.0.insert(name, val);
        }
    }
}

/// Inject W3C trace context into outbound headers. No-op unless `[otel]`
/// is configured.
pub fn inject_trace_context(headers: &mut HeaderMap) {
    if !ENABLED.load(Ordering::SeqCst) {
        return;
    }
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(
            &tracing::Span::current().context(),
            &mut HeaderInjector(headers),
        );
    });
}
