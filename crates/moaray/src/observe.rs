//! Observability: tracing init, Prometheus metrics handle, and request metrics.
//!
//! Secret hygiene (no-secret-logging rule): nothing here ever receives a token
//! or upstream key. Metric labels are restricted to `model` and `status_class`
//! (2xx/4xx/5xx) to avoid high cardinality and to keep secrets out of labels.

use std::sync::OnceLock;

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

static PROM: OnceLock<PrometheusHandle> = OnceLock::new();

/// Install the Prometheus recorder once and return its render handle.
pub fn init_metrics() -> PrometheusHandle {
    PROM.get_or_init(|| {
        PrometheusBuilder::new()
            .install_recorder()
            .expect("prometheus recorder installs once")
    })
    .clone()
}

/// Render the current metrics in Prometheus text format.
pub fn render_metrics(handle: &PrometheusHandle) -> String {
    handle.render()
}

/// Bucket an HTTP status into a low-cardinality class label.
pub fn status_class(status: u16) -> &'static str {
    match status {
        100..=199 => "1xx",
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        _ => "5xx",
    }
}

/// Which gateway path served a request. Used as a fixed, low-cardinality
/// histogram label so passthrough vs MoA latency is bucketed separately
/// (plan P3-4) without ever leaking a high-cardinality value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestPath {
    Passthrough,
    Moa,
    /// A request rejected *before* routing (auth/allowlist/limit/body checks), so
    /// no upstream path was selected. A fixed label so these protective rejections
    /// are still visible on `/metrics` (plan P3-1: an inbound limit that never
    /// shows up in metrics is unobservable).
    PreRouting,
}

impl RequestPath {
    /// Stable, low-cardinality string for the `path` metric label.
    pub fn as_str(&self) -> &'static str {
        match self {
            RequestPath::Passthrough => "passthrough",
            RequestPath::Moa => "moa",
            RequestPath::PreRouting => "pre_routing",
        }
    }
}

/// Record one request's outcome.
///
/// The latency histogram is bucketed by `path` (passthrough vs MoA) and `model`,
/// both low-cardinality. No request-id / key / URL ever becomes a label
/// (no-secret-logging + cardinality discipline; asserted by a metrics test).
pub fn record_request(path: RequestPath, model: &str, status: u16, latency_secs: f64) {
    let class = status_class(status);
    metrics::counter!(
        "moaray_requests_total",
        "path" => path.as_str(),
        "model" => model.to_string(),
        "status_class" => class
    )
    .increment(1);
    if status >= 400 {
        metrics::counter!(
            "moaray_errors_total",
            "path" => path.as_str(),
            "model" => model.to_string(),
            "status_class" => class
        )
        .increment(1);
    }
    metrics::histogram!(
        "moaray_request_duration_seconds",
        "path" => path.as_str(),
        "model" => model.to_string()
    )
    .record(latency_secs);
}

/// Record a request rejected before routing (auth/allowlist/per-key limit/body
/// limit/bad request). These are counted under the fixed `path="pre_routing"`
/// label so the inbound protections (notably per-key 429) are visible on
/// `/metrics` — a protective rejection that never increments a counter is
/// unobservable (plan P3-1).
///
/// The caller-supplied model name is deliberately NOT a label here: at this
/// stage the model is unvalidated client input (unbounded cardinality) and may
/// not even exist, so it is collapsed to a fixed `model="_pre_routing"` sentinel.
/// `status_class` keeps the same 2xx/4xx/5xx bucketing as routed requests.
pub fn record_rejection(status: u16) {
    let class = status_class(status);
    let path = RequestPath::PreRouting.as_str();
    metrics::counter!(
        "moaray_requests_total",
        "path" => path,
        "model" => "_pre_routing",
        "status_class" => class
    )
    .increment(1);
    if status >= 400 {
        metrics::counter!(
            "moaray_errors_total",
            "path" => path,
            "model" => "_pre_routing",
            "status_class" => class
        )
        .increment(1);
    }
}

/// Record one MoA arm's outcome.
///
/// Labels are restricted to `model`, `upstream_id`, and `status_class` — all
/// low-cardinality, non-secret values (no request-id, key, or URL ever becomes a
/// label; see no-secret-logging rule).
pub fn record_moa_arm(model: &str, upstream_id: &str, status_class: &str, latency_secs: f64) {
    metrics::counter!(
        "moaray_moa_arm_total",
        "model" => model.to_string(),
        "upstream_id" => upstream_id.to_string(),
        "status_class" => status_class.to_string()
    )
    .increment(1);
    metrics::histogram!(
        "moaray_moa_arm_duration_seconds",
        "model" => model.to_string(),
        "upstream_id" => upstream_id.to_string()
    )
    .record(latency_secs);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_class_buckets() {
        assert_eq!(status_class(200), "2xx");
        assert_eq!(status_class(404), "4xx");
        assert_eq!(status_class(502), "5xx");
    }
}
