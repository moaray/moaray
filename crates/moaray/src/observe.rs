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

/// Record one request's outcome.
pub fn record_request(model: &str, status: u16, latency_secs: f64) {
    let class = status_class(status);
    metrics::counter!(
        "moaray_requests_total",
        "model" => model.to_string(),
        "status_class" => class
    )
    .increment(1);
    if status >= 400 {
        metrics::counter!(
            "moaray_errors_total",
            "model" => model.to_string(),
            "status_class" => class
        )
        .increment(1);
    }
    metrics::histogram!(
        "moaray_request_duration_seconds",
        "model" => model.to_string()
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
