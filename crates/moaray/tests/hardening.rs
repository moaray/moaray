//! Phase 3 production-hardening integration tests: per-key + per-upstream rate
//! limiting (incl. MoA sharing the same upstream bucket as passthrough), and the
//! observability surface (passthrough/MoA bucketed latency + label cardinality).
//!
//! Real axum app in-process against a wiremock upstream.

use std::time::Duration;

use moaray::app::{build_router, ServerCtx};
use moaray::observe::init_metrics;
use moaray::registry;
use moaray::runtime::{AppState, Runtime, StatefulState};
use tower::ServiceExt;

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use serde_json::json;
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn app_from_yaml(yaml: &str) -> axum::Router {
    let config = moaray_config::load_yaml(yaml).expect("valid config");
    let request_timeout = Duration::from_millis(config.server.request_timeout_ms);
    let max_body_bytes = config.server.max_body_bytes;
    let moa_expose_metadata = config.server.moa_expose_metadata;
    let stateful = std::sync::Arc::new(StatefulState::from_config(&config));
    let providers = registry::build_providers(&config, &stateful);
    let orchestrator = registry::build_orchestrator(&config, &providers);
    let runtime = Runtime {
        config,
        providers,
        orchestrator,
    };
    build_router(ServerCtx {
        state: AppState::with_stateful(runtime, stateful),
        metrics: init_metrics(),
        request_timeout,
        max_body_bytes,
        moa_expose_metadata,
    })
}

fn set_keys() {
    std::env::set_var("MOARAY_HARDEN_INBOUND", "sk-inbound");
    std::env::set_var("MOARAY_HARDEN_UPSTREAM", "sk-upstream");
}

async fn mount_ok(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "x",
            "object": "chat.completion",
            "model": "real",
            "choices": [{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}
        })))
        .mount(server)
        .await;
}

async fn post(app: axum::Router, model: &str) -> StatusCode {
    let body = format!(r#"{{"model":"{model}","messages":[]}}"#);
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header(header::AUTHORIZATION, "Bearer sk-inbound")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .unwrap(),
    )
    .await
    .unwrap()
    .status()
}

/// P3-1: per-key inbound limit returns 429 once the bucket is drained.
#[tokio::test]
async fn per_key_rate_limit_returns_429() {
    set_keys();
    let server = MockServer::start().await;
    mount_ok(&server).await;
    let yaml = format!(
        r#"
auth:
  keys:
    - id: team-a
      key_env: MOARAY_HARDEN_INBOUND
      allow_models: [gpt]
      rate_limit: {{rps: 1, burst: 2}}
models:
  - name: gpt
    provider_type: openai-compat
    base_url: {uri}
    api_key_env: MOARAY_HARDEN_UPSTREAM
    upstream_id: up-gpt
"#,
        uri = server.uri()
    );
    // Build the router once; Router is Clone and shares the same StatefulState
    // (and thus the same per-key bucket) across oneshot calls.
    let app = app_from_yaml(&yaml);
    // burst 2 -> first two OK, third 429 rate_limited.
    assert_eq!(post(app.clone(), "gpt").await, StatusCode::OK);
    assert_eq!(post(app.clone(), "gpt").await, StatusCode::OK);
    assert_eq!(
        post(app.clone(), "gpt").await,
        StatusCode::TOO_MANY_REQUESTS
    );
}

/// P3-1: per-upstream limit returns 429 for passthrough once drained.
#[tokio::test]
async fn per_upstream_rate_limit_returns_429() {
    set_keys();
    let server = MockServer::start().await;
    mount_ok(&server).await;
    let yaml = format!(
        r#"
auth:
  keys:
    - id: team-a
      key_env: MOARAY_HARDEN_INBOUND
      allow_models: [gpt]
models:
  - name: gpt
    provider_type: openai-compat
    base_url: {uri}
    api_key_env: MOARAY_HARDEN_UPSTREAM
    upstream_id: up-shared
    rate_limit: {{rps: 1, burst: 1}}
"#,
        uri = server.uri()
    );
    let app = app_from_yaml(&yaml);
    assert_eq!(post(app.clone(), "gpt").await, StatusCode::OK);
    assert_eq!(
        post(app.clone(), "gpt").await,
        StatusCode::TOO_MANY_REQUESTS
    );
}

/// P3-1 acceptance (load-bearing): MoA fan-out is constrained by the SAME
/// per-upstream bucket as passthrough. A passthrough call drains the shared
/// upstream's single-token bucket; the MoA arm that resolves to that same
/// upstream is then rate-limited, so the recipe drops below quorum and MoA
/// surfaces 503 moa_quorum_failed (the arm's 429 is not enough successes).
#[tokio::test]
async fn moa_traffic_is_bounded_by_shared_upstream_bucket() {
    set_keys();
    let server = MockServer::start().await;
    mount_ok(&server).await;
    // Two proposers, but BOTH map to upstream_id `up-shared` which is also the
    // passthrough model `gpt`. The shared bucket has burst=1.
    let yaml = format!(
        r#"
auth:
  keys:
    - id: team-a
      key_env: MOARAY_HARDEN_INBOUND
      allow_models: [gpt, "moa/arm-e"]
models:
  - name: gpt
    provider_type: openai-compat
    base_url: {uri}
    api_key_env: MOARAY_HARDEN_UPSTREAM
    upstream_id: up-shared
    rate_limit: {{rps: 1, burst: 1}}
  - name: agg
    provider_type: openai-compat
    base_url: {uri}
    api_key_env: MOARAY_HARDEN_UPSTREAM
    upstream_id: up-agg
recipes:
  arm-e:
    proposers: [gpt, gpt]
    aggregator: agg
    strategy: concat-synthesize
    arm_timeout_ms: 5000
    quorum: 2
"#,
        uri = server.uri()
    );
    let app = app_from_yaml(&yaml);
    // Drain the shared bucket via a passthrough call.
    assert_eq!(post(app.clone(), "gpt").await, StatusCode::OK);
    // Now MoA fan-out hits the SAME drained bucket on both arms -> both arms are
    // rate-limited -> successes (0) < quorum (2) -> 503 moa_quorum_failed.
    let status = post(app.clone(), "moa/arm-e").await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "MoA must be constrained by the shared upstream bucket"
    );
}

/// P3-2 acceptance: a *sent* generation request that the upstream answers with a
/// 5xx is NOT retried, even with retry enabled — the upstream must see exactly
/// one request (no double-charge). wiremock `expect(1)` asserts the count on drop.
#[tokio::test]
async fn sent_generation_request_is_not_retried_on_upstream_5xx() {
    set_keys();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(500))
        .expect(1) // exactly one upstream hit — retry must NOT fire for a sent request
        .mount(&server)
        .await;
    let yaml = format!(
        r#"
server:
  retry: {{enabled: true, max_retries: 3, backoff_ms: 0}}
auth:
  keys:
    - id: team-a
      key_env: MOARAY_HARDEN_INBOUND
      allow_models: [gpt]
models:
  - name: gpt
    provider_type: openai-compat
    base_url: {uri}
    api_key_env: MOARAY_HARDEN_UPSTREAM
    upstream_id: up-gpt
"#,
        uri = server.uri()
    );
    let app = app_from_yaml(&yaml);
    let status = post(app, "gpt").await;
    // 500 from upstream maps to 502 upstream_error.
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    // expect(1) verified when `server` drops at end of test.
}

/// P3-2 acceptance: repeated upstream failures trip the per-upstream breaker;
/// once open, further requests fail fast with 503 circuit_open WITHOUT hitting
/// the upstream. With failure_threshold=2, the upstream is hit at most twice even
/// though we send three requests.
#[tokio::test]
async fn breaker_opens_and_fails_fast() {
    set_keys();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(500))
        .expect(2) // breaker opens after 2 failures; the 3rd never reaches upstream
        .mount(&server)
        .await;
    let yaml = format!(
        r#"
server:
  breaker: {{failure_threshold: 2, open_ms: 60000, half_open_successes: 1}}
auth:
  keys:
    - id: team-a
      key_env: MOARAY_HARDEN_INBOUND
      allow_models: [gpt]
models:
  - name: gpt
    provider_type: openai-compat
    base_url: {uri}
    api_key_env: MOARAY_HARDEN_UPSTREAM
    upstream_id: up-gpt
"#,
        uri = server.uri()
    );
    let app = app_from_yaml(&yaml);
    // Two failing requests trip the breaker (each 500 -> 502).
    assert_eq!(post(app.clone(), "gpt").await, StatusCode::BAD_GATEWAY);
    assert_eq!(post(app.clone(), "gpt").await, StatusCode::BAD_GATEWAY);
    // Third request fails fast: 503 circuit_open, upstream not contacted.
    assert_eq!(
        post(app.clone(), "gpt").await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    // expect(2) verified when `server` drops.
}

/// P3-4: /metrics exposes passthrough + MoA bucketed latency and per-model
/// counters, and NO high-cardinality / secret label (request-id, key, url).
#[tokio::test]
async fn metrics_have_buckets_and_no_high_cardinality_labels() {
    set_keys();
    let server = MockServer::start().await;
    // proposer + aggregator responses for a MoA call.
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(body_string_contains("Mixture-of-Agents"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "agg",
            "object": "chat.completion",
            "model": "real",
            "choices": [{"index":0,"message":{"role":"assistant","content":"FUSED"},"finish_reason":"stop"}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 5, "total_tokens": 10}
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "p",
            "object": "chat.completion",
            "model": "real",
            "choices": [{"index":0,"message":{"role":"assistant","content":"prop"},"finish_reason":"stop"}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 10, "total_tokens": 20}
        })))
        .mount(&server)
        .await;
    let request_id_marker = "harden-cardinality-probe-rid";
    let yaml = format!(
        r#"
auth:
  keys:
    - id: team-a
      key_env: MOARAY_HARDEN_INBOUND
      allow_models: [gpt, "moa/arm-e"]
models:
  - name: gpt
    provider_type: openai-compat
    base_url: {uri}
    api_key_env: MOARAY_HARDEN_UPSTREAM
    upstream_id: up-gpt
  - name: agg
    provider_type: openai-compat
    base_url: {uri}
    api_key_env: MOARAY_HARDEN_UPSTREAM
    upstream_id: up-agg
recipes:
  arm-e:
    proposers: [gpt]
    aggregator: agg
    strategy: concat-synthesize
    arm_timeout_ms: 5000
    quorum: 1
"#,
        uri = server.uri()
    );
    let app = app_from_yaml(&yaml);

    // One passthrough request (with an inbound request-id we can scan for) ...
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header(header::AUTHORIZATION, "Bearer sk-inbound")
                .header(header::CONTENT_TYPE, "application/json")
                .header("x-request-id", request_id_marker)
                .body(Body::from(r#"{"model":"gpt","messages":[]}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    // ... and one MoA request.
    assert_eq!(post(app.clone(), "moa/arm-e").await, StatusCode::OK);

    // Scrape /metrics.
    let m = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(m.into_body(), usize::MAX)
        .await
        .unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();

    // passthrough + MoA latency buckets present.
    assert!(
        text.contains("moaray_request_duration_seconds"),
        "missing request latency histogram"
    );
    assert!(
        text.contains("path=\"passthrough\""),
        "missing passthrough path label"
    );
    assert!(text.contains("path=\"moa\""), "missing moa path label");
    // per-model counters + MoA arm metric present.
    assert!(
        text.contains("moaray_requests_total"),
        "missing per-request counter"
    );
    assert!(text.contains("model=\"gpt\""), "missing per-model label");
    assert!(
        text.contains("moaray_moa_arm_total"),
        "missing MoA arm metric"
    );

    // Cardinality / secret discipline: NO request-id, key, url, or auth label.
    assert!(
        !text.contains(request_id_marker),
        "request-id leaked into a metric label"
    );
    assert!(
        !text.contains("request_id"),
        "request_id must never be a metric label"
    );
    assert!(
        !text.contains("sk-inbound") && !text.contains("sk-upstream"),
        "a secret leaked into metrics"
    );
    assert!(
        !text.contains(&server.uri()),
        "raw upstream URL leaked into a metric label"
    );
    assert!(
        !text.contains("api_key") && !text.contains("authorization"),
        "credential-ish label present in metrics"
    );
}
