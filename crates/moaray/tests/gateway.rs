//! End-to-end gateway tests: real axum app in-process against wiremock upstream.

use std::time::Duration;

use moaray::app::{build_router, ServerCtx};
use moaray::observe::init_metrics;
use moaray::registry;
use moaray::runtime::{AppState, Runtime, StatefulState};
use tower::ServiceExt; // oneshot

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn app_from_yaml(yaml: &str) -> axum::Router {
    let config = moaray_config::load_yaml(yaml).expect("valid config");
    let request_timeout = Duration::from_millis(config.server.request_timeout_ms);
    let max_body_bytes = config.server.max_body_bytes;
    let moa_expose_metadata = config.server.moa_expose_metadata;
    let stateful = std::sync::Arc::new(StatefulState::from_config(&config));
    let providers = registry::build_providers(&config, &stateful).expect("providers build");
    let orchestrator = registry::build_orchestrator(&config, &providers);
    let runtime = Runtime {
        config,
        providers,
        orchestrator,
    };
    let state = AppState::with_stateful(runtime, stateful);
    let ctx = ServerCtx {
        state,
        metrics: init_metrics(),
        request_timeout,
        max_body_bytes,
        moa_expose_metadata,
    };
    build_router(ctx)
}

fn cfg_yaml(base_url: &str, max_body: usize) -> String {
    format!(
        r#"
server:
  max_body_bytes: {max_body}
auth:
  keys:
    - id: team-a
      key_env: MOARAY_TEST_INBOUND
      allow_models: [gpt, hidden-other]
models:
  - name: gpt
    provider_type: openai-compat
    base_url: {base_url}
    api_key_env: MOARAY_TEST_UPSTREAM
  - name: hidden-other
    provider_type: openai-compat
    base_url: {base_url}
    api_key_env: MOARAY_TEST_UPSTREAM
  - name: not-allowed
    provider_type: openai-compat
    base_url: {base_url}
    api_key_env: MOARAY_TEST_UPSTREAM
"#
    )
}

fn set_keys() {
    std::env::set_var("MOARAY_TEST_INBOUND", "sk-inbound");
    std::env::set_var("MOARAY_TEST_UPSTREAM", "sk-upstream");
}

#[tokio::test]
async fn healthz_ok() {
    set_keys();
    let server = MockServer::start().await;
    let app = app_from_yaml(&cfg_yaml(&server.uri(), 1_000_000));
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp.headers().get("x-request-id").is_some());
}

#[tokio::test]
async fn passthrough_non_stream_forwards_body_and_status() {
    set_keys();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            br#"{"id":"x","choices":[{"message":{"content":"hi"}}]}"#.to_vec(),
            "application/json",
        ))
        .mount(&server)
        .await;
    let app = app_from_yaml(&cfg_yaml(&server.uri(), 1_000_000));
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header(header::AUTHORIZATION, "Bearer sk-inbound")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"model":"gpt","messages":[]}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp.headers().get("x-request-id").is_some());
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        v["choices"][0]["message"]["content"],
        serde_json::json!("hi")
    );
}

#[tokio::test]
async fn passthrough_stream_sse_end_to_end() {
    set_keys();
    let server = MockServer::start().await;
    let sse = "data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\n\
               data: {\"choices\":[{\"delta\":{\"content\":\"b\"}}]}\n\n\
               data: [DONE]\n\n";
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(sse.as_bytes().to_vec(), "text/event-stream"),
        )
        .mount(&server)
        .await;
    let app = app_from_yaml(&cfg_yaml(&server.uri(), 1_000_000));
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header(header::AUTHORIZATION, "Bearer sk-inbound")
                .body(Body::from(r#"{"model":"gpt","stream":true,"messages":[]}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "text/event-stream"
    );
    assert_eq!(
        resp.headers().get(header::CACHE_CONTROL).unwrap(),
        "no-cache"
    );
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(text.matches("delta").count() >= 2);
    assert!(text.contains("[DONE]"));
}

#[tokio::test]
async fn models_filtered_by_allowlist() {
    set_keys();
    let server = MockServer::start().await;
    let app = app_from_yaml(&cfg_yaml(&server.uri(), 1_000_000));
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/models")
                .header(header::AUTHORIZATION, "Bearer sk-inbound")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let ids: Vec<&str> = v["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["id"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&"gpt"));
    assert!(ids.contains(&"hidden-other"));
    assert!(!ids.contains(&"not-allowed"));
}

async fn post_model(app: axum::Router, token: Option<&str>, model: &str) -> StatusCode {
    let mut b = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions");
    if let Some(t) = token {
        b = b.header(header::AUTHORIZATION, format!("Bearer {t}"));
    }
    let req = b
        .body(Body::from(format!(
            r#"{{"model":"{model}","messages":[]}}"#
        )))
        .unwrap();
    app.oneshot(req).await.unwrap().status()
}

#[tokio::test]
async fn auth_and_error_code_matrix() {
    set_keys();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(b"{}".to_vec(), "application/json"))
        .mount(&server)
        .await;
    let yaml = cfg_yaml(&server.uri(), 1_000_000);

    assert_eq!(
        post_model(app_from_yaml(&yaml), None, "gpt").await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        post_model(app_from_yaml(&yaml), Some("nope"), "gpt").await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        post_model(app_from_yaml(&yaml), Some("sk-inbound"), "not-allowed").await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        post_model(app_from_yaml(&yaml), Some("sk-inbound"), "gpt").await,
        StatusCode::OK
    );
}

#[tokio::test]
async fn unknown_model_returns_404() {
    set_keys();
    let server = MockServer::start().await;
    let yaml = format!(
        r#"
auth:
  keys:
    - id: team-a
      key_env: MOARAY_TEST_INBOUND
      allow_models: [ghost]
models:
  - name: gpt
    provider_type: openai-compat
    base_url: {}
    api_key_env: MOARAY_TEST_UPSTREAM
"#,
        server.uri()
    );
    assert_eq!(
        post_model(app_from_yaml(&yaml), Some("sk-inbound"), "ghost").await,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn oversized_body_returns_413() {
    set_keys();
    let server = MockServer::start().await;
    let app = app_from_yaml(&cfg_yaml(&server.uri(), 64));
    let big = format!(
        r#"{{"model":"gpt","messages":[],"pad":"{}"}}"#,
        "x".repeat(500)
    );
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header(header::AUTHORIZATION, "Bearer sk-inbound")
                .body(Body::from(big))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn oversized_body_without_auth_returns_401_not_413() {
    // Auth must fail closed before the body is read: an oversized but
    // unauthenticated request is 401, not 413.
    set_keys();
    let server = MockServer::start().await;
    let app = app_from_yaml(&cfg_yaml(&server.uri(), 64));
    let big = format!(
        r#"{{"model":"gpt","messages":[],"pad":"{}"}}"#,
        "x".repeat(500)
    );
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .body(Body::from(big))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn metrics_endpoint_serves_prometheus_text() {
    set_keys();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(b"{}".to_vec(), "application/json"))
        .mount(&server)
        .await;
    let app = app_from_yaml(&cfg_yaml(&server.uri(), 1_000_000));
    let _ = post_model(app.clone(), Some("sk-inbound"), "gpt").await;
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(text.contains("moaray_requests_total"));
}
