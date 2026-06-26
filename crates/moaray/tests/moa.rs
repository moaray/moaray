//! End-to-end MoA orchestration tests: a real axum app in-process against
//! wiremock upstreams, exercising fan-out -> aggregate -> single completion.

use moaray::app::{build_router, ServerCtx};
use moaray::observe::init_metrics;
use moaray::registry;
use moaray::runtime::{AppState, Runtime, StatefulState};
use tower::ServiceExt;

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use serde_json::{json, Value};
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn app_from_yaml(yaml: &str) -> axum::Router {
    let config = moaray_config::load_yaml(yaml).expect("valid config");
    let stateful = std::sync::Arc::new(StatefulState::from_config(&config));
    let providers = registry::build_providers(&config, &stateful).expect("providers build");
    let orchestrator = registry::build_orchestrator(&config, &providers);
    let runtime = Runtime {
        config,
        providers,
        orchestrator,
    };
    build_router(ServerCtx {
        state: AppState::with_stateful(runtime, stateful),
        metrics: init_metrics(),
    })
}

fn set_keys() {
    std::env::set_var("MOARAY_MOA_INBOUND", "sk-inbound");
    std::env::set_var("MOARAY_MOA_UPSTREAM", "sk-upstream");
}

/// Config with a 3-proposer concat-synthesize recipe (quorum 2) over a single
/// mock upstream, plus an optional `moa_expose_metadata` toggle.
fn cfg_yaml(base_url: &str, strategy: &str, expose: bool) -> String {
    format!(
        r#"
server:
  moa_expose_metadata: {expose}
auth:
  keys:
    - id: team-a
      key_env: MOARAY_MOA_INBOUND
      allow_models: ["moa/arm-e", "moa/judge"]
models:
  - name: a
    provider_type: openai-compat
    base_url: {base_url}
    api_key_env: MOARAY_MOA_UPSTREAM
    upstream_id: up-a
  - name: b
    provider_type: openai-compat
    base_url: {base_url}
    api_key_env: MOARAY_MOA_UPSTREAM
    upstream_id: up-b
  - name: c
    provider_type: openai-compat
    base_url: {base_url}
    api_key_env: MOARAY_MOA_UPSTREAM
    upstream_id: up-c
  - name: agg
    provider_type: openai-compat
    base_url: {base_url}
    api_key_env: MOARAY_MOA_UPSTREAM
    upstream_id: up-agg
recipes:
  arm-e:
    proposers: [a, b, c]
    aggregator: agg
    strategy: {strategy}
    arm_timeout_ms: 5000
    quorum: 2
"#
    )
}

/// Mount a default proposer response (10 tokens each) and a distinct aggregator
/// response (5 tokens) keyed by a system-prompt marker only the aggregator gets.
async fn mount_upstreams(server: &MockServer) {
    // Aggregator call: its request body contains the fixed synthesizer/judge
    // system prompt marker "Mixture-of-Agents". Matched first (more specific).
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(body_string_contains("Mixture-of-Agents"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "agg-1",
            "object": "chat.completion",
            "model": "real-agg",
            "choices": [{"index":0,"message":{"role":"assistant","content":"FUSED ANSWER"},"finish_reason":"stop"}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 5, "total_tokens": 10}
        })))
        .mount(server)
        .await;
    // Proposer calls: anything else.
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "prop",
            "object": "chat.completion",
            "model": "real-prop",
            "choices": [{"index":0,"message":{"role":"assistant","content":"proposer answer"},"finish_reason":"stop"}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 10, "total_tokens": 20}
        })))
        .mount(server)
        .await;
}

async fn post_moa(app: axum::Router, model: &str, stream: bool) -> (StatusCode, Value) {
    let body =
        json!({"model": model, "messages": [{"role":"user","content":"q"}], "stream": stream});
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header(header::AUTHORIZATION, "Bearer sk-inbound")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, v)
}

/// P2-3 + Phase-2 acceptance #2: concat-synthesize returns ONE completion and
/// usage = proposers (3x20) + aggregator (10) summed.
#[tokio::test]
async fn concat_synthesize_returns_single_completion_with_summed_usage() {
    set_keys();
    let server = MockServer::start().await;
    mount_upstreams(&server).await;
    let app = app_from_yaml(&cfg_yaml(&server.uri(), "concat-synthesize", false));

    let (status, v) = post_moa(app, "moa/arm-e", false).await;
    assert_eq!(status, StatusCode::OK);
    // single completion, fused content
    assert_eq!(v["choices"][0]["message"]["content"], json!("FUSED ANSWER"));
    // response model reflects the requested moa model
    assert_eq!(v["model"], json!("moa/arm-e"));
    // usage summed: 3 proposers * 20 total + aggregator 10 = 70
    assert_eq!(v["usage"]["total_tokens"], json!(70));
    assert_eq!(v["usage"]["prompt_tokens"], json!(35)); // 3*10 + 5
                                                        // extension field OFF by default
    assert!(
        v.get("moaray").is_none(),
        "extension field leaked while off"
    );
}

/// Phase-2 acceptance #3 (DESIGN §6 hard metric): 1 proposer fails but quorum
/// still met -> MoA returns the fused answer.
#[tokio::test]
async fn one_arm_fails_quorum_still_met_returns_fused_answer() {
    set_keys();
    let server = MockServer::start().await;
    // aggregator first
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(body_string_contains("Mixture-of-Agents"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "choices": [{"index":0,"message":{"role":"assistant","content":"FUSED"},"finish_reason":"stop"}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}
        })))
        .mount(&server)
        .await;
    // one proposer (model "a") fails with 500; others succeed. Match on the
    // retargeted model field in the body.
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(body_string_contains("\"model\":\"a\""))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "choices": [{"index":0,"message":{"role":"assistant","content":"ok"}}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 10, "total_tokens": 20}
        })))
        .mount(&server)
        .await;

    let app = app_from_yaml(&cfg_yaml(&server.uri(), "concat-synthesize", false));
    let (status, v) = post_moa(app, "moa/arm-e", false).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["choices"][0]["message"]["content"], json!("FUSED"));
}

/// P2-4: quorum-judge integration returns a single completion.
#[tokio::test]
async fn quorum_judge_returns_single_completion() {
    set_keys();
    let server = MockServer::start().await;
    mount_upstreams(&server).await;
    let app = app_from_yaml(&cfg_yaml(&server.uri(), "quorum-judge", false));
    let (status, v) = post_moa(app, "moa/arm-e", false).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["choices"][0]["message"]["content"], json!("FUSED ANSWER"));
    assert!(v["choices"].as_array().unwrap().len() == 1);
}

/// Phase-2 acceptance #4: extension field appears once the toggle is on, with
/// per-arm metadata.
#[tokio::test]
async fn extension_field_present_when_toggled_on() {
    set_keys();
    let server = MockServer::start().await;
    mount_upstreams(&server).await;
    let app = app_from_yaml(&cfg_yaml(&server.uri(), "concat-synthesize", true));
    let (status, v) = post_moa(app, "moa/arm-e", false).await;
    assert_eq!(status, StatusCode::OK);
    let ext = v
        .get("moaray")
        .expect("extension field present when toggled on");
    let arms = ext["arms"].as_array().expect("arms array");
    assert_eq!(arms.len(), 3);
    // per-arm metadata fields present and non-secret
    for a in arms {
        assert!(a.get("model").is_some());
        assert!(a.get("upstream_id").is_some());
        assert!(a.get("status").is_some());
        assert!(a.get("latency_ms").is_some());
    }
    assert!(ext["aggregator"].get("upstream_id").is_some());
}

/// Phase-2 acceptance #5: `model: moa/*` + `stream:true` -> 400
/// moa_streaming_unsupported.
#[tokio::test]
async fn moa_streaming_is_rejected_400() {
    set_keys();
    let server = MockServer::start().await;
    mount_upstreams(&server).await;
    let app = app_from_yaml(&cfg_yaml(&server.uri(), "concat-synthesize", false));
    let (status, v) = post_moa(app, "moa/arm-e", true).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(v["error"]["code"], json!("moa_streaming_unsupported"));
    assert_eq!(v["error"]["type"], json!("invalid_request_error"));
}

/// Unknown recipe -> 404 model_not_found.
#[tokio::test]
async fn unknown_recipe_returns_404() {
    set_keys();
    let server = MockServer::start().await;
    mount_upstreams(&server).await;
    let app = app_from_yaml(&cfg_yaml(&server.uri(), "concat-synthesize", false));
    // "moa/judge" is allowlisted but has no recipe defined.
    let (status, v) = post_moa(app, "moa/judge", false).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(v["error"]["code"], json!("model_not_found"));
}

/// Quorum failure -> 503 moa_quorum_failed (2 of 3 proposers fail, quorum 2).
#[tokio::test]
async fn quorum_failure_returns_503() {
    set_keys();
    let server = MockServer::start().await;
    // aggregator (should never be reached)
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(body_string_contains("Mixture-of-Agents"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"choices":[]})))
        .mount(&server)
        .await;
    // models a and b fail; only c succeeds -> 1 < quorum(2)
    for m in ["a", "b"] {
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .and(body_string_contains(format!("\"model\":\"{m}\"")))
            .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
            .mount(&server)
            .await;
    }
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "choices": [{"index":0,"message":{"role":"assistant","content":"ok"}}],
            "usage": {"total_tokens": 1}
        })))
        .mount(&server)
        .await;

    let app = app_from_yaml(&cfg_yaml(&server.uri(), "concat-synthesize", false));
    let (status, v) = post_moa(app, "moa/arm-e", false).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(v["error"]["code"], json!("moa_quorum_failed"));
}

/// Per-arm metrics appear in /metrics after a MoA request.
#[tokio::test]
async fn metrics_include_per_arm_series() {
    set_keys();
    let server = MockServer::start().await;
    mount_upstreams(&server).await;
    let app = app_from_yaml(&cfg_yaml(&server.uri(), "concat-synthesize", false));
    let (status, _) = post_moa(app.clone(), "moa/arm-e", false).await;
    assert_eq!(status, StatusCode::OK);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(
        text.contains("moaray_moa_arm_total"),
        "per-arm metric missing:\n{text}"
    );
    // upstream_id label present (low-cardinality), no secret labels
    assert!(text.contains("upstream_id"));
}
