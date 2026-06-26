//! P3-3 config hot-reload acceptance tests (machine-checkable, 7 criteria from
//! DESIGN-P3-3.md §"P3-3 验收").
//!
//! Each test drives a real `ConfigReloader` over the live `AppState` an in-process
//! axum app serves from, against a wiremock upstream, and asserts the
//! state-preserving / publish-order / live-field behaviour the design pins:
//!
//! 1. 429 rate-limit and OPEN breaker survive a reload that doesn't touch the upstream.
//! 2. Renaming a model (base_url unchanged) preserves limiter/breaker state.
//! 3. Two model aliases on one identity triple share ONE bucket (no bypass).
//! 4. A reload that adds an upstream never yields "provider without limiter" (no panic).
//! 5. A removed upstream's state outlives in-flight requests on the old Runtime.
//! 6. request_timeout / max_body changes take effect live (or warn+ignore).
//! 7. A partially-invalid reload keeps the old Runtime + errors + the service survives.

use std::sync::Arc;
use std::time::Duration;

use moaray::app::{build_router, ServerCtx};
use moaray::observe::init_metrics;
use moaray::registry;
use moaray::reload::ConfigReloader;
use moaray::runtime::{AppState, Runtime, StatefulState};
use moaray_providers::build_client;
use tower::ServiceExt;

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Build the live `AppState` + a `ConfigReloader` (short GC window for tests) from
/// an initial YAML, sharing one persistent upstream client. Returns the app
/// router (cloneable, shares the same `ArcSwap`/`StatefulState`) and the reloader.
fn harness(yaml: &str) -> (axum::Router, Arc<ConfigReloader>) {
    let config = moaray_config::load_yaml(yaml).expect("valid initial config");
    let stateful = Arc::new(StatefulState::from_config(&config));
    let client = build_client();
    let built =
        registry::build_providers_with(&config, &stateful, &client, None).expect("providers build");
    let orchestrator = registry::build_orchestrator_from_built(&config, &built);
    let providers = built
        .iter()
        .map(|(n, b)| (n.clone(), b.provider.clone()))
        .collect();
    let runtime = Runtime {
        config,
        providers,
        orchestrator,
    };
    let state = AppState::with_stateful(runtime, stateful);
    let reloader = Arc::new(
        ConfigReloader::new(state.clone(), client, "unused-path.yaml", built)
            .with_gc_delay(Duration::from_millis(150)),
    );
    let router = build_router(ServerCtx {
        state,
        metrics: init_metrics(),
    });
    (router, reloader)
}

fn set_keys() {
    std::env::set_var("MOARAY_HR_INBOUND", "sk-inbound");
    std::env::set_var("MOARAY_HR_UPSTREAM", "sk-upstream");
    // Same secret under a second env name -> distinct upstream identity against
    // one mock (state_key = provider_type|base_url|api_key_env).
    std::env::set_var("MOARAY_HR_UPSTREAM_ALT", "sk-upstream");
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

async fn mount_500(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .mount(server)
        .await;
}

fn req(model: &str, body_extra: &str) -> Request<Body> {
    let body = format!(r#"{{"model":"{model}","messages":[]{body_extra}}}"#);
    Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(header::AUTHORIZATION, "Bearer sk-inbound")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap()
}

async fn post(app: &axum::Router, model: &str) -> StatusCode {
    app.clone().oneshot(req(model, "")).await.unwrap().status()
}

/// One model `gpt` against `uri`, optional per-upstream rate_limit / breaker /
/// concurrency / timeout / body knobs filled by the caller.
fn one_model_yaml(uri: &str, model_extra: &str, server_extra: &str) -> String {
    format!(
        r#"
server:
{server_extra}
auth:
  keys:
    - id: team-a
      key_env: MOARAY_HR_INBOUND
      allow_models: [gpt, "moa/r"]
models:
  - name: gpt
    provider_type: openai-compat
    base_url: {uri}
    api_key_env: MOARAY_HR_UPSTREAM
{model_extra}
"#
    )
}

// ---------------------------------------------------------------------------
// Acceptance 1: 429 survives reload; OPEN breaker survives reload.
// ---------------------------------------------------------------------------

/// [P0 盲区] A request that drained the per-upstream bucket stays 429 across a
/// reload that does not touch that upstream; an OPEN breaker stays open (503
/// circuit_open) across such a reload. The reload must NOT reset limiter/breaker.
#[tokio::test]
async fn accept1_rate_limit_and_breaker_survive_reload() {
    set_keys();

    // -- 1a. rate limit (429) survives a reload --
    {
        let server = MockServer::start().await;
        mount_ok(&server).await;
        let yaml = one_model_yaml(
            &server.uri(),
            "    upstream_id: up-gpt\n    rate_limit: {rps: 1, burst: 1}",
            "  request_timeout_ms: 2000",
        );
        let (app, reloader) = harness(&yaml);
        // burst 1: first OK, second drains -> 429.
        assert_eq!(post(&app, "gpt").await, StatusCode::OK);
        assert_eq!(post(&app, "gpt").await, StatusCode::TOO_MANY_REQUESTS);
        // Reload the SAME config (upstream untouched). Must preserve the drained bucket.
        let config = moaray_config::load_yaml(&yaml).unwrap();
        let out = reloader.apply_validated(&config).await.unwrap();
        assert_eq!(out.upstreams_unchanged, 1);
        assert_eq!(out.upstreams_added, 0);
        // Still rate-limited right after the reload — state was not reset.
        assert_eq!(
            post(&app, "gpt").await,
            StatusCode::TOO_MANY_REQUESTS,
            "reload must not refill the drained per-upstream bucket"
        );
    }

    // -- 1b. OPEN breaker survives a reload --
    {
        let server = MockServer::start().await;
        mount_500(&server).await;
        let yaml = one_model_yaml(
            &server.uri(),
            "    upstream_id: up-gpt",
            "  request_timeout_ms: 2000\n  breaker: {failure_threshold: 2, open_ms: 60000, half_open_successes: 1}",
        );
        let (app, reloader) = harness(&yaml);
        // Two 5xx -> 502 each -> breaker trips OPEN.
        assert_eq!(post(&app, "gpt").await, StatusCode::BAD_GATEWAY);
        assert_eq!(post(&app, "gpt").await, StatusCode::BAD_GATEWAY);
        // Third fails fast: 503 circuit_open.
        assert_eq!(post(&app, "gpt").await, StatusCode::SERVICE_UNAVAILABLE);
        // Reload (upstream untouched) must keep the breaker OPEN.
        let config = moaray_config::load_yaml(&yaml).unwrap();
        reloader.apply_validated(&config).await.unwrap();
        assert_eq!(
            post(&app, "gpt").await,
            StatusCode::SERVICE_UNAVAILABLE,
            "reload must not reset the OPEN circuit breaker"
        );
    }
}

// ---------------------------------------------------------------------------
// Acceptance 2: rename a model (base_url unchanged) -> state preserved.
// ---------------------------------------------------------------------------

/// [F1] Renaming the public model name while keeping the identity triple
/// (provider_type|base_url|api_key_env) preserves the per-upstream limiter state —
/// because state is keyed by the derived `state_key`, not the model name or the
/// observability `upstream_id`.
#[tokio::test]
async fn accept2_model_rename_preserves_state_by_identity() {
    set_keys();
    let server = MockServer::start().await;
    mount_ok(&server).await;

    let yaml_v1 = one_model_yaml(
        &server.uri(),
        "    upstream_id: friendly-1\n    rate_limit: {rps: 1, burst: 1}",
        "  request_timeout_ms: 2000",
    );
    let (app, reloader) = harness(&yaml_v1);
    // Drain the bucket.
    assert_eq!(post(&app, "gpt").await, StatusCode::OK);
    assert_eq!(post(&app, "gpt").await, StatusCode::TOO_MANY_REQUESTS);

    // v2: rename the model gpt -> gpt2 AND relabel upstream_id, same base_url+key.
    let yaml_v2 = format!(
        r#"
server:
  request_timeout_ms: 2000
auth:
  keys:
    - id: team-a
      key_env: MOARAY_HR_INBOUND
      allow_models: [gpt2]
models:
  - name: gpt2
    provider_type: openai-compat
    base_url: {uri}
    api_key_env: MOARAY_HR_UPSTREAM
    upstream_id: friendly-2
    rate_limit: {{rps: 1, burst: 1}}
"#,
        uri = server.uri()
    );
    let config_v2 = moaray_config::load_yaml(&yaml_v2).unwrap();
    let out = reloader.apply_validated(&config_v2).await.unwrap();
    // Same identity triple -> treated as unchanged upstream (state preserved).
    assert_eq!(out.upstreams_unchanged, 1, "identity triple unchanged");
    assert_eq!(out.upstreams_added, 0);
    // The renamed model inherits the drained bucket -> immediately 429.
    assert_eq!(
        post(&app, "gpt2").await,
        StatusCode::TOO_MANY_REQUESTS,
        "rename preserves per-upstream state (keyed by identity, not name)"
    );
}

// ---------------------------------------------------------------------------
// Acceptance 3: two aliases on one triple share one bucket (no bypass).
// ---------------------------------------------------------------------------

/// [F1] Two distinct model names pointing at the same identity triple share ONE
/// per-upstream bucket — a caller cannot defeat the per-upstream cap by aliasing.
#[tokio::test]
async fn accept3_two_aliases_share_one_bucket() {
    set_keys();
    let server = MockServer::start().await;
    mount_ok(&server).await;
    // alias1 and alias2: same provider_type/base_url/api_key_env, distinct names
    // and distinct observability upstream_id labels. burst=1 on the shared bucket.
    let yaml = format!(
        r#"
auth:
  keys:
    - id: team-a
      key_env: MOARAY_HR_INBOUND
      allow_models: [alias1, alias2]
models:
  - name: alias1
    provider_type: openai-compat
    base_url: {uri}
    api_key_env: MOARAY_HR_UPSTREAM
    upstream_id: label-a
    rate_limit: {{rps: 1, burst: 1}}
  - name: alias2
    provider_type: openai-compat
    base_url: {uri}
    api_key_env: MOARAY_HR_UPSTREAM
    upstream_id: label-b
    rate_limit: {{rps: 1, burst: 1}}
"#,
        uri = server.uri()
    );
    let (app, _reloader) = harness(&yaml);
    // First call on alias1 drains the shared bucket; alias2 then hits the SAME
    // drained bucket -> 429. If they had separate buckets, alias2 would be 200.
    assert_eq!(post(&app, "alias1").await, StatusCode::OK);
    assert_eq!(
        post(&app, "alias2").await,
        StatusCode::TOO_MANY_REQUESTS,
        "aliases of one upstream identity must share one bucket (no bypass)"
    );
}

// ---------------------------------------------------------------------------
// Acceptance 4: reload adds an upstream -> swap-time concurrency never panics
// and never sees "provider without limiter".
// ---------------------------------------------------------------------------

/// [F3 发布序] A reload that introduces a brand-new upstream + model, hammered by
/// concurrent traffic across the swap, must never panic and never route to a
/// provider whose limiter slot is missing. The publish order (ensure state ->
/// build providers -> swap) guarantees the bucket exists before the model is
/// routable.
#[tokio::test]
async fn accept4_reload_add_upstream_no_provider_without_limiter() {
    set_keys();
    let server = MockServer::start().await;
    mount_ok(&server).await;

    // v1: only gpt.
    let yaml_v1 = one_model_yaml(
        &server.uri(),
        "    upstream_id: up-gpt",
        "  request_timeout_ms: 2000",
    );
    let (app, reloader) = harness(&yaml_v1);
    assert_eq!(post(&app, "gpt").await, StatusCode::OK);

    // v2: add a second upstream identity (distinct key env) + model `added`.
    let yaml_v2 = format!(
        r#"
server:
  request_timeout_ms: 2000
auth:
  keys:
    - id: team-a
      key_env: MOARAY_HR_INBOUND
      allow_models: [gpt, added]
models:
  - name: gpt
    provider_type: openai-compat
    base_url: {uri}
    api_key_env: MOARAY_HR_UPSTREAM
    upstream_id: up-gpt
  - name: added
    provider_type: openai-compat
    base_url: {uri}
    api_key_env: MOARAY_HR_UPSTREAM_ALT
    upstream_id: up-added
    rate_limit: {{rps: 1000, burst: 1000}}
"#,
        uri = server.uri()
    );
    let config_v2 = Arc::new(moaray_config::load_yaml(&yaml_v2).unwrap());

    // Spawn concurrent traffic against the soon-to-exist `added` model while the
    // reload runs. Any "provider without limiter" window would panic the handler
    // task (fail-closed build) — we assert every spawned request returns a real
    // HTTP status (200 once routable, or 404 model_not_found before the swap),
    // never a task panic.
    let mut handles = Vec::new();
    for _ in 0..64 {
        let app2 = app.clone();
        handles.push(tokio::spawn(async move { post(&app2, "added").await }));
    }
    let out = reloader.apply_validated(&config_v2).await.unwrap();
    assert_eq!(out.upstreams_added, 1, "one new upstream identity added");

    // A few more calls strictly after the swap must all be 200 (routable + bucket).
    for _ in 0..16 {
        assert_eq!(post(&app, "added").await, StatusCode::OK);
    }
    // None of the racing tasks panicked; each yielded a valid status.
    for h in handles {
        let status = h.await.expect("handler task must not panic");
        assert!(
            status == StatusCode::OK || status == StatusCode::NOT_FOUND,
            "racing request saw an invalid status {status} (provider-without-limiter window?)"
        );
    }
}

// ---------------------------------------------------------------------------
// Acceptance 5: removed upstream's state outlives in-flight requests.
// ---------------------------------------------------------------------------

/// [F4] When a reload removes an upstream, an in-flight request that resolved the
/// OLD `Runtime` (and thus holds an `Arc<dyn Provider>` -> `Arc<UpstreamState>`)
/// must complete without panic, with its limiter/breaker state alive — even after
/// the state map has dropped the slot. We simulate the in-flight hold by cloning
/// the provider out of the old runtime, then reloading it away, then GC'ing.
#[tokio::test]
async fn accept5_removed_upstream_state_survives_inflight() {
    set_keys();
    let server = MockServer::start().await;
    mount_ok(&server).await;

    // v1: two upstreams, `keep` and `drop_me`.
    let yaml_v1 = format!(
        r#"
server:
  request_timeout_ms: 2000
auth:
  keys:
    - id: team-a
      key_env: MOARAY_HR_INBOUND
      allow_models: [keep, drop_me]
models:
  - name: keep
    provider_type: openai-compat
    base_url: {uri}
    api_key_env: MOARAY_HR_UPSTREAM
    upstream_id: up-keep
  - name: drop_me
    provider_type: openai-compat
    base_url: {uri}
    api_key_env: MOARAY_HR_UPSTREAM_ALT
    upstream_id: up-drop
    rate_limit: {{rps: 5, burst: 5}}
"#,
        uri = server.uri()
    );
    let (app, reloader) = harness(&yaml_v1);
    assert_eq!(post(&app, "drop_me").await, StatusCode::OK);

    // Grab the OLD runtime's provider for drop_me — this models an in-flight
    // request that already resolved its provider before the swap.
    let old_runtime = reloader.state().runtime.load_full();
    let inflight_provider = old_runtime
        .provider("drop_me")
        .expect("drop_me resolvable on old runtime");

    // v2: remove drop_me, keep only `keep`. (drop_me stays in the allowlist so a
    // call to it reaches routing and yields 404 model_not_found, not a 403.)
    let yaml_v2 = format!(
        r#"
server:
  request_timeout_ms: 2000
auth:
  keys:
    - id: team-a
      key_env: MOARAY_HR_INBOUND
      allow_models: [keep, drop_me]
models:
  - name: keep
    provider_type: openai-compat
    base_url: {uri}
    api_key_env: MOARAY_HR_UPSTREAM
    upstream_id: up-keep
"#,
        uri = server.uri()
    );
    let config_v2 = moaray_config::load_yaml(&yaml_v2).unwrap();
    let out = reloader.apply_validated(&config_v2).await.unwrap();
    assert_eq!(
        out.upstreams_removed, 1,
        "drop_me's upstream identity removed"
    );

    // The new runtime no longer routes drop_me (404), proving the swap happened.
    assert_eq!(post(&app, "drop_me").await, StatusCode::NOT_FOUND);

    // Wait past the GC window so orphaned state is actually reclaimed from the map.
    tokio::time::sleep(Duration::from_millis(400)).await;

    // The in-flight provider (held since before the swap) still works: a real
    // upstream call through it succeeds and its breaker/limiter are alive. No panic.
    use moaray_core::provider::ReqCtx;
    let rctx = ReqCtx {
        request_id: "inflight-after-reload".into(),
        deadline: std::time::Instant::now() + Duration::from_secs(2),
        caller_key_id: "team-a".into(),
        model: "drop_me".into(),
    };
    let resp = inflight_provider
        .passthrough(
            &rctx,
            bytes::Bytes::from_static(b"{\"model\":\"drop_me\",\"messages\":[]}"),
        )
        .await
        .expect("in-flight request on removed upstream must still complete");
    assert_eq!(
        resp.status, 200,
        "removed-upstream in-flight call still succeeds"
    );
}

// ---------------------------------------------------------------------------
// Acceptance 6: request_timeout / max_body changes take effect live.
// ---------------------------------------------------------------------------

/// [F2] `max_body_bytes` is read from the live config snapshot per request, so a
/// reload that lowers it takes effect immediately: a body that was accepted
/// becomes 413 after the reload. (Proves the hot field is not frozen at startup.)
#[tokio::test]
async fn accept6_max_body_bytes_is_live_after_reload() {
    set_keys();
    let server = MockServer::start().await;
    mount_ok(&server).await;
    // v1: generous body cap.
    let yaml_v1 = one_model_yaml(
        &server.uri(),
        "    upstream_id: up-gpt",
        "  request_timeout_ms: 2000\n  max_body_bytes: 1048576",
    );
    let (app, reloader) = harness(&yaml_v1);
    // A ~200-byte padded body is accepted under the 1 MiB cap.
    let padded = format!(r#","pad":"{}""#, "x".repeat(200));
    let big = app
        .clone()
        .oneshot(req("gpt", &padded))
        .await
        .unwrap()
        .status();
    assert_eq!(big, StatusCode::OK, "body fits under the initial cap");

    // v2: shrink the cap to 64 bytes (live field).
    let yaml_v2 = one_model_yaml(
        &server.uri(),
        "    upstream_id: up-gpt",
        "  request_timeout_ms: 2000\n  max_body_bytes: 64",
    );
    let config_v2 = moaray_config::load_yaml(&yaml_v2).unwrap();
    reloader.apply_validated(&config_v2).await.unwrap();

    // The SAME padded body now exceeds the live cap -> 413, no restart needed.
    let after = app
        .clone()
        .oneshot(req("gpt", &padded))
        .await
        .unwrap()
        .status();
    assert_eq!(
        after,
        StatusCode::PAYLOAD_TOO_LARGE,
        "lowered max_body_bytes must take effect live after reload (not frozen)"
    );
}

// ---------------------------------------------------------------------------
// Acceptance 7: partially-invalid reload keeps the old Runtime + service alive.
// ---------------------------------------------------------------------------

/// [全或无] A reload of an invalid config (here: an unknown recipe aggregator)
/// must fail with an error, leave the running `Runtime` untouched, and keep the
/// service serving on the last-good config. This drives the real file-based
/// `reload()` path (read -> validate -> reject) to mirror production.
#[tokio::test]
async fn accept7_invalid_reload_keeps_old_runtime_and_serves() {
    set_keys();
    let server = MockServer::start().await;
    mount_ok(&server).await;
    let yaml_v1 = one_model_yaml(
        &server.uri(),
        "    upstream_id: up-gpt",
        "  request_timeout_ms: 2000",
    );

    // Write the good config to a temp file and build a reloader pointed at it.
    let dir = std::env::temp_dir();
    let path = dir.join(format!("moaray-hr-accept7-{}.yaml", std::process::id()));
    std::fs::write(&path, &yaml_v1).unwrap();

    let config = moaray_config::load_yaml(&yaml_v1).expect("valid initial config");
    let stateful = Arc::new(StatefulState::from_config(&config));
    let client = build_client();
    let built =
        registry::build_providers_with(&config, &stateful, &client, None).expect("providers build");
    let orchestrator = registry::build_orchestrator_from_built(&config, &built);
    let providers = built
        .iter()
        .map(|(n, b)| (n.clone(), b.provider.clone()))
        .collect();
    let runtime = Runtime {
        config,
        providers,
        orchestrator,
    };
    let state = AppState::with_stateful(runtime, stateful);
    let reloader = ConfigReloader::new(
        state.clone(),
        client,
        path.to_string_lossy().to_string(),
        built,
    );
    let app = build_router(ServerCtx {
        state,
        metrics: init_metrics(),
    });

    assert_eq!(post(&app, "gpt").await, StatusCode::OK);

    // Overwrite the file with an invalid config: unknown recipe aggregator.
    let bad_yaml = format!(
        r#"
server:
  request_timeout_ms: 2000
auth:
  keys:
    - id: team-a
      key_env: MOARAY_HR_INBOUND
      allow_models: [gpt, "moa/r"]
models:
  - name: gpt
    provider_type: openai-compat
    base_url: {uri}
    api_key_env: MOARAY_HR_UPSTREAM
    upstream_id: up-gpt
recipes:
  r:
    proposers: [gpt]
    aggregator: ghost-model
    strategy: concat-synthesize
    quorum: 1
"#,
        uri = server.uri()
    );
    std::fs::write(&path, &bad_yaml).unwrap();

    // The real reload path must reject and return an error.
    let err = reloader.reload().await;
    assert!(err.is_err(), "invalid config reload must return Err");

    // Service keeps serving the last-good config, and the live runtime is unchanged.
    assert_eq!(
        post(&app, "gpt").await,
        StatusCode::OK,
        "service keeps serving the last-good config after a rejected reload"
    );
    let rt = reloader.state().runtime.load();
    assert!(rt.config.is_known_model("gpt"));
    assert!(
        rt.config.recipes.is_empty(),
        "rejected config was not applied (old runtime intact)"
    );

    let _ = std::fs::remove_file(&path);
}

// ---------------------------------------------------------------------------
// Regression (codex review): governance change with the SAME identity triple
// must take effect live, not be silently preserved.
// ---------------------------------------------------------------------------

/// A reload that keeps `state_key` (same provider_type|base_url|api_key_env) but
/// CHANGES the per-upstream `rate_limit` must apply the new limit — the old
/// `UpstreamState` + its `GovernedProvider` must be rebuilt, not preserved.
#[tokio::test]
async fn reload_changes_governance_for_same_identity() {
    set_keys();
    let server = MockServer::start().await;
    mount_ok(&server).await;
    // v1: rps high (effectively unlimited for the test's 3 calls).
    let yaml_v1 = one_model_yaml(
        &server.uri(),
        "    upstream_id: up-gpt\n    rate_limit: {rps: 1000, burst: 1000}",
        "  request_timeout_ms: 2000",
    );
    let (app, reloader) = harness(&yaml_v1);
    assert_eq!(post(&app, "gpt").await, StatusCode::OK);
    assert_eq!(post(&app, "gpt").await, StatusCode::OK);

    // v2: SAME identity (same base_url + api_key_env), tighten to burst=1.
    let yaml_v2 = one_model_yaml(
        &server.uri(),
        "    upstream_id: up-gpt\n    rate_limit: {rps: 1, burst: 1}",
        "  request_timeout_ms: 2000",
    );
    let config_v2 = moaray_config::load_yaml(&yaml_v2).unwrap();
    let out = reloader.apply_validated(&config_v2).await.unwrap();
    assert_eq!(out.upstreams_unchanged, 1, "identity triple unchanged");
    assert_eq!(
        out.providers_built, 1,
        "provider rebuilt for new governance"
    );
    assert_eq!(out.providers_reused, 0);

    // New tight limit must apply live: first OK, second 429.
    assert_eq!(post(&app, "gpt").await, StatusCode::OK);
    assert_eq!(
        post(&app, "gpt").await,
        StatusCode::TOO_MANY_REQUESTS,
        "tightened rate_limit must take effect on reload (not silently preserved)"
    );
}

// ---------------------------------------------------------------------------
// Regression (codex review): dropping a per-key rate_limit takes effect live.
// ---------------------------------------------------------------------------

/// A reload that removes an inbound key's `rate_limit` (upstreams unchanged) must
/// stop limiting that caller immediately — the orphan per-key bucket is reclaimed
/// at publish time, not left to keep 429'ing.
#[tokio::test]
async fn reload_drops_per_key_limit_live() {
    set_keys();
    let server = MockServer::start().await;
    mount_ok(&server).await;
    // v1: per-key limit burst=1.
    let yaml_v1 = format!(
        r#"
server:
  request_timeout_ms: 2000
auth:
  keys:
    - id: team-a
      key_env: MOARAY_HR_INBOUND
      allow_models: [gpt]
      rate_limit: {{rps: 1, burst: 1}}
models:
  - name: gpt
    provider_type: openai-compat
    base_url: {uri}
    api_key_env: MOARAY_HR_UPSTREAM
    upstream_id: up-gpt
"#,
        uri = server.uri()
    );
    let (app, reloader) = harness(&yaml_v1);
    assert_eq!(post(&app, "gpt").await, StatusCode::OK);
    assert_eq!(post(&app, "gpt").await, StatusCode::TOO_MANY_REQUESTS);

    // v2: same upstream, key loses its rate_limit.
    let yaml_v2 = format!(
        r#"
server:
  request_timeout_ms: 2000
auth:
  keys:
    - id: team-a
      key_env: MOARAY_HR_INBOUND
      allow_models: [gpt]
models:
  - name: gpt
    provider_type: openai-compat
    base_url: {uri}
    api_key_env: MOARAY_HR_UPSTREAM
    upstream_id: up-gpt
"#,
        uri = server.uri()
    );
    let config_v2 = moaray_config::load_yaml(&yaml_v2).unwrap();
    reloader.apply_validated(&config_v2).await.unwrap();

    // The caller is no longer limited — repeated calls all succeed immediately.
    for _ in 0..5 {
        assert_eq!(
            post(&app, "gpt").await,
            StatusCode::OK,
            "dropping the per-key limit must take effect live (orphan bucket reclaimed)"
        );
    }
}

// ---------------------------------------------------------------------------
// Regression (codex review): an upstream removed then re-added before the GC
// window keeps its live slot (GC retains against the live config, not a snapshot).
// ---------------------------------------------------------------------------

/// Remove an upstream, then re-add the SAME identity before the (short) GC window
/// elapses. The stale GC task must NOT delete the now-live slot — it retains
/// against the live runtime config.
#[tokio::test]
async fn reload_readd_before_gc_keeps_live_state() {
    set_keys();
    let server = MockServer::start().await;
    mount_ok(&server).await;

    // v1: keep + drop_me (distinct identities).
    let yaml_v1 = format!(
        r#"
server:
  request_timeout_ms: 2000
auth:
  keys:
    - id: team-a
      key_env: MOARAY_HR_INBOUND
      allow_models: [keep, drop_me]
models:
  - name: keep
    provider_type: openai-compat
    base_url: {uri}
    api_key_env: MOARAY_HR_UPSTREAM
    upstream_id: up-keep
  - name: drop_me
    provider_type: openai-compat
    base_url: {uri}
    api_key_env: MOARAY_HR_UPSTREAM_ALT
    upstream_id: up-drop
    rate_limit: {{rps: 1, burst: 1}}
"#,
        uri = server.uri()
    );
    let (app, reloader) = harness(&yaml_v1); // gc_delay = 150ms
                                             // Drain drop_me's bucket so we can tell if its state survived (429) or was
                                             // wrongly reset (200).
    assert_eq!(post(&app, "drop_me").await, StatusCode::OK);
    assert_eq!(post(&app, "drop_me").await, StatusCode::TOO_MANY_REQUESTS);

    // v2: remove drop_me (schedules a delayed GC for up-drop).
    let yaml_v2 = format!(
        r#"
server:
  request_timeout_ms: 2000
auth:
  keys:
    - id: team-a
      key_env: MOARAY_HR_INBOUND
      allow_models: [keep, drop_me]
models:
  - name: keep
    provider_type: openai-compat
    base_url: {uri}
    api_key_env: MOARAY_HR_UPSTREAM
    upstream_id: up-keep
"#,
        uri = server.uri()
    );
    let config_v2 = moaray_config::load_yaml(&yaml_v2).unwrap();
    assert_eq!(
        reloader
            .apply_validated(&config_v2)
            .await
            .unwrap()
            .upstreams_removed,
        1
    );

    // v3: re-add drop_me with the SAME identity triple BEFORE the GC fires.
    let config_v3 = moaray_config::load_yaml(&yaml_v1).unwrap();
    let out3 = reloader.apply_validated(&config_v3).await.unwrap();
    // Same identity restored from the still-present slot -> preserved (drained).
    assert_eq!(
        out3.upstreams_unchanged, 1,
        "re-added identity still present"
    );

    // Wait past the original GC window. The stale GC task must NOT delete up-drop,
    // because it retains against the LIVE config (which now includes drop_me).
    tokio::time::sleep(Duration::from_millis(400)).await;

    // drop_me's bucket survived end-to-end: still drained -> 429 (not reset to 200).
    assert_eq!(
        post(&app, "drop_me").await,
        StatusCode::TOO_MANY_REQUESTS,
        "re-added-before-GC upstream must keep its live state (GC is generation-safe)"
    );
}
