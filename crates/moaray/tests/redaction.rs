//! Machine-checked secret redaction: drive a full request through the gateway
//! while capturing all tracing output into a buffer, then assert that neither
//! the inbound bearer token nor the upstream API key appears anywhere in logs.

use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use moaray::app::{build_router, ServerCtx};
use moaray::observe::init_metrics;
use moaray::registry;
use moaray::runtime::{AppState, Runtime};
use tower::ServiceExt;
use tracing_subscriber::fmt::MakeWriter;

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const INBOUND_SECRET: &str = "sk-inbound-SUPER-SECRET-TOKEN";
const UPSTREAM_SECRET: &str = "sk-upstream-DO-NOT-LOG-9999";

/// A `MakeWriter` that appends everything into a shared byte buffer.
#[derive(Clone)]
struct BufWriter(Arc<Mutex<Vec<u8>>>);

impl io::Write for BufWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for BufWriter {
    type Writer = BufWriter;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

#[tokio::test]
async fn secrets_never_appear_in_tracing_output() {
    std::env::set_var("MOARAY_TEST_INBOUND_REDACT", INBOUND_SECRET);
    std::env::set_var("MOARAY_TEST_UPSTREAM_REDACT", UPSTREAM_SECRET);

    let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
    let writer = BufWriter(buf.clone());

    // Scope a subscriber that captures ALL levels into our buffer.
    let subscriber = tracing_subscriber::fmt()
        .with_writer(writer)
        .with_max_level(tracing::Level::TRACE)
        .with_ansi(false)
        .finish();

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            br#"{"choices":[{"message":{"content":"ok"}}]}"#.to_vec(),
            "application/json",
        ))
        .mount(&server)
        .await;

    let yaml = format!(
        r#"
auth:
  keys:
    - id: team-a
      key_env: MOARAY_TEST_INBOUND_REDACT
      allow_models: [gpt]
models:
  - name: gpt
    provider_type: openai-compat
    base_url: {}
    api_key_env: MOARAY_TEST_UPSTREAM_REDACT
"#,
        server.uri()
    );

    let config = moaray_config::load_yaml(&yaml).expect("valid config");
    let request_timeout = Duration::from_millis(config.server.request_timeout_ms);
    let max_body_bytes = config.server.max_body_bytes;
    let providers = registry::build_providers(&config);
    let runtime = Runtime { config, providers };
    let app = build_router(ServerCtx {
        state: AppState::new(runtime),
        metrics: init_metrics(),
        request_timeout,
        max_body_bytes,
    });

    // Install the capturing subscriber for this thread; awaits run on the
    // ambient tokio runtime.
    let guard = tracing::subscriber::set_default(subscriber);
    tracing::info!(model = "gpt", "handling request");
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header(header::AUTHORIZATION, format!("Bearer {INBOUND_SECRET}"))
                .body(Body::from(r#"{"model":"gpt","messages":[]}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    drop(guard);

    let captured = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(
        !captured.contains(INBOUND_SECRET),
        "inbound token leaked into logs:\n{captured}"
    );
    assert!(
        !captured.contains(UPSTREAM_SECRET),
        "upstream api key leaked into logs:\n{captured}"
    );
    // sanity: we actually captured *something* so the test isn't vacuous
    assert!(captured.contains("handling request"));
}
