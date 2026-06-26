//! Trace-correlation test, isolated in its own test binary.
//!
//! Phase-2 acceptance #5: every fan-out arm span carries the main request id.
//! This lives in a dedicated integration-test crate (its own process) rather
//! than the in-module unit tests because `tracing` caches per-callsite interest
//! globally per process: a sibling unit test that first touches the `moa_arm`
//! callsite with no subscriber installed can poison the interest cache and make
//! a later capturing test flaky. A separate binary gives this callsite a clean
//! process. The assertion itself is deterministic — a custom `Layer` records the
//! `request_id` field synchronously in `on_new_span`.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use moaray_core::error::Result;
use moaray_core::provider::{Provider, RawResponse, ReqCtx};
use moaray_core::types::{ChatMessage, ChatRequest, ChatResponse};
use moaray_moa::{MapResolver, Orchestrator, Recipe, Strategy};
use tracing::field::{Field, Visit};
use tracing::span::Attributes;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

struct OkProvider {
    id: String,
}

#[async_trait]
impl Provider for OkProvider {
    fn upstream_id(&self) -> &str {
        &self.id
    }
    async fn passthrough(&self, _: &ReqCtx, _: bytes::Bytes) -> Result<RawResponse> {
        unreachable!()
    }
    async fn passthrough_stream(&self, _: &ReqCtx, _: bytes::Bytes) -> Result<RawResponse> {
        unreachable!()
    }
    async fn chat(&self, _: &ReqCtx, _: ChatRequest) -> Result<ChatResponse> {
        Ok(ChatResponse {
            id: Some("c".into()),
            object: Some("chat.completion".into()),
            model: Some("real".into()),
            choices: vec![serde_json::json!({
                "index": 0,
                "message": {"role":"assistant","content":"ok"},
                "finish_reason":"stop"
            })],
            usage: Some(serde_json::json!({"total_tokens": 1})),
            extra: Default::default(),
        })
    }
}

#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<Vec<(String, String)>>>);
struct RidVisitor(Option<String>);
impl Visit for RidVisitor {
    fn record_debug(&mut self, f: &Field, v: &dyn std::fmt::Debug) {
        if f.name() == "request_id" {
            self.0 = Some(format!("{v:?}"));
        }
    }
    fn record_str(&mut self, f: &Field, v: &str) {
        if f.name() == "request_id" {
            self.0 = Some(v.to_string());
        }
    }
}
impl<S> Layer<S> for Recorder
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, _: &tracing::Id, _: Context<'_, S>) {
        let name = attrs.metadata().name().to_string();
        let mut v = RidVisitor(None);
        attrs.record(&mut v);
        if let Some(rid) = v.0 {
            self.0.lock().unwrap().push((name, rid));
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn fan_out_arm_spans_carry_main_request_id() {
    let mut resolver = MapResolver::new();
    for (m, u) in [("a", "u1"), ("b", "u2"), ("c", "u3"), ("agg", "uagg")] {
        resolver.insert(
            m,
            Arc::new(OkProvider { id: u.into() }) as Arc<dyn Provider>,
        );
    }
    let recipe = Recipe {
        name: "arm-e".into(),
        proposers: vec!["a".into(), "b".into(), "c".into()],
        aggregator: "agg".into(),
        strategy: Strategy::ConcatSynthesize,
        arm_timeout_ms: 1000,
        quorum: 2,
    };
    let recipes = std::iter::once((recipe.name.clone(), recipe)).collect();
    let orch = Orchestrator::new(resolver, recipes);

    let recorder = Recorder::default();
    let subscriber = tracing_subscriber::registry().with(recorder.clone());
    let guard = tracing::subscriber::set_default(subscriber);

    let rctx = ReqCtx {
        request_id: "rid-trace-9".into(),
        deadline: Instant::now() + Duration::from_secs(5),
        caller_key_id: "team-a".into(),
        model: "moa/arm-e".into(),
    };
    let req = ChatRequest {
        model: "moa/arm-e".into(),
        messages: vec![ChatMessage {
            role: "user".into(),
            content: Some(serde_json::json!("q")),
            extra: Default::default(),
        }],
        stream: None,
        max_tokens: None,
        temperature: None,
        top_p: None,
        extra: Default::default(),
    };
    orch.run(&rctx, "arm-e", req)
        .await
        .unwrap()
        .outcome
        .unwrap();
    drop(guard);

    let spans = recorder.0.lock().unwrap().clone();
    let arms: Vec<_> = spans.iter().filter(|(n, _)| n == "moa_arm").collect();
    assert_eq!(arms.len(), 3, "expected 3 arm spans, got {spans:?}");
    for (_, rid) in &arms {
        assert_eq!(
            rid, "rid-trace-9",
            "fan-out arm span missing main request id"
        );
    }
    // aggregate span correlated too (created + awaited inline on this thread)
    assert!(
        spans
            .iter()
            .any(|(n, r)| n == "moa_aggregate" && r == "rid-trace-9"),
        "aggregate span missing request id: {spans:?}"
    );
}
