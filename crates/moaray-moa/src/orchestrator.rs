//! The MoA orchestrator: parallel fan-out, quorum tolerance, aggregation.
//!
//! **Dependency boundary (load-bearing):** drives upstreams only through
//! `Arc<dyn Provider>`; never references `moaray-providers`.
//!
//! **Concurrency & cancellation (moa-partial-failure rule):** proposer arms run
//! concurrently on a [`FuturesUnordered`] driven directly by this future — there
//! is **no `tokio::spawn`**. That is deliberate: structured concurrency means if
//! the caller's future is dropped (client disconnect, request timeout), the
//! whole set is dropped, which cancels every in-flight arm and tears down its
//! upstream connection. Each arm has its own effective timeout (the smaller of
//! the recipe `arm_timeout_ms` and the time left on [`ReqCtx::deadline`]), so a
//! slow arm never blocks the others. We wait for all arms to finish or time out,
//! then aggregate; if fewer than `quorum` proposers succeed we fail closed with
//! [`Error::MoaQuorumFailed`] rather than return a partial silent result.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::stream::{FuturesUnordered, StreamExt};
use moaray_core::error::{Error, Result};
use moaray_core::provider::{Provider, ReqCtx};
use moaray_core::types::ChatRequest;
use serde_json::{Map, Value};
use tracing::Instrument;

use crate::recipe::Recipe;
use crate::strategy::build_aggregator_request;
use crate::{Orchestrator, ProviderResolver};

/// Outcome class of a single arm — low-cardinality, safe as a metric label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArmStatus {
    /// Returned a usable structured response.
    Ok,
    /// Did not return within its effective timeout.
    Timeout,
    /// Upstream/transport error (raw upstream body never retained or leaked).
    Error,
}

impl ArmStatus {
    /// Stable, low-cardinality string for metric labels.
    pub fn as_str(&self) -> &'static str {
        match self {
            ArmStatus::Ok => "ok",
            ArmStatus::Timeout => "timeout",
            ArmStatus::Error => "error",
        }
    }
}

/// Per-arm metadata for metrics and the optional debug extension field.
///
/// Carries only non-secret data: the (recipe-level) model name, the stable
/// `upstream_id` for metric keying, latency, status, and the arm's usage if any.
#[derive(Debug, Clone)]
pub struct ArmOutcome {
    /// Recipe-level model name (proposer or aggregator role).
    pub model: String,
    /// Stable upstream id for metric labels.
    pub upstream_id: String,
    /// Wall-clock latency of the arm in milliseconds.
    pub latency_ms: u64,
    /// Outcome class.
    pub status: ArmStatus,
    /// Whether the arm reported a `usage` object (false => omitted upstream).
    pub usage_present: bool,
}

/// What a successful MoA run produces.
#[derive(Debug)]
pub struct MoaResult {
    /// The final, single OpenAI-shaped completion (usage already summed).
    pub response: moaray_core::types::ChatResponse,
    /// Per-proposer-arm outcomes (in recipe order).
    pub arms: Vec<ArmOutcome>,
    /// The aggregator/judge arm outcome.
    pub aggregator: ArmOutcome,
}

/// Internal: a finished arm — its outcome plus the response when successful.
struct ArmRun {
    idx: usize,
    outcome: ArmOutcome,
    response: Option<moaray_core::types::ChatResponse>,
}

/// Compute the effective per-arm timeout: the smaller of the recipe budget and
/// the time remaining on the request deadline (never negative).
fn effective_timeout(deadline: Instant, arm_timeout_ms: u64) -> Duration {
    let remaining = deadline.saturating_duration_since(Instant::now());
    Duration::from_millis(arm_timeout_ms).min(remaining)
}

/// Build a single proposer arm request: the original request retargeted to the
/// proposer model, forced non-streaming (MoA arms always use the structured
/// `chat()` path).
fn arm_request(original: &ChatRequest, model: &str) -> ChatRequest {
    let mut r = original.clone();
    r.model = model.to_string();
    r.stream = Some(false);
    r
}

/// Extract the assistant text from a structured chat response (choice 0).
fn extract_text(resp: &moaray_core::types::ChatResponse) -> String {
    resp.choices
        .first()
        .and_then(|c| c.get("message"))
        .and_then(|m| m.get("content"))
        .map(|v| match v {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        })
        .unwrap_or_default()
}

/// Sum the integer fields of several `usage` objects into one merged object.
///
/// Every integer-valued key (`prompt_tokens`, `completion_tokens`,
/// `total_tokens`, and any vendor integer field) is accumulated across inputs;
/// missing fields simply contribute nothing. Non-integer fields are ignored so a
/// vendor float cost field never corrupts the token totals.
fn sum_usage<'a>(usages: impl Iterator<Item = &'a Value>) -> Value {
    let mut acc: Map<String, Value> = Map::new();
    for u in usages {
        if let Some(obj) = u.as_object() {
            for (k, v) in obj {
                if let Some(n) = v.as_i64() {
                    let entry = acc.entry(k.clone()).or_insert(Value::from(0i64));
                    let cur = entry.as_i64().unwrap_or(0);
                    *entry = Value::from(cur + n);
                }
            }
        }
    }
    Value::Object(acc)
}

/// Run one arm to completion (or timeout), capturing a non-leaking outcome.
async fn run_arm(
    provider: Option<Arc<dyn Provider>>,
    ctx: &ReqCtx,
    req: ChatRequest,
    model: String,
    idx: usize,
    timeout: Duration,
) -> ArmRun {
    let upstream_id = provider
        .as_ref()
        .map(|p| p.upstream_id().to_string())
        .unwrap_or_else(|| model.clone());
    let start = Instant::now();

    let Some(provider) = provider else {
        // Misconfiguration: a proposer with no provider. Treat as an arm error
        // (config validation should prevent this) — never panic mid-fan-out.
        tracing::warn!(arm_model = %model, "no provider for proposer arm");
        return ArmRun {
            idx,
            outcome: ArmOutcome {
                model,
                upstream_id,
                latency_ms: start.elapsed().as_millis() as u64,
                status: ArmStatus::Error,
                usage_present: false,
            },
            response: None,
        };
    };

    let result = tokio::time::timeout(timeout, provider.chat(ctx, req)).await;
    let latency_ms = start.elapsed().as_millis() as u64;
    match result {
        Err(_) => {
            tracing::warn!(arm_model = %model, latency_ms, "arm timed out");
            ArmRun {
                idx,
                outcome: ArmOutcome {
                    model,
                    upstream_id,
                    latency_ms,
                    status: ArmStatus::Timeout,
                    usage_present: false,
                },
                response: None,
            }
        }
        Ok(Ok(resp)) => {
            let usage_present = resp.usage.is_some();
            ArmRun {
                idx,
                outcome: ArmOutcome {
                    model,
                    upstream_id,
                    latency_ms,
                    status: ArmStatus::Ok,
                    usage_present,
                },
                response: Some(resp),
            }
        }
        Ok(Err(_e)) => {
            // Do NOT log or retain the raw upstream error body (no-secret-logging
            // + moa-partial-failure): only the class is recorded.
            tracing::warn!(arm_model = %model, latency_ms, "arm failed");
            ArmRun {
                idx,
                outcome: ArmOutcome {
                    model,
                    upstream_id,
                    latency_ms,
                    status: ArmStatus::Error,
                    usage_present: false,
                },
                response: None,
            }
        }
    }
}

impl<R: ProviderResolver> Orchestrator<R> {
    /// Execute a MoA recipe end-to-end and return a single fused completion.
    ///
    /// Steps: resolve recipe (unknown => `model_not_found`), fan out proposers,
    /// enforce quorum, build the fixed aggregation prompt, call the aggregator,
    /// and sum usage across all successful proposers + the aggregator.
    pub async fn run(
        &self,
        ctx: &ReqCtx,
        recipe_name: &str,
        request: ChatRequest,
    ) -> Result<MoaResult> {
        let recipe = self
            .recipe(recipe_name)
            .ok_or_else(|| Error::ModelNotFound {
                model: format!("moa/{recipe_name}"),
            })?
            .clone();

        let arms = self.fan_out(ctx, &recipe, &request).await;

        // Partition into successes and outcomes. `FuturesUnordered` yields arms
        // in completion order, so we keep each arm's recipe index and sort both
        // collections by it: `MoaResult::arms` is documented as recipe order, and
        // candidate numbering must be stable/deterministic for the aggregator.
        let mut outcomes_idx: Vec<(usize, ArmOutcome)> = Vec::with_capacity(arms.len());
        let mut successes: Vec<(usize, moaray_core::types::ChatResponse)> = Vec::new();
        for a in arms {
            if let Some(resp) = a.response {
                successes.push((a.idx, resp));
            }
            outcomes_idx.push((a.idx, a.outcome));
        }
        outcomes_idx.sort_by_key(|(i, _)| *i);
        successes.sort_by_key(|(i, _)| *i);
        let outcomes: Vec<ArmOutcome> = outcomes_idx.into_iter().map(|(_, o)| o).collect();

        let succeeded = successes.len();
        if succeeded < recipe.quorum {
            return Err(Error::MoaQuorumFailed {
                succeeded,
                required: recipe.quorum,
            });
        }

        // Build the fixed, injection-guarded aggregation prompt from the
        // successful candidate texts (anonymous, numbered).
        let candidates: Vec<String> = successes
            .iter()
            .map(|(_, resp)| extract_text(resp))
            .collect();
        let agg_req = build_aggregator_request(&recipe, &request, &candidates);

        let agg_provider = self
            .resolver
            .resolve(&recipe.aggregator)
            .ok_or(Error::Internal)?;
        let agg_timeout = effective_timeout(ctx.deadline, recipe.arm_timeout_ms);
        let agg_upstream_id = agg_provider.upstream_id().to_string();
        let agg_start = Instant::now();
        let span = tracing::info_span!(
            "moa_aggregate",
            request_id = %ctx.request_id,
            strategy = ?recipe.strategy,
        );
        let agg_result = tokio::time::timeout(agg_timeout, agg_provider.chat(ctx, agg_req))
            .instrument(span)
            .await;
        let agg_latency_ms = agg_start.elapsed().as_millis() as u64;

        let mut agg_resp = match agg_result {
            Err(_) => return Err(Error::UpstreamTimeout),
            Ok(Ok(resp)) => resp,
            Ok(Err(e)) => return Err(e),
        };

        let aggregator = ArmOutcome {
            model: recipe.aggregator.clone(),
            upstream_id: agg_upstream_id,
            latency_ms: agg_latency_ms,
            status: ArmStatus::Ok,
            usage_present: agg_resp.usage.is_some(),
        };

        // usage = sum over successful proposers + aggregator.
        let mut usages: Vec<&Value> = successes
            .iter()
            .filter_map(|(_, r)| r.usage.as_ref())
            .collect();
        if let Some(u) = agg_resp.usage.as_ref() {
            usages.push(u);
        }
        let summed = sum_usage(usages.into_iter());

        // Present a single completion shaped as the requested moa model.
        agg_resp.model = Some(ctx.model.clone());
        agg_resp.usage = Some(summed);

        Ok(MoaResult {
            response: agg_resp,
            arms: outcomes,
            aggregator,
        })
    }

    /// Fan out all proposer arms concurrently and collect their finished runs.
    async fn fan_out(&self, ctx: &ReqCtx, recipe: &Recipe, request: &ChatRequest) -> Vec<ArmRun> {
        let mut futs = FuturesUnordered::new();
        for (idx, proposer) in recipe.proposers.iter().enumerate() {
            let provider = self.resolver.resolve(proposer);
            let arm_req = arm_request(request, proposer);
            let model = proposer.clone();
            let timeout = effective_timeout(ctx.deadline, recipe.arm_timeout_ms);
            let ctx = ctx.clone();
            // Instrument each arm with a span carrying the main request id so
            // fan-out logs correlate to the originating request (trace-id).
            let span = tracing::info_span!(
                "moa_arm",
                request_id = %ctx.request_id,
                arm_model = %model,
            );
            futs.push(
                async move { run_arm(provider, &ctx, arm_req, model, idx, timeout).await }
                    .instrument(span),
            );
        }

        let mut runs = Vec::with_capacity(futs.len());
        while let Some(run) = futs.next().await {
            runs.push(run);
        }
        runs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::recipe::Strategy;
    use crate::MapResolver;
    use async_trait::async_trait;
    use moaray_core::provider::RawResponse;
    use moaray_core::types::{ChatRequest, ChatResponse};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;

    /// A controllable fake provider for fan-out tests.
    struct FakeProvider {
        id: String,
        behavior: Behavior,
        calls: Arc<AtomicUsize>,
    }

    #[derive(Clone)]
    enum Behavior {
        /// Reply with this content + usage tokens.
        Ok { content: String, tokens: i64 },
        /// Reply with content but no usage object.
        OkNoUsage { content: String },
        /// Return an upstream error.
        Err,
        /// Sleep this long before replying (drives the timeout path).
        Slow { ms: u64 },
    }

    impl FakeProvider {
        fn new(id: &str, behavior: Behavior) -> (Arc<Self>, Arc<AtomicUsize>) {
            let calls = Arc::new(AtomicUsize::new(0));
            (
                Arc::new(Self {
                    id: id.to_string(),
                    behavior,
                    calls: calls.clone(),
                }),
                calls,
            )
        }
    }

    fn completion(content: &str, tokens: Option<i64>) -> ChatResponse {
        let usage = tokens.map(|t| {
            serde_json::json!({
                "prompt_tokens": t,
                "completion_tokens": t,
                "total_tokens": t * 2
            })
        });
        ChatResponse {
            id: Some("cmpl".into()),
            object: Some("chat.completion".into()),
            model: Some("real".into()),
            choices: vec![serde_json::json!({
                "index": 0,
                "message": {"role": "assistant", "content": content},
                "finish_reason": "stop"
            })],
            usage,
            extra: Default::default(),
        }
    }

    #[async_trait]
    impl Provider for FakeProvider {
        fn upstream_id(&self) -> &str {
            &self.id
        }
        async fn passthrough(&self, _: &ReqCtx, _: bytes::Bytes) -> Result<RawResponse> {
            unreachable!("MoA uses chat() only")
        }
        async fn passthrough_stream(&self, _: &ReqCtx, _: bytes::Bytes) -> Result<RawResponse> {
            unreachable!("MoA uses chat() only")
        }
        async fn chat(&self, _: &ReqCtx, _req: ChatRequest) -> Result<ChatResponse> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match &self.behavior {
                Behavior::Ok { content, tokens } => Ok(completion(content, Some(*tokens))),
                Behavior::OkNoUsage { content } => Ok(completion(content, None)),
                Behavior::Err => Err(Error::UpstreamError),
                Behavior::Slow { ms } => {
                    tokio::time::sleep(Duration::from_millis(*ms)).await;
                    Ok(completion("slow", Some(1)))
                }
            }
        }
    }

    fn ctx(deadline_ms: u64) -> ReqCtx {
        ReqCtx {
            request_id: "rid-123".into(),
            deadline: Instant::now() + Duration::from_millis(deadline_ms),
            caller_key_id: "team-a".into(),
            model: "moa/arm-e".into(),
        }
    }

    fn user_request() -> ChatRequest {
        ChatRequest {
            model: "moa/arm-e".into(),
            messages: vec![moaray_core::types::ChatMessage {
                role: "user".into(),
                content: Some(serde_json::json!("solve it")),
                extra: Default::default(),
            }],
            stream: None,
            max_tokens: None,
            temperature: None,
            top_p: None,
            extra: Default::default(),
        }
    }

    fn recipe(strategy: Strategy, proposers: Vec<&str>, quorum: usize) -> Recipe {
        Recipe {
            name: "arm-e".into(),
            proposers: proposers.into_iter().map(String::from).collect(),
            aggregator: "agg".into(),
            strategy,
            arm_timeout_ms: 200,
            quorum,
        }
    }

    /// 3/4 proposers succeed, quorum=3 -> proceeds and fuses.
    #[tokio::test]
    async fn quorum_met_with_one_failure_returns_fused_answer() {
        let mut resolver = MapResolver::new();
        let (p1, _) = FakeProvider::new(
            "u1",
            Behavior::Ok {
                content: "ans1".into(),
                tokens: 10,
            },
        );
        let (p2, _) = FakeProvider::new(
            "u2",
            Behavior::Ok {
                content: "ans2".into(),
                tokens: 20,
            },
        );
        let (p3, _) = FakeProvider::new(
            "u3",
            Behavior::Ok {
                content: "ans3".into(),
                tokens: 30,
            },
        );
        let (p4, _) = FakeProvider::new("u4", Behavior::Err);
        let (agg, agg_calls) = FakeProvider::new(
            "uagg",
            Behavior::Ok {
                content: "fused".into(),
                tokens: 5,
            },
        );
        resolver.insert("a", p1);
        resolver.insert("b", p2);
        resolver.insert("c", p3);
        resolver.insert("d", p4);
        resolver.insert("agg", agg);

        let recipes = std::iter::once(recipe(
            Strategy::ConcatSynthesize,
            vec!["a", "b", "c", "d"],
            3,
        ))
        .map(|r| (r.name.clone(), r))
        .collect();
        let orch = Orchestrator::new(resolver, recipes);

        let res = orch
            .run(&ctx(5_000), "arm-e", user_request())
            .await
            .expect("quorum met");

        assert_eq!(extract_text(&res.response), "fused");
        assert_eq!(res.response.model.as_deref(), Some("moa/arm-e"));
        assert_eq!(agg_calls.load(Ordering::SeqCst), 1);
        // usage = 3 successful proposers (10/20/30) + aggregator (5) summed
        let usage = res.response.usage.unwrap();
        assert_eq!(usage["prompt_tokens"], serde_json::json!(65));
        assert_eq!(usage["completion_tokens"], serde_json::json!(65));
        assert_eq!(usage["total_tokens"], serde_json::json!(130));
        // 4 arm outcomes, one of them error
        assert_eq!(res.arms.len(), 4);
        assert_eq!(
            res.arms
                .iter()
                .filter(|a| a.status == ArmStatus::Ok)
                .count(),
            3
        );
        assert_eq!(
            res.arms
                .iter()
                .filter(|a| a.status == ArmStatus::Error)
                .count(),
            1
        );
    }

    /// 2/4 succeed, quorum=3 -> moa_quorum_failed (503).
    #[tokio::test]
    async fn quorum_not_met_returns_quorum_failed() {
        let mut resolver = MapResolver::new();
        let (p1, _) = FakeProvider::new(
            "u1",
            Behavior::Ok {
                content: "ans1".into(),
                tokens: 10,
            },
        );
        let (p2, _) = FakeProvider::new(
            "u2",
            Behavior::Ok {
                content: "ans2".into(),
                tokens: 20,
            },
        );
        let (p3, _) = FakeProvider::new("u3", Behavior::Err);
        let (p4, _) = FakeProvider::new("u4", Behavior::Err);
        let (agg, agg_calls) = FakeProvider::new(
            "uagg",
            Behavior::Ok {
                content: "fused".into(),
                tokens: 5,
            },
        );
        resolver.insert("a", p1);
        resolver.insert("b", p2);
        resolver.insert("c", p3);
        resolver.insert("d", p4);
        resolver.insert("agg", agg);

        let recipes = std::iter::once(recipe(
            Strategy::ConcatSynthesize,
            vec!["a", "b", "c", "d"],
            3,
        ))
        .map(|r| (r.name.clone(), r))
        .collect();
        let orch = Orchestrator::new(resolver, recipes);

        let err = orch
            .run(&ctx(5_000), "arm-e", user_request())
            .await
            .expect_err("quorum not met");
        match err {
            Error::MoaQuorumFailed {
                succeeded,
                required,
            } => {
                assert_eq!(succeeded, 2);
                assert_eq!(required, 3);
            }
            other => panic!("expected MoaQuorumFailed, got {other:?}"),
        }
        assert_eq!(err.envelope().status, 503);
        // aggregator must NOT be called when quorum fails
        assert_eq!(agg_calls.load(Ordering::SeqCst), 0);
    }

    /// A slow arm times out but does not block the others; quorum still met.
    #[tokio::test]
    async fn slow_arm_times_out_without_blocking_others() {
        let mut resolver = MapResolver::new();
        let (p1, _) = FakeProvider::new(
            "u1",
            Behavior::Ok {
                content: "fast1".into(),
                tokens: 10,
            },
        );
        let (p2, _) = FakeProvider::new(
            "u2",
            Behavior::Ok {
                content: "fast2".into(),
                tokens: 10,
            },
        );
        // Slow arm sleeps far longer than arm_timeout_ms (200).
        let (p3, _) = FakeProvider::new("u3", Behavior::Slow { ms: 5_000 });
        let (agg, _) = FakeProvider::new(
            "uagg",
            Behavior::Ok {
                content: "fused".into(),
                tokens: 5,
            },
        );
        resolver.insert("a", p1);
        resolver.insert("b", p2);
        resolver.insert("c", p3);
        resolver.insert("agg", agg);

        let recipes = std::iter::once(recipe(Strategy::ConcatSynthesize, vec!["a", "b", "c"], 2))
            .map(|r| (r.name.clone(), r))
            .collect();
        let orch = Orchestrator::new(resolver, recipes);

        let start = Instant::now();
        let res = orch
            .run(&ctx(10_000), "arm-e", user_request())
            .await
            .expect("quorum met despite slow arm");
        // Completes well under the 5s slow-arm sleep (arm timeout is 200ms).
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "slow arm blocked the fan-out"
        );
        assert_eq!(extract_text(&res.response), "fused");
        let timed_out = res
            .arms
            .iter()
            .filter(|a| a.status == ArmStatus::Timeout)
            .count();
        assert_eq!(timed_out, 1);
    }

    /// Dropping the run future cancels in-flight arms (no spawn => structured
    /// cancellation). We assert the slow arm never completes its sleep.
    #[tokio::test]
    async fn dropping_run_cancels_in_flight_arms() {
        use std::sync::atomic::AtomicBool;

        // A provider that flips a flag only if it runs to completion.
        struct TrackingSlow {
            id: String,
            completed: Arc<AtomicBool>,
        }
        #[async_trait]
        impl Provider for TrackingSlow {
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
                tokio::time::sleep(Duration::from_secs(10)).await;
                self.completed.store(true, Ordering::SeqCst);
                Ok(completion("late", Some(1)))
            }
        }

        let completed = Arc::new(AtomicBool::new(false));
        let mut resolver = MapResolver::new();
        resolver.insert(
            "a",
            Arc::new(TrackingSlow {
                id: "u1".into(),
                completed: completed.clone(),
            }) as Arc<dyn Provider>,
        );
        let (agg, _) = FakeProvider::new(
            "uagg",
            Behavior::Ok {
                content: "fused".into(),
                tokens: 1,
            },
        );
        resolver.insert("agg", agg);

        // Long arm timeout so the arm would otherwise keep running.
        let mut r = recipe(Strategy::ConcatSynthesize, vec!["a"], 1);
        r.arm_timeout_ms = 60_000;
        let recipes = std::iter::once(r).map(|r| (r.name.clone(), r)).collect();
        let orch = Orchestrator::new(resolver, recipes);

        // Race the run against a short timeout, then drop the run future.
        let rctx = ctx(60_000);
        let fut = orch.run(&rctx, "arm-e", user_request());
        let raced = tokio::time::timeout(Duration::from_millis(100), fut).await;
        assert!(raced.is_err(), "run should still be pending (arm sleeping)");
        // Give any leaked task a moment; the arm must NOT have completed because
        // dropping the run future cancels it (no tokio::spawn anywhere).
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !completed.load(Ordering::SeqCst),
            "arm kept running after the run future was dropped (cancellation broken)"
        );
    }

    /// usage summing tolerates a proposer that omitted its usage object.
    #[tokio::test]
    async fn usage_sum_tolerates_missing_usage() {
        let mut resolver = MapResolver::new();
        let (p1, _) = FakeProvider::new(
            "u1",
            Behavior::Ok {
                content: "ans1".into(),
                tokens: 10,
            },
        );
        let (p2, _) = FakeProvider::new(
            "u2",
            Behavior::OkNoUsage {
                content: "ans2".into(),
            },
        );
        let (agg, _) = FakeProvider::new(
            "uagg",
            Behavior::Ok {
                content: "fused".into(),
                tokens: 5,
            },
        );
        resolver.insert("a", p1);
        resolver.insert("b", p2);
        resolver.insert("agg", agg);

        let recipes = std::iter::once(recipe(Strategy::ConcatSynthesize, vec!["a", "b"], 2))
            .map(|r| (r.name.clone(), r))
            .collect();
        let orch = Orchestrator::new(resolver, recipes);
        let res = orch
            .run(&ctx(5_000), "arm-e", user_request())
            .await
            .unwrap();
        let usage = res.response.usage.unwrap();
        // p1(10) + agg(5) = 15 (p2 contributed nothing)
        assert_eq!(usage["prompt_tokens"], serde_json::json!(15));
        // one proposer arm flagged usage missing
        assert_eq!(res.arms.iter().filter(|a| !a.usage_present).count(), 1);
    }

    /// quorum-judge resolves and the judge receives all successful candidates.
    #[tokio::test]
    async fn quorum_judge_receives_all_candidates() {
        // A judge that records how many CANDIDATE blocks it saw.
        struct CountingJudge {
            id: String,
            seen: Arc<AtomicUsize>,
        }
        #[async_trait]
        impl Provider for CountingJudge {
            fn upstream_id(&self) -> &str {
                &self.id
            }
            async fn passthrough(&self, _: &ReqCtx, _: bytes::Bytes) -> Result<RawResponse> {
                unreachable!()
            }
            async fn passthrough_stream(&self, _: &ReqCtx, _: bytes::Bytes) -> Result<RawResponse> {
                unreachable!()
            }
            async fn chat(&self, _: &ReqCtx, req: ChatRequest) -> Result<ChatResponse> {
                let user = req.messages[1]
                    .content
                    .as_ref()
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .to_string();
                let n = user.matches("<<<CANDIDATE ").count();
                self.seen.store(n, Ordering::SeqCst);
                Ok(completion("best", Some(2)))
            }
        }

        let seen = Arc::new(AtomicUsize::new(0));
        let mut resolver = MapResolver::new();
        let (p1, _) = FakeProvider::new(
            "u1",
            Behavior::Ok {
                content: "ans1".into(),
                tokens: 1,
            },
        );
        let (p2, _) = FakeProvider::new(
            "u2",
            Behavior::Ok {
                content: "ans2".into(),
                tokens: 1,
            },
        );
        let (p3, _) = FakeProvider::new(
            "u3",
            Behavior::Ok {
                content: "ans3".into(),
                tokens: 1,
            },
        );
        resolver.insert("a", p1);
        resolver.insert("b", p2);
        resolver.insert("c", p3);
        resolver.insert(
            "agg",
            Arc::new(CountingJudge {
                id: "uagg".into(),
                seen: seen.clone(),
            }) as Arc<dyn Provider>,
        );

        let recipes = std::iter::once(recipe(Strategy::QuorumJudge, vec!["a", "b", "c"], 2))
            .map(|r| (r.name.clone(), r))
            .collect();
        let orch = Orchestrator::new(resolver, recipes);
        let res = orch
            .run(&ctx(5_000), "arm-e", user_request())
            .await
            .unwrap();
        assert_eq!(extract_text(&res.response), "best");
        assert_eq!(seen.load(Ordering::SeqCst), 3, "judge saw all 3 candidates");
    }

    /// Unknown recipe -> model_not_found (404).
    #[tokio::test]
    async fn unknown_recipe_is_model_not_found() {
        let resolver = MapResolver::new();
        let orch = Orchestrator::new(resolver, std::collections::HashMap::new());
        let err = orch
            .run(&ctx(1_000), "ghost", user_request())
            .await
            .expect_err("unknown recipe");
        assert!(matches!(err, Error::ModelNotFound { .. }));
        assert_eq!(err.envelope().status, 404);
    }
}
