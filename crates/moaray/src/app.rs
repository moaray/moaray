//! axum application: router, foundational-defaults middleware, and handlers.
//!
//! Foundational defaults wired here (plan §4): request-id injection +
//! propagation, per-request timeout, and an inbound body-size limit. Auth and
//! the per-key allowlist run inside the chat handler so that `/healthz` and
//! `/metrics` stay unauthenticated.

use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use bytes::Bytes;
use metrics_exporter_prometheus::PrometheusHandle;
use moaray_core::error::Error;
use moaray_core::provider::ReqCtx;
use moaray_core::router::{route, RouteTarget};
use moaray_core::usage::{compute_cost, UsageArm, UsagePath, UsageRecord, UsageStatus};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::auth::{authenticate, parse_bearer};
use crate::http_error::ApiError;
use crate::observe::{
    record_moa_arm, record_rejection, record_request, render_metrics, RequestPath,
};
use crate::runtime::AppState;

const REQUEST_ID_HEADER: &str = "x-request-id";

/// Everything the server needs at runtime beyond [`AppState`].
///
/// The three "hot" server knobs — `request_timeout_ms`, `max_body_bytes`, and
/// `moa_expose_metadata` — are deliberately **not** cached here. They are read
/// from the live config snapshot ([`AppState::runtime`]) on each request so a
/// config hot-reload takes effect immediately (P3-3 F2), instead of being frozen
/// at startup. `bind` / `port` / `shutdown_grace_ms` are NOT hot (they need a
/// restart); a reload warns and ignores changes to them (see [`crate::reload`]).
#[derive(Clone)]
pub struct ServerCtx {
    pub state: AppState,
    pub metrics: PrometheusHandle,
}

impl ServerCtx {
    /// Live per-request timeout (hot-reloadable).
    fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.state.runtime().config.server.request_timeout_ms)
    }

    /// Live inbound body cap (hot-reloadable).
    fn max_body_bytes(&self) -> usize {
        self.state.runtime().config.server.max_body_bytes
    }

    /// Live `moaray` debug-extension toggle (hot-reloadable).
    fn moa_expose_metadata(&self) -> bool {
        self.state.runtime().config.server.moa_expose_metadata
    }
}

/// Build the axum router with all middleware applied.
///
/// Note: the inbound body cap is enforced per-request from the **live** config in
/// [`pre_route`] (via `to_bytes` with a hard bound — no unbounded buffering), not
/// by a startup-frozen `DefaultBodyLimit` tower layer. This is what makes
/// `max_body_bytes` hot-reloadable (P3-3 F2): a startup-bound layer would cap a
/// live increase and silently ignore the new value.
pub fn build_router(ctx: ServerCtx) -> Router {
    Router::new()
        .route("/v1/chat/completions", post(chat_completions))
        .route("/v1/models", get(list_models))
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics_handler))
        .layer(middleware::from_fn(request_id_mw))
        .with_state(ctx)
}

/// request-id middleware: accept an inbound `x-request-id` or mint a UUID, store
/// it in request extensions for handlers, and echo it on the response.
async fn request_id_mw(mut req: Request, next: Next) -> Response {
    let rid = req
        .headers()
        .get(REQUEST_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    req.extensions_mut().insert(RequestId(rid.clone()));
    let mut resp = next.run(req).await;
    if let Ok(val) = HeaderValue::from_str(&rid) {
        resp.headers_mut().insert(REQUEST_ID_HEADER, val);
    }
    resp
}

#[derive(Clone)]
struct RequestId(String);

async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn metrics_handler(State(ctx): State<ServerCtx>) -> impl IntoResponse {
    let body = render_metrics(&ctx.metrics);
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        body,
    )
}

/// `GET /v1/models` — filtered by the caller's allowlist.
async fn list_models(State(ctx): State<ServerCtx>, req: Request) -> Result<Response, ApiError> {
    let token = parse_bearer(
        req.headers()
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok()),
    )?;
    let rt = ctx.state.runtime();
    let auth = authenticate(&rt.config.keys, token)?;

    let data: Vec<Value> = rt
        .config
        .models
        .keys()
        .filter(|m| auth.allows(m))
        .map(|m| {
            json!({
                "id": m,
                "object": "model",
                "owned_by": "moaray",
            })
        })
        .collect();
    Ok(axum::Json(json!({"object": "list", "data": data})).into_response())
}

/// A request that passed every pre-routing gate and is ready to execute.
enum Routed {
    Passthrough {
        auth: crate::auth::AuthContext,
        raw: Bytes,
        model: String,
        stream: bool,
        provider: std::sync::Arc<dyn moaray_core::provider::Provider>,
        /// The runtime snapshot this request was routed against. Carried so the
        /// per-model price used for the usage row comes from the SAME config the
        /// provider was resolved from — not a live snapshot a hot reload may have
        /// swapped mid-request (mirrors `Routed::Moa.runtime`, codex C1).
        runtime: std::sync::Arc<crate::runtime::Runtime>,
    },
    Moa {
        caller_key_id: String,
        recipe: String,
        raw: Bytes,
        model: String,
        /// The exact runtime snapshot this request was authorized + routed
        /// against. Carried through so MoA execution uses the same orchestrator /
        /// provider set even if a config hot-reload swaps the runtime in between —
        /// the request can never run a recipe/provider set it was not routed for
        /// (the passthrough path already carries its resolved provider for the same
        /// reason).
        runtime: std::sync::Arc<crate::runtime::Runtime>,
    },
}

/// `POST /v1/chat/completions` — auth, route, passthrough (stream or not).
///
/// Pre-routing gates (auth / per-key limit / body / parse / allowlist / route)
/// run in [`pre_route`]; any rejection there is recorded under
/// `path="pre_routing"` so inbound protections (notably the per-key 429) are
/// visible on `/metrics` (plan P3-1). Once routed, the passthrough / MoA paths
/// record their own outcome.
async fn chat_completions(
    State(ctx): State<ServerCtx>,
    req: Request,
) -> Result<Response, ApiError> {
    let started = Instant::now();
    let request_id = req
        .extensions()
        .get::<RequestId>()
        .map(|r| r.0.clone())
        .unwrap_or_default();

    let routed = match pre_route(&ctx, req).await {
        Ok(r) => r,
        Err(e) => {
            // A protective rejection before any upstream path was chosen — count
            // it so /metrics reflects the inbound limit/auth/body checks.
            record_rejection(e.envelope().status);
            return Err(e.into());
        }
    };

    match routed {
        Routed::Passthrough {
            auth,
            raw,
            model,
            stream,
            provider,
            runtime,
        } => {
            passthrough(
                ctx, runtime, request_id, auth, raw, model, stream, started, provider,
            )
            .await
        }
        Routed::Moa {
            caller_key_id,
            recipe,
            raw,
            model,
            runtime,
        } => {
            run_moa(
                ctx,
                runtime,
                request_id,
                caller_key_id,
                recipe,
                raw,
                model,
                started,
            )
            .await
        }
    }
}

/// Run every pre-routing gate and resolve the request to a [`Routed`] decision.
/// Returns the canonical [`Error`] on any rejection (recorded by the caller).
async fn pre_route(ctx: &ServerCtx, req: Request) -> Result<Routed, Error> {
    // auth first (401 on missing/invalid) — own the token before consuming req.
    let token = parse_bearer(
        req.headers()
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok()),
    )?
    .to_string();

    // Authenticate BEFORE touching the body so an invalid key fails closed with
    // 401 rather than doing body work / returning 413 for an oversized payload.
    // Pin ONE runtime snapshot (Arc) for the whole pre-route + (for MoA) execution
    // so auth / allowlist / routing / orchestration all see the same config even if
    // a hot-reload swaps the runtime mid-request.
    let rt = ctx.state.runtime.load_full();
    let auth = authenticate(&rt.config.keys, &token)?;

    // Inbound per-key rate limit (429 rate_limited). Checked right after auth and
    // before any body work, so an over-rate caller fails fast and cheap.
    ctx.state.stateful.check_key_limit(&auth.key_id)?;

    // Read the body (bounded by the live max_body_bytes; over-limit yields 413).
    let body = req.into_body();
    let raw = match axum::body::to_bytes(body, ctx.max_body_bytes()).await {
        Ok(b) => b,
        Err(_) => return Err(Error::PayloadTooLarge),
    };

    // Parse just enough to learn the model + stream flag (without losing data).
    let parsed: Value = serde_json::from_slice(&raw)
        .map_err(|e| Error::BadRequest(format!("invalid JSON: {e}")))?;
    let model = parsed
        .get("model")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::BadRequest("missing `model`".into()))?
        .to_string();
    let stream = parsed
        .get("stream")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    // allowlist (403) — checked before routing so unauthorized models can't probe.
    if !auth.allows(&model) {
        return Err(Error::ModelNotAllowed { model });
    }

    // route by model name
    match route(&model, |m| rt.config.is_known_model(m)) {
        RouteTarget::Passthrough { model } => {
            let provider = rt.provider(&model).ok_or_else(|| Error::ModelNotFound {
                model: model.clone(),
            })?;
            Ok(Routed::Passthrough {
                auth,
                raw,
                model,
                stream,
                provider,
                runtime: rt.clone(),
            })
        }
        RouteTarget::Moa { recipe } => {
            // v1 MoA is non-streaming only (DESIGN §4): reject stream up front.
            if stream {
                return Err(Error::MoaStreamingUnsupported);
            }
            Ok(Routed::Moa {
                caller_key_id: auth.key_id.clone(),
                recipe,
                raw,
                model,
                runtime: rt.clone(),
            })
        }
        RouteTarget::Unknown { model } => Err(Error::ModelNotFound { model }),
    }
}

/// Passthrough path (extracted from the router branch): forward raw bytes to the
/// resolved upstream under the per-request timeout.
#[allow(clippy::too_many_arguments)]
async fn passthrough(
    ctx: ServerCtx,
    runtime: std::sync::Arc<crate::runtime::Runtime>,
    request_id: String,
    auth: crate::auth::AuthContext,
    raw: Bytes,
    model: String,
    stream: bool,
    started: Instant,
    provider: std::sync::Arc<dyn moaray_core::provider::Provider>,
) -> Result<Response, ApiError> {
    // Read the per-request timeout from the live config snapshot (hot-reloadable).
    let request_timeout = ctx.request_timeout();
    let rctx = ReqCtx {
        request_id: request_id.clone(),
        deadline: Instant::now() + request_timeout,
        caller_key_id: auth.key_id.clone(),
        model: model.clone(),
    };

    let raw_body = Bytes::from(raw.to_vec());
    // Enforce the configured per-request timeout around the upstream call so a
    // stalled upstream surfaces as 504 upstream_timeout. For streaming this
    // bounds time-to-first-response (headers/connect); the relayed body stream
    // then flows under client backpressure.
    let call = async {
        if stream {
            provider.passthrough_stream(&rctx, raw_body).await
        } else {
            provider.passthrough(&rctx, raw_body).await
        }
    };
    let result = match tokio::time::timeout(request_timeout, call).await {
        Ok(r) => r,
        Err(_) => Err(Error::UpstreamTimeout),
    };

    match result {
        Ok(raw_resp) => {
            let status = raw_resp.status;
            record_request(
                RequestPath::Passthrough,
                &model,
                status,
                started.elapsed().as_secs_f64(),
            );
            if stream {
                // Streaming usage is not tapped this period (would buffer the SSE
                // stream); make the gap observable instead of silent (Step 6b).
                metrics::counter!("moaray_usage_unaccounted_stream_total").increment(1);
            } else {
                // Non-stream: book ONE usage row from the tapped `raw_resp.usage`,
                // priced from the routed snapshot's per-model price (codex C1).
                let (price_p, price_c) = model_prices(&runtime, &model);
                let (pt, ct) = match raw_resp.usage {
                    Some((p, c)) => (Some(p), Some(c)),
                    None => (None, None),
                };
                let status_kind = usage_status(pt, ct, price_p, price_c);
                if matches!(status_kind, UsageStatus::Unpriced) {
                    metrics::counter!("moaray_usage_unpriced_total").increment(1);
                }
                let cost = compute_cost(pt, ct, price_p, price_c);
                ctx.state.usage_sink.record(UsageRecord {
                    request_id: request_id.clone(),
                    ts_unix_ms: now_unix_ms(),
                    path: UsagePath::Passthrough,
                    arm: UsageArm::Passthrough,
                    model: model.clone(),
                    upstream_id: provider.upstream_id().to_string(),
                    caller_key_id: auth.key_id.clone(),
                    prompt_tokens: pt,
                    completion_tokens: ct,
                    price_prompt_nano_per_mtok: price_p,
                    price_completion_nano_per_mtok: price_c,
                    cost_nano_usd: cost,
                    status: status_kind,
                });
            }
            Ok(into_response(raw_resp, stream))
        }
        Err(e) => {
            let status = e.envelope().status;
            record_request(
                RequestPath::Passthrough,
                &model,
                status,
                started.elapsed().as_secs_f64(),
            );
            // A failed non-stream passthrough books NO usage row (no tokens to
            // count) — intentionally asymmetric with MoA's explicit failed-arm
            // rows (plan §5). Streaming failures likewise book no row.
            Err(e.into())
        }
    }
}

/// Current wall-clock time in unix milliseconds (saturating, never negative).
fn now_unix_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Resolve the `(prompt, completion)` nano-USD-per-Mtok price for a model from a
/// pinned runtime snapshot. `(None, None)` when the model is unknown/unpriced.
fn model_prices(runtime: &crate::runtime::Runtime, model: &str) -> (Option<i64>, Option<i64>) {
    runtime
        .config
        .models
        .get(model)
        .map(|m| {
            (
                m.price_prompt_nano_per_mtok,
                m.price_completion_nano_per_mtok,
            )
        })
        .unwrap_or((None, None))
}

/// Map measured tokens + a price snapshot to a [`UsageStatus`].
///
/// `(tokens present, price present)` → `ok`; `(tokens present, price absent)` →
/// `unpriced`; `(tokens absent, _)` → `ok_no_usage`. Failed/timeout statuses are
/// set by their own call sites, not here.
fn usage_status(
    pt: Option<i64>,
    ct: Option<i64>,
    price_p: Option<i64>,
    price_c: Option<i64>,
) -> UsageStatus {
    match (
        pt.is_some() && ct.is_some(),
        price_p.is_some() && price_c.is_some(),
    ) {
        (false, _) => UsageStatus::OkNoUsage,
        (true, true) => UsageStatus::Ok,
        (true, false) => UsageStatus::Unpriced,
    }
}

/// MoA path: parse the full request, run the orchestrator, book a usage row per
/// arm (unconditionally, even on failure — so failed runs still bill the arms
/// that incurred cost), emit per-arm metrics in lockstep, then map the run's
/// outcome to the HTTP response.
#[allow(clippy::too_many_arguments)]
async fn run_moa(
    ctx: ServerCtx,
    runtime: std::sync::Arc<crate::runtime::Runtime>,
    request_id: String,
    caller_key_id: String,
    recipe: String,
    raw: Bytes,
    model: String,
    started: Instant,
) -> Result<Response, ApiError> {
    // Parse the full structured request (MoA uses the typed chat() path).
    let request: moaray_core::types::ChatRequest = serde_json::from_slice(&raw)
        .map_err(|e| Error::BadRequest(format!("invalid request: {e}")))?;

    let rctx = ReqCtx {
        request_id: request_id.clone(),
        deadline: Instant::now() + ctx.request_timeout(),
        caller_key_id: caller_key_id.clone(),
        model: model.clone(),
    };

    // The pre-fan-out ModelNotFound is the only Err here (no arm ran → book 0
    // rows, correct). Everything else is a MoaRun carrying the arm outcomes.
    let run = match runtime.orchestrator.run(&rctx, &recipe, request).await {
        Ok(run) => run,
        Err(e) => {
            let status = e.envelope().status;
            record_request(
                RequestPath::Moa,
                &model,
                status,
                started.elapsed().as_secs_f64(),
            );
            return Err(e.into());
        }
    };

    // Book a usage row + emit a per-arm metric for EVERY arm that ran — proposers
    // and the aggregator (if any) — UNCONDITIONALLY, before mapping the outcome.
    // This keeps the usage rows and the moaray_moa_arm_total metric series in
    // lockstep even for failed runs (plan P1-④ + §8③).
    let ts = now_unix_ms();
    for arm in &run.proposers {
        book_moa_arm(
            &ctx,
            &runtime,
            &request_id,
            &caller_key_id,
            &model,
            ts,
            UsageArm::Proposer,
            arm,
        );
    }
    if let Some(agg) = &run.aggregator {
        book_moa_arm(
            &ctx,
            &runtime,
            &request_id,
            &caller_key_id,
            &model,
            ts,
            UsageArm::Aggregator,
            agg,
        );
    }

    match run.outcome {
        Ok(response) => {
            record_request(
                RequestPath::Moa,
                &model,
                200,
                started.elapsed().as_secs_f64(),
            );
            let mut body = serde_json::to_value(&response).unwrap_or_else(|_| json!({}));
            if ctx.moa_expose_metadata() {
                if let Some(obj) = body.as_object_mut() {
                    obj.insert(
                        "moaray".to_string(),
                        moa_metadata(&run.proposers, run.aggregator.as_ref()),
                    );
                }
            }
            Ok(axum::Json(body).into_response())
        }
        Err(e) => {
            let status = e.envelope().status;
            record_request(
                RequestPath::Moa,
                &model,
                status,
                started.elapsed().as_secs_f64(),
            );
            Err(e.into())
        }
    }
}

/// Emit one arm's per-arm metric AND book its usage row (priced from the routed
/// snapshot). Called unconditionally for every arm that ran.
#[allow(clippy::too_many_arguments)]
fn book_moa_arm(
    ctx: &ServerCtx,
    runtime: &crate::runtime::Runtime,
    request_id: &str,
    caller_key_id: &str,
    model: &str,
    ts_unix_ms: i64,
    arm_kind: UsageArm,
    arm: &moaray_moa::ArmOutcome,
) {
    record_moa_arm(
        &arm.model,
        &arm.upstream_id,
        arm.status.as_str(),
        arm.latency_ms as f64 / 1000.0,
    );

    let (price_p, price_c) = model_prices(runtime, &arm.model);
    // Map the orchestrator ArmStatus → the persisted UsageStatus, applying the
    // unpriced override when tokens are present but no price is configured.
    let status = match arm.status {
        moaray_moa::ArmStatus::Timeout => UsageStatus::Timeout,
        moaray_moa::ArmStatus::Error => UsageStatus::Failed,
        moaray_moa::ArmStatus::Ok => {
            usage_status(arm.prompt_tokens, arm.completion_tokens, price_p, price_c)
        }
    };
    if matches!(status, UsageStatus::Unpriced) {
        metrics::counter!("moaray_usage_unpriced_total").increment(1);
    }
    // Cost is only computed for ok+priced rows; failed/timeout/unmeasured → NULL.
    let cost = compute_cost(arm.prompt_tokens, arm.completion_tokens, price_p, price_c);
    ctx.state.usage_sink.record(UsageRecord {
        request_id: request_id.to_string(),
        ts_unix_ms,
        path: UsagePath::Moa,
        arm: arm_kind,
        model: arm.model.clone(),
        upstream_id: arm.upstream_id.clone(),
        caller_key_id: caller_key_id.to_string(),
        prompt_tokens: arm.prompt_tokens,
        completion_tokens: arm.completion_tokens,
        price_prompt_nano_per_mtok: price_p,
        price_completion_nano_per_mtok: price_c,
        cost_nano_usd: cost,
        status,
    });
    let _ = model; // model (the moa/* name) is carried on the request, not the arm row
}

/// Build the optional `moaray` extension object: per-arm metadata (model,
/// upstream_id, latency, status). No secrets, no raw upstream error bodies.
fn moa_metadata(
    proposers: &[moaray_moa::ArmOutcome],
    aggregator: Option<&moaray_moa::ArmOutcome>,
) -> Value {
    let arm_json = |a: &moaray_moa::ArmOutcome| {
        json!({
            "model": a.model,
            "upstream_id": a.upstream_id,
            "latency_ms": a.latency_ms,
            "status": a.status.as_str(),
            "usage_present": a.usage_present,
        })
    };
    let arms: Vec<Value> = proposers.iter().map(arm_json).collect();
    json!({
        "arms": arms,
        "aggregator": aggregator.map(arm_json),
    })
}

/// Convert a provider [`RawResponse`] into an axum streaming response. The body
/// is always a stream, so the SSE path is never buffered.
fn into_response(raw: moaray_core::provider::RawResponse, stream: bool) -> Response {
    use futures_util::StreamExt;
    let status = StatusCode::from_u16(raw.status).unwrap_or(StatusCode::OK);
    let content_type = raw.content_type.clone().unwrap_or_else(|| {
        if stream {
            "text/event-stream".to_string()
        } else {
            "application/json".to_string()
        }
    });
    let mapped = raw
        .body
        .map(|chunk| chunk.map_err(|e| std::io::Error::other(e.envelope().message)));
    let body = Body::from_stream(mapped);
    let mut resp = Response::new(body);
    *resp.status_mut() = status;
    if let Ok(ct) = HeaderValue::from_str(&content_type) {
        resp.headers_mut().insert(header::CONTENT_TYPE, ct);
    }
    if stream {
        resp.headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    }
    resp
}
