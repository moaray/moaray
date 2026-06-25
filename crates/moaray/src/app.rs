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
#[derive(Clone)]
pub struct ServerCtx {
    pub state: AppState,
    pub metrics: PrometheusHandle,
    pub request_timeout: Duration,
    pub max_body_bytes: usize,
    /// Emit the optional `moaray` MoA debug extension field. Off in prod.
    pub moa_expose_metadata: bool,
}

/// Build the axum router with all middleware applied.
pub fn build_router(ctx: ServerCtx) -> Router {
    let body_limit = ctx.max_body_bytes;
    Router::new()
        .route("/v1/chat/completions", post(chat_completions))
        .route("/v1/models", get(list_models))
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics_handler))
        .layer(axum::extract::DefaultBodyLimit::max(body_limit))
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
    },
    Moa {
        caller_key_id: String,
        recipe: String,
        raw: Bytes,
        model: String,
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
        } => passthrough(ctx, request_id, auth, raw, model, stream, started, provider).await,
        Routed::Moa {
            caller_key_id,
            recipe,
            raw,
            model,
        } => {
            let runtime = ctx.state.runtime.load_full();
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
    let rt = ctx.state.runtime();
    let auth = authenticate(&rt.config.keys, &token)?;

    // Inbound per-key rate limit (429 rate_limited). Checked right after auth and
    // before any body work, so an over-rate caller fails fast and cheap.
    ctx.state.stateful.check_key_limit(&auth.key_id)?;

    // Read the body (bounded by DefaultBodyLimit; over-limit yields 413).
    let body = req.into_body();
    let raw = match axum::body::to_bytes(body, ctx.max_body_bytes).await {
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
    request_id: String,
    auth: crate::auth::AuthContext,
    raw: Bytes,
    model: String,
    stream: bool,
    started: Instant,
    provider: std::sync::Arc<dyn moaray_core::provider::Provider>,
) -> Result<Response, ApiError> {
    let rctx = ReqCtx {
        request_id,
        deadline: Instant::now() + ctx.request_timeout,
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
    let result = match tokio::time::timeout(ctx.request_timeout, call).await {
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
            Err(e.into())
        }
    }
}

/// MoA path: parse the full request, run the orchestrator, render the single
/// fused completion (usage summed), record per-arm metrics, and — only when the
/// config toggle is on — attach the `moaray` debug extension field.
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
        request_id,
        deadline: Instant::now() + ctx.request_timeout,
        caller_key_id,
        model: model.clone(),
    };

    let result = runtime.orchestrator.run(&rctx, &recipe, request).await;

    match result {
        Ok(moa) => {
            // Per-arm metrics (proposers + aggregator); low-cardinality labels.
            for arm in &moa.arms {
                record_moa_arm(
                    &arm.model,
                    &arm.upstream_id,
                    arm.status.as_str(),
                    arm.latency_ms as f64 / 1000.0,
                );
            }
            record_moa_arm(
                &moa.aggregator.model,
                &moa.aggregator.upstream_id,
                moa.aggregator.status.as_str(),
                moa.aggregator.latency_ms as f64 / 1000.0,
            );
            record_request(
                RequestPath::Moa,
                &model,
                200,
                started.elapsed().as_secs_f64(),
            );

            let mut body = serde_json::to_value(&moa.response).unwrap_or_else(|_| json!({}));
            if ctx.moa_expose_metadata {
                if let Some(obj) = body.as_object_mut() {
                    obj.insert("moaray".to_string(), moa_metadata(&moa));
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

/// Build the optional `moaray` extension object: per-arm metadata (model,
/// upstream_id, latency, status). No secrets, no raw upstream error bodies.
fn moa_metadata(moa: &moaray_moa::MoaResult) -> Value {
    let arm_json = |a: &moaray_moa::ArmOutcome| {
        json!({
            "model": a.model,
            "upstream_id": a.upstream_id,
            "latency_ms": a.latency_ms,
            "status": a.status.as_str(),
            "usage_present": a.usage_present,
        })
    };
    let arms: Vec<Value> = moa.arms.iter().map(arm_json).collect();
    json!({
        "arms": arms,
        "aggregator": arm_json(&moa.aggregator),
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
