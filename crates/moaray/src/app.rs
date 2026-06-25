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
use crate::observe::{record_request, render_metrics};
use crate::runtime::AppState;

const REQUEST_ID_HEADER: &str = "x-request-id";

/// Everything the server needs at runtime beyond [`AppState`].
#[derive(Clone)]
pub struct ServerCtx {
    pub state: AppState,
    pub metrics: PrometheusHandle,
    pub request_timeout: Duration,
    pub max_body_bytes: usize,
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

/// `POST /v1/chat/completions` — auth, route, passthrough (stream or not).
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

    // auth first (401 on missing/invalid) — own the token before consuming req.
    let token = parse_bearer(
        req.headers()
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok()),
    )?
    .to_string();

    // Read the body (bounded by DefaultBodyLimit; over-limit yields 413 below).
    let body = req.into_body();
    let raw = match axum::body::to_bytes(body, ctx.max_body_bytes).await {
        Ok(b) => b,
        Err(_) => return Err(Error::PayloadTooLarge.into()),
    };

    let rt = ctx.state.runtime();
    let auth = authenticate(&rt.config.keys, &token)?;

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
        return Err(Error::ModelNotAllowed { model }.into());
    }

    // route by model name
    let target = route(&model, |m| rt.config.is_known_model(m));
    let provider = match target {
        RouteTarget::Passthrough { model } => {
            rt.provider(&model).ok_or(Error::ModelNotFound { model })?
        }
        RouteTarget::Moa { .. } => {
            // Phase 2 wires the orchestrator; v1 passthrough build rejects clearly.
            return Err(Error::Unsupported("MoA mode arrives in Phase 2".into()).into());
        }
        RouteTarget::Unknown { model } => return Err(Error::ModelNotFound { model }.into()),
    };

    let rctx = ReqCtx {
        request_id: request_id.clone(),
        deadline: Instant::now() + ctx.request_timeout,
        caller_key_id: auth.key_id.clone(),
        model: model.clone(),
    };

    let raw_body = Bytes::from(raw.to_vec());
    let result = if stream {
        provider.passthrough_stream(&rctx, raw_body).await
    } else {
        provider.passthrough(&rctx, raw_body).await
    };

    match result {
        Ok(raw_resp) => {
            let status = raw_resp.status;
            record_request(&model, status, started.elapsed().as_secs_f64());
            Ok(into_response(raw_resp, stream))
        }
        Err(e) => {
            let status = e.envelope().status;
            record_request(&model, status, started.elapsed().as_secs_f64());
            Err(e.into())
        }
    }
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
