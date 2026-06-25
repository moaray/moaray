//! The `Provider` trait — the dual-path abstraction over an upstream.
//!
//! Two paths, deliberately separate:
//!
//! - **Passthrough** (`passthrough` / `passthrough_stream`): forwards the raw
//!   request bytes to the upstream and relays the raw response. It does NOT
//!   parse business fields, so unknown fields, vendor extensions, and usage
//!   chunks survive untouched. This is the sub-millisecond gateway path.
//! - **Structured** (`chat`): parses into `ChatResponse` for the MoA
//!   orchestrator, which needs typed access to fuse/judge answers. (Wired up in
//!   Phase 2; the trait method exists now so providers implement both paths.)
//!
//! Every call carries a [`ReqCtx`] with request id, deadline, caller key id, and
//! the resolved model — never any secret material.

use std::pin::Pin;
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;

use crate::error::Result;
use crate::types::{ChatRequest, ChatResponse};

/// Per-request context threaded through every provider call.
///
/// Holds only non-secret routing/observability data. The caller's API key is
/// represented by an opaque `caller_key_id` (e.g. a config key label or hash
/// prefix), never the token itself.
#[derive(Debug, Clone)]
pub struct ReqCtx {
    /// Correlation id, propagated to the upstream and echoed to the client.
    pub request_id: String,
    /// Hard deadline for this request (per-request timeout).
    pub deadline: Instant,
    /// Opaque identifier for the calling key (non-secret label).
    pub caller_key_id: String,
    /// Resolved model name for this request.
    pub model: String,
}

/// A raw passthrough response: status + headers-relevant content type + a byte
/// stream body. The body is a stream so the streaming path never buffers.
pub struct RawResponse {
    /// Upstream HTTP status code.
    pub status: u16,
    /// `content-type` to relay (e.g. `application/json` or `text/event-stream`).
    pub content_type: Option<String>,
    /// Response body as a byte stream (single chunk for non-stream).
    pub body: ByteStream,
}

impl std::fmt::Debug for RawResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawResponse")
            .field("status", &self.status)
            .field("content_type", &self.content_type)
            .field("body", &"<stream>")
            .finish()
    }
}

/// Boxed byte stream used for streaming bodies.
pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>;

/// An upstream provider (openai-compat, anthropic, ...).
#[async_trait]
pub trait Provider: Send + Sync {
    /// Stable provider/upstream identifier (used for metrics + stateful keying).
    fn upstream_id(&self) -> &str;

    /// Non-streaming passthrough: forward raw request bytes, return raw bytes.
    /// `raw_body` is the exact inbound JSON; the provider only injects auth.
    async fn passthrough(&self, ctx: &ReqCtx, raw_body: Bytes) -> Result<RawResponse>;

    /// Streaming passthrough: forward raw request bytes, relay the upstream SSE
    /// byte stream frame-by-frame (no full-body buffering).
    async fn passthrough_stream(&self, ctx: &ReqCtx, raw_body: Bytes) -> Result<RawResponse>;

    /// Structured path for MoA: parse into a typed response. Wired in Phase 2.
    async fn chat(&self, ctx: &ReqCtx, req: ChatRequest) -> Result<ChatResponse>;
}
