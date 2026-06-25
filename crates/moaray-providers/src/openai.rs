//! OpenAI-compatible provider adapter.
//!
//! Passthrough is byte-exact: the inbound request body is forwarded verbatim to
//! `{base_url}/v1/chat/completions` with only the `Authorization` header
//! injected. Unknown fields, vendor extensions, and streamed usage chunks are
//! never parsed, so nothing is lost. The structured `chat()` path parses into
//! typed responses for the Phase 2 MoA orchestrator.
//!
//! The upstream API key comes from the environment and is never logged.

use async_trait::async_trait;
use bytes::Bytes;
use moaray_core::error::{Error, Result};
use moaray_core::provider::{Provider, RawResponse, ReqCtx};
use moaray_core::types::{ChatRequest, ChatResponse};
use reqwest::Client;

use crate::common::{
    collect_response, map_reqwest_error, map_upstream_status, stream_response, REQUEST_ID_HEADER,
};

/// An OpenAI-compatible upstream.
pub struct OpenAiProvider {
    upstream_id: String,
    base_url: String,
    /// Resolved upstream API key (secret, never logged).
    api_key: String,
    client: Client,
}

impl OpenAiProvider {
    /// Build from resolved config values. `api_key` is the secret resolved from
    /// the configured env var by the caller.
    pub fn new(
        upstream_id: impl Into<String>,
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        client: Client,
    ) -> Self {
        Self {
            upstream_id: upstream_id.into(),
            base_url: base_url.into(),
            api_key: api_key.into(),
            client,
        }
    }

    fn endpoint(&self) -> String {
        format!(
            "{}/v1/chat/completions",
            self.base_url.trim_end_matches('/')
        )
    }

    async fn send(&self, ctx: &ReqCtx, raw_body: Bytes) -> Result<reqwest::Response> {
        self.client
            .post(self.endpoint())
            .bearer_auth(&self.api_key)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(REQUEST_ID_HEADER, &ctx.request_id)
            .body(raw_body)
            .send()
            .await
            .map_err(|e| map_reqwest_error(&e))
    }
}

#[async_trait]
impl Provider for OpenAiProvider {
    fn upstream_id(&self) -> &str {
        &self.upstream_id
    }

    async fn passthrough(&self, ctx: &ReqCtx, raw_body: Bytes) -> Result<RawResponse> {
        let resp = self.send(ctx, raw_body).await?;
        // Non-2xx maps to the canonical error matrix; the handler renders the
        // envelope. (We surface a typed error rather than relaying the upstream
        // error body to avoid leaking provider internals.)
        map_upstream_status(resp.status().as_u16())?;
        collect_response(resp).await
    }

    async fn passthrough_stream(&self, ctx: &ReqCtx, raw_body: Bytes) -> Result<RawResponse> {
        let resp = self.send(ctx, raw_body).await?;
        map_upstream_status(resp.status().as_u16())?;
        // Relay the SSE byte stream frame-by-frame — no buffering.
        Ok(stream_response(resp))
    }

    async fn chat(&self, ctx: &ReqCtx, req: ChatRequest) -> Result<ChatResponse> {
        // Structured path: serialize the typed request, post, parse typed back.
        let body = serde_json::to_vec(&req).map_err(|_| Error::Internal)?;
        let resp = self.send(ctx, Bytes::from(body)).await?;
        map_upstream_status(resp.status().as_u16())?;
        let bytes = resp.bytes().await.map_err(|e| map_reqwest_error(&e))?;
        serde_json::from_slice(&bytes).map_err(|_| Error::UpstreamError)
    }
}
