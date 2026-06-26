//! Anthropic Messages API provider adapter.
//!
//! Callers always speak OpenAI to moaray; this adapter translates to/from the
//! Anthropic `/v1/messages` API on every path:
//! - passthrough (non-stream): OpenAI req -> Anthropic req -> Anthropic resp ->
//!   OpenAI resp bytes.
//! - passthrough_stream: OpenAI req -> Anthropic streaming req -> translate the
//!   Anthropic SSE events into OpenAI `chat.completion.chunk` frames + [DONE].
//! - chat(): structured path for MoA (Phase 2).
//!
//! Headers: `x-api-key`, `anthropic-version: 2023-06-01`, JSON content-type.
//! The upstream key is never logged. v1 is text-only: tool_use / non-text
//! blocks STOP with [`Error::Unsupported`].

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use moaray_core::error::{Error, Result};
use moaray_core::provider::{ByteStream, Provider, RawResponse, ReqCtx};
use moaray_core::types::{ChatRequest, ChatResponse};
use reqwest::Client;
use serde_json::Value;

use crate::anthropic_map::{
    anthropic_to_openai, openai_to_anthropic, usage_tokens, ANTHROPIC_VERSION,
};
use crate::anthropic_sse::translate;
use crate::common::{map_reqwest_error, map_upstream_status, REQUEST_ID_HEADER};

/// An Anthropic Messages upstream.
pub struct AnthropicProvider {
    upstream_id: String,
    base_url: String,
    api_key: String,
    default_max_tokens: u32,
    client: Client,
}

impl AnthropicProvider {
    /// Build from resolved config values.
    pub fn new(
        upstream_id: impl Into<String>,
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        default_max_tokens: u32,
        client: Client,
    ) -> Self {
        Self {
            upstream_id: upstream_id.into(),
            base_url: base_url.into(),
            api_key: api_key.into(),
            default_max_tokens,
            client,
        }
    }

    fn endpoint(&self) -> String {
        format!("{}/v1/messages", self.base_url.trim_end_matches('/'))
    }

    /// Parse inbound OpenAI JSON and build the Anthropic request body. `stream`
    /// is forced to the desired value so the two paths control framing.
    fn build_body(&self, raw_body: &Bytes, stream: bool) -> Result<(String, Value)> {
        let openai: Value = serde_json::from_slice(raw_body)
            .map_err(|e| Error::BadRequest(format!("invalid JSON: {e}")))?;
        let model = openai
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let mut anthropic = openai_to_anthropic(&openai, self.default_max_tokens)?;
        anthropic["stream"] = Value::Bool(stream);
        Ok((model, anthropic))
    }

    async fn send(&self, ctx: &ReqCtx, body: &Value) -> Result<reqwest::Response> {
        self.client
            .post(self.endpoint())
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", ANTHROPIC_VERSION)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(REQUEST_ID_HEADER, &ctx.request_id)
            .json(body)
            .send()
            .await
            .map_err(|e| map_reqwest_error(&e))
    }
}

#[async_trait]
impl Provider for AnthropicProvider {
    fn upstream_id(&self) -> &str {
        &self.upstream_id
    }

    async fn passthrough(&self, ctx: &ReqCtx, raw_body: Bytes) -> Result<RawResponse> {
        let (model, body) = self.build_body(&raw_body, false)?;
        let resp = self.send(ctx, &body).await?;
        let status = resp.status().as_u16();
        map_upstream_status(status)?;
        let bytes = resp.bytes().await.map_err(|e| map_reqwest_error(&e))?;
        let anthropic: Value = serde_json::from_slice(&bytes).map_err(|_| Error::UpstreamError)?;
        // Usage tap (raw-key absence mitigation, DP2): `anthropic_to_openai`
        // ALWAYS emits a `usage` object, defaulting a missing upstream usage to
        // `0,0` — so the translated body cannot tell "absent" from "genuinely
        // zero". Judge absence on the RAW upstream key here: `usage` present =>
        // take its translated `(input,output)` tokens; absent => `None` (the app
        // maps that to `ok_no_usage`). We never inspect/rewrite the `0,0` values
        // themselves, so genuinely-zero rows are preserved.
        let usage = anthropic.get("usage").map(|u| {
            let (p, c) = usage_tokens(Some(u));
            (p as i64, c as i64)
        });
        let openai = anthropic_to_openai(&anthropic, &model)?;
        let out = serde_json::to_vec(&openai).map_err(|_| Error::Internal)?;
        let body: ByteStream = Box::pin(futures_util::stream::once(
            async move { Ok(Bytes::from(out)) },
        ));
        Ok(RawResponse {
            status,
            content_type: Some("application/json".to_string()),
            body,
            usage,
        })
    }

    async fn passthrough_stream(&self, ctx: &ReqCtx, raw_body: Bytes) -> Result<RawResponse> {
        let (model, body) = self.build_body(&raw_body, true)?;
        let resp = self.send(ctx, &body).await?;
        let status = resp.status().as_u16();
        map_upstream_status(status)?;
        let upstream = resp.bytes_stream().map(|item| {
            item.map_err(|e| {
                if e.is_timeout() {
                    Error::UpstreamTimeout
                } else {
                    Error::UpstreamError
                }
            })
        });
        let translated = translate(upstream, model);
        Ok(RawResponse {
            status,
            content_type: Some("text/event-stream".to_string()),
            body: Box::pin(translated),
            // Streaming path never taps usage (would buffer the SSE stream).
            usage: None,
        })
    }

    async fn chat(&self, ctx: &ReqCtx, req: ChatRequest) -> Result<ChatResponse> {
        let openai = serde_json::to_value(&req).map_err(|_| Error::Internal)?;
        let model = req.model.clone();
        let mut body = openai_to_anthropic(&openai, self.default_max_tokens)?;
        body["stream"] = Value::Bool(false);
        let resp = self.send(ctx, &body).await?;
        map_upstream_status(resp.status().as_u16())?;
        let bytes = resp.bytes().await.map_err(|e| map_reqwest_error(&e))?;
        let anthropic: Value = serde_json::from_slice(&bytes).map_err(|_| Error::UpstreamError)?;
        // Raw-key absence mitigation (DP2), structured path: `anthropic_to_openai`
        // ALWAYS emits a `usage` object (zeros when the upstream omits it), so a
        // genuinely-absent usage would otherwise be booked as a measured `0,0`
        // (status `ok`, cost 0 = "free") by the MoA accounting path. Judge absence
        // on the RAW upstream `usage` key here and drop the synthetic usage so the
        // orchestrator sees `usage: None` → `ok_no_usage`/NULL. Genuinely-zero
        // upstreams (key present) keep their `0,0` untouched.
        let usage_absent = anthropic.get("usage").is_none();
        let openai_resp = anthropic_to_openai(&anthropic, &model)?;
        let mut chat: ChatResponse =
            serde_json::from_value(openai_resp).map_err(|_| Error::Internal)?;
        if usage_absent {
            chat.usage = None;
        }
        Ok(chat)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn ctx() -> ReqCtx {
        ReqCtx {
            request_id: "rid".into(),
            deadline: Instant::now() + std::time::Duration::from_secs(5),
            caller_key_id: "k".into(),
            model: "claude".into(),
        }
    }

    fn req() -> ChatRequest {
        serde_json::from_value(serde_json::json!({
            "model": "claude",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .unwrap()
    }

    /// chat(): when the raw Anthropic response OMITS `usage`, the structured
    /// response must carry `usage: None` (not a synthetic 0,0) so the MoA
    /// accounting path books `ok_no_usage`/NULL, not a free measured row.
    #[tokio::test]
    async fn chat_usage_absent_yields_none() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/messages"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "msg_1",
                "type": "message",
                "role": "assistant",
                "model": "claude",
                "content": [{"type": "text", "text": "hello"}],
                "stop_reason": "end_turn"
                // NOTE: no `usage` key
            })))
            .mount(&server)
            .await;
        let p = AnthropicProvider::new("up", server.uri(), "sk", 1024, Client::new());
        let resp = p.chat(&ctx(), req()).await.unwrap();
        assert!(
            resp.usage.is_none(),
            "absent upstream usage must map to None"
        );
    }

    /// chat(): a genuinely-zero usage (key PRESENT) is preserved, not dropped.
    #[tokio::test]
    async fn chat_usage_present_zero_is_preserved() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/messages"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "msg_1",
                "type": "message",
                "role": "assistant",
                "model": "claude",
                "content": [{"type": "text", "text": "hello"}],
                "stop_reason": "end_turn",
                "usage": {"input_tokens": 0, "output_tokens": 0}
            })))
            .mount(&server)
            .await;
        let p = AnthropicProvider::new("up", server.uri(), "sk", 1024, Client::new());
        let resp = p.chat(&ctx(), req()).await.unwrap();
        let usage = resp.usage.expect("present usage preserved");
        assert_eq!(usage.get("prompt_tokens").and_then(|v| v.as_i64()), Some(0));
        assert_eq!(
            usage.get("completion_tokens").and_then(|v| v.as_i64()),
            Some(0)
        );
    }
}
