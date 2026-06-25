//! Integration tests for the Anthropic adapter (wiremock): request mapping,
//! response mapping, and SSE translation.

use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::StreamExt;
use moaray_core::provider::{Provider, ReqCtx};
use moaray_providers::{build_client, AnthropicProvider};
use serde_json::Value;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

fn ctx() -> ReqCtx {
    ReqCtx {
        request_id: "req".into(),
        deadline: Instant::now() + Duration::from_secs(30),
        caller_key_id: "k".into(),
        model: "claude".into(),
    }
}

async fn drain(body: moaray_core::provider::ByteStream) -> Vec<u8> {
    let mut out = Vec::new();
    let mut s = body;
    while let Some(c) = s.next().await {
        out.extend_from_slice(&c.unwrap());
    }
    out
}

#[tokio::test]
async fn maps_request_system_roles_and_default_max_tokens() {
    use std::sync::{Arc, Mutex};
    let server = MockServer::start().await;
    let captured: Arc<Mutex<Option<Value>>> = Arc::new(Mutex::new(None));
    let cap = captured.clone();
    // capture the body the provider sent upstream and assert the mapping.
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .and(header("x-api-key", "sk-anthropic"))
        .and(header("anthropic-version", "2023-06-01"))
        .respond_with(move |req: &Request| {
            let sent: Value = serde_json::from_slice(&req.body).unwrap();
            *cap.lock().unwrap() = Some(sent);
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "msg_1",
                "content": [{"type":"text","text":"ok"}],
                "stop_reason": "end_turn",
                "usage": {"input_tokens": 1, "output_tokens": 1}
            }))
        })
        .mount(&server)
        .await;

    let provider =
        AnthropicProvider::new("claude", server.uri(), "sk-anthropic", 4096, build_client());
    let raw = br#"{"model":"claude","messages":[{"role":"system","content":"be brief"},{"role":"user","content":"hi"}]}"#;
    let resp = provider
        .passthrough(&ctx(), Bytes::from_static(raw))
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(&drain(resp.body).await).unwrap();
    // response was mapped to OpenAI shape
    assert_eq!(
        v["choices"][0]["message"]["content"],
        serde_json::json!("ok")
    );
    assert_eq!(v["choices"][0]["finish_reason"], serde_json::json!("stop"));
    assert_eq!(v["usage"]["prompt_tokens"], serde_json::json!(1));
    // and the upstream request was mapped: system extracted, max_tokens defaulted
    let echo = captured
        .lock()
        .unwrap()
        .clone()
        .expect("upstream was called");
    assert_eq!(echo["system"], serde_json::json!("be brief"));
    assert_eq!(echo["max_tokens"], serde_json::json!(4096));
    assert_eq!(echo["messages"].as_array().unwrap().len(), 1);
    assert_eq!(echo["messages"][0]["role"], serde_json::json!("user"));
}

#[tokio::test]
async fn translates_streaming_content_block_deltas_to_openai() {
    let server = MockServer::start().await;
    let sse = "\
event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_7\"}}\n\n\
event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"Hel\"}}\n\n\
event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"lo\"}}\n\n\
event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n\
event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(sse.as_bytes().to_vec(), "text/event-stream"),
        )
        .mount(&server)
        .await;

    let provider = AnthropicProvider::new("claude", server.uri(), "k", 4096, build_client());
    let raw = br#"{"model":"claude","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
    let resp = provider
        .passthrough_stream(&ctx(), Bytes::from_static(raw))
        .await
        .unwrap();
    assert_eq!(resp.content_type.as_deref(), Some("text/event-stream"));
    let out = String::from_utf8(drain(resp.body).await).unwrap();
    assert!(out.contains("Hel"));
    assert!(out.contains("lo"));
    assert!(out.contains("chat.completion.chunk"));
    assert!(out.contains("\"finish_reason\":\"stop\""));
    assert!(out.trim_end().ends_with("data: [DONE]"));
    // at least two delta frames
    assert!(out.matches("\"delta\"").count() >= 2);
}
