//! Integration tests for the OpenAI-compatible passthrough adapter (wiremock).

use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::StreamExt;
use moaray_core::provider::{Provider, ReqCtx};
use moaray_providers::{build_client, OpenAiProvider};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

fn ctx() -> ReqCtx {
    ReqCtx {
        request_id: "req-test".into(),
        deadline: Instant::now() + Duration::from_secs(30),
        caller_key_id: "team-a".into(),
        model: "gpt".into(),
    }
}

async fn drain(body: moaray_core::provider::ByteStream) -> Vec<u8> {
    let mut out = Vec::new();
    let mut s = body;
    while let Some(chunk) = s.next().await {
        out.extend_from_slice(&chunk.unwrap());
    }
    out
}

#[tokio::test]
async fn passthrough_forwards_body_verbatim_including_unknown_fields() {
    let server = MockServer::start().await;
    // Echo the request body back so we can assert byte-exact forwarding.
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(header("authorization", "Bearer sk-upstream"))
        .respond_with(|req: &Request| {
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_raw(req.body.clone(), "application/json")
        })
        .mount(&server)
        .await;

    let provider = OpenAiProvider::new("gpt", server.uri(), "sk-upstream", build_client());

    // Body carries an unknown vendor field that must be forwarded untouched.
    let raw = br#"{"model":"gpt","messages":[{"role":"user","content":"hi"}],"vendor_x":{"a":1},"seed":7}"#;
    let resp = provider
        .passthrough(&ctx(), Bytes::from_static(raw))
        .await
        .expect("passthrough ok");
    assert_eq!(resp.status, 200);
    let body = drain(resp.body).await;
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["vendor_x"]["a"], serde_json::json!(1));
    assert_eq!(v["seed"], serde_json::json!(7));
}

#[tokio::test]
async fn passthrough_stream_relays_frames_and_done() {
    let server = MockServer::start().await;
    let sse = "data: {\"choices\":[{\"delta\":{\"content\":\"He\"}}]}\n\n\
               data: {\"choices\":[{\"delta\":{\"content\":\"llo\"}}]}\n\n\
               data: {\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":2}}\n\n\
               data: [DONE]\n\n";
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(sse.as_bytes().to_vec(), "text/event-stream"),
        )
        .mount(&server)
        .await;

    let provider = OpenAiProvider::new("gpt", server.uri(), "sk-upstream", build_client());
    let raw = br#"{"model":"gpt","stream":true,"messages":[]}"#;
    let resp = provider
        .passthrough_stream(&ctx(), Bytes::from_static(raw))
        .await
        .expect("stream ok");
    assert_eq!(resp.status, 200);
    assert_eq!(resp.content_type.as_deref(), Some("text/event-stream"));
    let body = String::from_utf8(drain(resp.body).await).unwrap();
    // Two delta frames, the usage chunk, and the terminal [DONE] all survive.
    assert!(body.contains("He"));
    assert!(body.contains("llo"));
    assert!(body.contains("completion_tokens"));
    assert!(body.contains("[DONE]"));
}

#[tokio::test]
async fn upstream_429_maps_to_rate_limited() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(429))
        .mount(&server)
        .await;
    let provider = OpenAiProvider::new("gpt", server.uri(), "k", build_client());
    let err = provider
        .passthrough(&ctx(), Bytes::from_static(b"{}"))
        .await
        .unwrap_err();
    assert_eq!(err.envelope().status, 429);
}

#[tokio::test]
async fn upstream_5xx_maps_to_upstream_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    let provider = OpenAiProvider::new("gpt", server.uri(), "k", build_client());
    let err = provider
        .passthrough(&ctx(), Bytes::from_static(b"{}"))
        .await
        .unwrap_err();
    assert_eq!(err.envelope().status, 502);
}
