//! OpenAI-compatible wire types.
//!
//! Every top-level request/response/chunk type carries a `#[serde(flatten)]`
//! `extra` map so unknown vendor fields (e.g. usage extensions, provider hints)
//! survive a serde round-trip. The passthrough path never touches these types
//! (it forwards raw bytes); they exist for the structured `chat()` path and for
//! the router, which only needs `model`.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// A single chat message (OpenAI shape).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChatMessage {
    pub role: String,
    /// Content is `String` in the common case, but may be an array of content
    /// parts for multimodal callers; keep it as raw JSON so we never lose data.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<Value>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Inbound `POST /v1/chat/completions` request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChatRequest {
    pub model: String,
    #[serde(default)]
    pub messages: Vec<ChatMessage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f64>,
    /// Unknown fields are preserved verbatim.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl ChatRequest {
    /// Whether the caller asked for a streamed response.
    pub fn is_stream(&self) -> bool {
        self.stream.unwrap_or(false)
    }
}

/// Non-streaming completion response (OpenAI shape).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChatResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default)]
    pub choices: Vec<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<Value>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// A single streamed chunk (`chat.completion.chunk`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChatChunk {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default)]
    pub choices: Vec<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<Value>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chat_request_round_trip_preserves_unknown_fields() {
        let raw = r#"{
            "model": "gpt-5.5",
            "messages": [{"role":"user","content":"hi","name":"bob"}],
            "stream": true,
            "vendor_flag": {"beta": true},
            "seed": 42
        }"#;
        let req: ChatRequest = serde_json::from_str(raw).unwrap();
        assert_eq!(req.model, "gpt-5.5");
        assert!(req.is_stream());
        // unknown top-level fields land in extra
        assert!(req.extra.contains_key("vendor_flag"));
        assert_eq!(req.extra.get("seed").unwrap(), &serde_json::json!(42));
        // unknown message fields land in message.extra
        assert_eq!(
            req.messages[0].extra.get("name").unwrap(),
            &serde_json::json!("bob")
        );

        // round trip back out keeps the unknown fields
        let out = serde_json::to_value(&req).unwrap();
        assert_eq!(
            out.get("vendor_flag").unwrap(),
            &serde_json::json!({"beta": true})
        );
        assert_eq!(out.get("seed").unwrap(), &serde_json::json!(42));
        assert_eq!(out["messages"][0]["name"], serde_json::json!("bob"));
    }

    #[test]
    fn chat_response_preserves_usage_extension() {
        let raw = r#"{
            "id":"cmpl-1","object":"chat.completion","model":"gpt-5.5",
            "choices":[{"index":0,"message":{"role":"assistant","content":"hi"}}],
            "usage":{"prompt_tokens":1,"completion_tokens":2,"vendor_cost_usd":0.01}
        }"#;
        let resp: ChatResponse = serde_json::from_str(raw).unwrap();
        let out = serde_json::to_value(&resp).unwrap();
        // vendor field inside usage survives because usage is raw Value
        assert_eq!(out["usage"]["vendor_cost_usd"], serde_json::json!(0.01));
    }

    #[test]
    fn chat_chunk_round_trip() {
        let raw = r#"{"id":"c1","object":"chat.completion.chunk","choices":[{"delta":{"content":"a"}}],"x_provider":"glm"}"#;
        let chunk: ChatChunk = serde_json::from_str(raw).unwrap();
        let out = serde_json::to_value(&chunk).unwrap();
        assert_eq!(out["x_provider"], serde_json::json!("glm"));
    }
}
