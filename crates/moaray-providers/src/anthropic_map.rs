//! Pure OpenAI <-> Anthropic translation (no I/O), unit-testable in isolation.
//!
//! v1 supports **text chat only**. If a request or response carries `tool_use`
//! or any non-text content block, we return [`Error::Unsupported`] — the STOP
//! condition from the plan, surfaced as a clean gateway error rather than a
//! silent mistranslation.

use moaray_core::error::{Error, Result};
use serde_json::{json, Map, Value};

pub const ANTHROPIC_VERSION: &str = "2023-06-01";

/// Build the Anthropic `/v1/messages` request body from an OpenAI chat request
/// (already parsed into a JSON value). `default_max_tokens` is injected when the
/// caller omitted `max_tokens` (Anthropic requires it).
pub fn openai_to_anthropic(req: &Value, default_max_tokens: u32) -> Result<Value> {
    let model = req
        .get("model")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::BadRequest("missing model".into()))?;

    // v1 is text-only. Tool calling / structured-output controls cannot be
    // faithfully mapped to the Anthropic text path, so STOP with a clean
    // `unsupported` error rather than silently dropping them and downgrading a
    // tool request to plain text. (Mirrors the response-side guard in
    // `anthropic_to_openai`.)
    for field in ["tools", "tool_choice", "response_format"] {
        if let Some(v) = req.get(field) {
            if !v.is_null() {
                return Err(Error::Unsupported(format!(
                    "`{field}` is not supported in v1 (text-only)"
                )));
            }
        }
    }

    let mut system_parts: Vec<String> = Vec::new();
    let mut messages: Vec<Value> = Vec::new();

    if let Some(arr) = req.get("messages").and_then(Value::as_array) {
        for m in arr {
            let role = m.get("role").and_then(Value::as_str).unwrap_or("user");
            let content = m.get("content");
            let text = content_to_text(content)?;
            match role {
                "system" => system_parts.push(text),
                "user" | "assistant" => messages.push(json!({
                    "role": role,
                    "content": text,
                })),
                other => {
                    return Err(Error::Unsupported(format!(
                        "unsupported message role `{other}`"
                    )))
                }
            }
        }
    }

    let max_tokens = req
        .get("max_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(default_max_tokens as u64);

    let mut out = Map::new();
    out.insert("model".into(), json!(model));
    out.insert("messages".into(), json!(messages));
    out.insert("max_tokens".into(), json!(max_tokens));
    if !system_parts.is_empty() {
        out.insert("system".into(), json!(system_parts.join("\n\n")));
    }
    if let Some(t) = req.get("temperature").and_then(Value::as_f64) {
        out.insert("temperature".into(), json!(t));
    }
    if let Some(p) = req.get("top_p").and_then(Value::as_f64) {
        out.insert("top_p".into(), json!(p));
    }
    if let Some(s) = req.get("stream").and_then(Value::as_bool) {
        out.insert("stream".into(), json!(s));
    }
    Ok(Value::Object(out))
}

/// Extract plain text from an OpenAI message `content` field. Accepts a string,
/// or an array of `{type:"text", text:"..."}` parts. Rejects non-text parts.
fn content_to_text(content: Option<&Value>) -> Result<String> {
    match content {
        None | Some(Value::Null) => Ok(String::new()),
        Some(Value::String(s)) => Ok(s.clone()),
        Some(Value::Array(parts)) => {
            let mut buf = String::new();
            for part in parts {
                let ty = part.get("type").and_then(Value::as_str).unwrap_or("text");
                if ty != "text" {
                    return Err(Error::Unsupported(format!(
                        "non-text content part `{ty}` not supported in v1"
                    )));
                }
                if let Some(t) = part.get("text").and_then(Value::as_str) {
                    buf.push_str(t);
                }
            }
            Ok(buf)
        }
        Some(other) => Err(Error::BadRequest(format!(
            "unexpected content shape: {other}"
        ))),
    }
}

/// Map an Anthropic `stop_reason` to an OpenAI `finish_reason`.
pub fn map_stop_reason(reason: Option<&str>) -> Option<&'static str> {
    match reason {
        Some("end_turn") | Some("stop_sequence") => Some("stop"),
        Some("max_tokens") => Some("length"),
        Some(_) => Some("stop"),
        None => None,
    }
}

/// Translate a full (non-streaming) Anthropic Messages response into an
/// OpenAI chat-completion response value.
pub fn anthropic_to_openai(resp: &Value, model: &str) -> Result<Value> {
    let mut text = String::new();
    if let Some(blocks) = resp.get("content").and_then(Value::as_array) {
        for b in blocks {
            let ty = b.get("type").and_then(Value::as_str).unwrap_or("text");
            if ty != "text" {
                return Err(Error::Unsupported(format!(
                    "anthropic content block `{ty}` not supported in v1"
                )));
            }
            if let Some(t) = b.get("text").and_then(Value::as_str) {
                text.push_str(t);
            }
        }
    }
    let finish = map_stop_reason(resp.get("stop_reason").and_then(Value::as_str)).unwrap_or("stop");

    let (prompt_tokens, completion_tokens) = usage_tokens(resp.get("usage"));
    let id = resp
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or("chatcmpl-anthropic");

    Ok(json!({
        "id": id,
        "object": "chat.completion",
        "model": model,
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": text},
            "finish_reason": finish,
        }],
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens + completion_tokens,
        }
    }))
}

pub(crate) fn usage_tokens(usage: Option<&Value>) -> (u64, u64) {
    let u = match usage {
        Some(v) => v,
        None => return (0, 0),
    };
    let p = u.get("input_tokens").and_then(Value::as_u64).unwrap_or(0);
    let c = u.get("output_tokens").and_then(Value::as_u64).unwrap_or(0);
    (p, c)
}

/// Build an OpenAI streaming delta frame body (the JSON after `data: `).
pub fn openai_delta_frame(model: &str, id: &str, content: &str) -> Value {
    json!({
        "id": id,
        "object": "chat.completion.chunk",
        "model": model,
        "choices": [{"index": 0, "delta": {"content": content}, "finish_reason": null}],
    })
}

/// Build the terminal OpenAI streaming frame carrying `finish_reason`.
pub fn openai_finish_frame(model: &str, id: &str, finish: &str) -> Value {
    json!({
        "id": id,
        "object": "chat.completion.chunk",
        "model": model,
        "choices": [{"index": 0, "delta": {}, "finish_reason": finish}],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_extracts_system_and_maps_roles_with_default_max_tokens() {
        let req = json!({
            "model": "claude-x",
            "messages": [
                {"role":"system","content":"be brief"},
                {"role":"user","content":"hi"},
                {"role":"assistant","content":"hello"}
            ],
            "temperature": 0.5
        });
        let out = openai_to_anthropic(&req, 4096).unwrap();
        assert_eq!(out["system"], json!("be brief"));
        assert_eq!(out["max_tokens"], json!(4096));
        assert_eq!(out["temperature"], json!(0.5));
        let msgs = out["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0]["role"], json!("user"));
        assert_eq!(msgs[1]["role"], json!("assistant"));
    }

    #[test]
    fn request_respects_explicit_max_tokens() {
        let req = json!({"model":"c","messages":[],"max_tokens":128});
        let out = openai_to_anthropic(&req, 4096).unwrap();
        assert_eq!(out["max_tokens"], json!(128));
    }

    #[test]
    fn response_maps_to_openai_shape() {
        let resp = json!({
            "id":"msg_1",
            "content":[{"type":"text","text":"Hello"},{"type":"text","text":" world"}],
            "stop_reason":"end_turn",
            "usage":{"input_tokens":3,"output_tokens":2}
        });
        let out = anthropic_to_openai(&resp, "claude-x").unwrap();
        assert_eq!(
            out["choices"][0]["message"]["content"],
            json!("Hello world")
        );
        assert_eq!(out["choices"][0]["finish_reason"], json!("stop"));
        assert_eq!(out["usage"]["prompt_tokens"], json!(3));
        assert_eq!(out["usage"]["completion_tokens"], json!(2));
        assert_eq!(out["usage"]["total_tokens"], json!(5));
    }

    #[test]
    fn max_tokens_stop_reason_maps_to_length() {
        assert_eq!(map_stop_reason(Some("max_tokens")), Some("length"));
        assert_eq!(map_stop_reason(Some("end_turn")), Some("stop"));
    }

    #[test]
    fn tool_use_block_is_unsupported() {
        let resp = json!({"content":[{"type":"tool_use","id":"t","name":"f"}]});
        let err = anthropic_to_openai(&resp, "c").unwrap_err();
        assert_eq!(err.envelope().code, "unsupported");
    }

    #[test]
    fn request_tools_are_unsupported() {
        let req = json!({
            "model": "claude-x",
            "messages": [{"role":"user","content":"hi"}],
            "tools": [{"type":"function","function":{"name":"get_weather"}}]
        });
        let err = openai_to_anthropic(&req, 4096).unwrap_err();
        assert_eq!(err.envelope().code, "unsupported");
    }

    #[test]
    fn request_tool_choice_is_unsupported() {
        let req = json!({
            "model": "claude-x",
            "messages": [{"role":"user","content":"hi"}],
            "tool_choice": "auto"
        });
        let err = openai_to_anthropic(&req, 4096).unwrap_err();
        assert_eq!(err.envelope().code, "unsupported");
    }

    #[test]
    fn request_response_format_is_unsupported() {
        let req = json!({
            "model": "claude-x",
            "messages": [{"role":"user","content":"hi"}],
            "response_format": {"type":"json_object"}
        });
        let err = openai_to_anthropic(&req, 4096).unwrap_err();
        assert_eq!(err.envelope().code, "unsupported");
    }

    #[test]
    fn request_with_null_tool_fields_is_allowed() {
        // Explicit JSON nulls are the OpenAI default for "unset" and must not
        // trip the guard — only present, non-null tool controls STOP.
        let req = json!({
            "model": "claude-x",
            "messages": [{"role":"user","content":"hi"}],
            "tools": null,
            "tool_choice": null,
            "response_format": null
        });
        let out = openai_to_anthropic(&req, 4096).unwrap();
        assert_eq!(out["model"], json!("claude-x"));
    }
}
