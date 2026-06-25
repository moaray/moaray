//! Anthropic SSE -> OpenAI SSE stream translation.
//!
//! Anthropic streams typed events (`message_start`, `content_block_delta`,
//! `message_delta`, `message_stop`). We parse the upstream byte stream into SSE
//! events with a small incremental parser, pull text deltas out of
//! `content_block_delta`, and re-emit OpenAI `chat.completion.chunk` frames,
//! ending with `data: [DONE]`. Non-text blocks trigger a STOP (Unsupported)
//! error mid-stream rather than emitting a corrupt translation.
//!
//! The translation never buffers the whole response: it emits each OpenAI frame
//! as soon as the corresponding Anthropic event is parsed.

use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use moaray_core::error::Error;
use serde_json::Value;

use crate::anthropic_map::{map_stop_reason, openai_delta_frame, openai_finish_frame};

/// One parsed SSE event: an `event:` type and the concatenated `data:` payload.
#[derive(Debug, Default, Clone)]
struct SseEvent {
    event: Option<String>,
    data: String,
}

/// Incremental SSE parser: feed bytes, get back complete events.
#[derive(Default)]
struct SseParser {
    buf: String,
    cur: SseEvent,
}

impl SseParser {
    fn push(&mut self, chunk: &str, out: &mut Vec<SseEvent>) {
        self.buf.push_str(chunk);
        // Process complete lines; keep any trailing partial line in buf.
        while let Some(nl) = self.buf.find('\n') {
            let line = self.buf[..nl].trim_end_matches('\r').to_string();
            self.buf.drain(..=nl);
            if line.is_empty() {
                // dispatch the event if it has any data/type
                if self.cur.event.is_some() || !self.cur.data.is_empty() {
                    out.push(std::mem::take(&mut self.cur));
                }
                continue;
            }
            if let Some(rest) = line.strip_prefix("event:") {
                self.cur.event = Some(rest.trim().to_string());
            } else if let Some(rest) = line.strip_prefix("data:") {
                if !self.cur.data.is_empty() {
                    self.cur.data.push('\n');
                }
                self.cur.data.push_str(rest.trim_start());
            }
            // comment lines (":...") and unknown fields are ignored
        }
    }
}

/// Translate an Anthropic SSE byte stream into a stream of OpenAI SSE byte
/// frames. `model` labels the emitted frames.
pub fn translate<S>(upstream: S, model: String) -> impl Stream<Item = Result<Bytes, Error>>
where
    S: Stream<Item = Result<Bytes, Error>> + Send + 'static,
{
    async_stream::stream! {
        let mut parser = SseParser::default();
        let mut id = "chatcmpl-anthropic".to_string();
        let mut finish: Option<String> = None;
        let mut done_sent = false;
        futures_util::pin_mut!(upstream);

        while let Some(item) = upstream.next().await {
            let bytes = match item {
                Ok(b) => b,
                Err(e) => { yield Err(e); return; }
            };
            let text = match std::str::from_utf8(&bytes) {
                Ok(t) => t.to_string(),
                Err(_) => { yield Err(Error::UpstreamError); return; }
            };
            let mut events = Vec::new();
            parser.push(&text, &mut events);
            for ev in events {
                let data = ev.data.trim();
                if data.is_empty() { continue; }
                let parsed: Value = match serde_json::from_str(data) {
                    Ok(v) => v,
                    Err(_) => continue, // skip non-JSON keepalive frames
                };
                let ev_type = ev
                    .event
                    .as_deref()
                    .or_else(|| parsed.get("type").and_then(Value::as_str))
                    .unwrap_or("");
                match ev_type {
                    "message_start" => {
                        if let Some(mid) = parsed
                            .get("message")
                            .and_then(|m| m.get("id"))
                            .and_then(Value::as_str)
                        {
                            id = mid.to_string();
                        }
                    }
                    "content_block_start" => {
                        let bt = parsed
                            .get("content_block")
                            .and_then(|c| c.get("type"))
                            .and_then(Value::as_str)
                            .unwrap_or("text");
                        if bt != "text" {
                            yield Err(Error::Unsupported(format!(
                                "anthropic stream block `{bt}` not supported in v1"
                            )));
                            return;
                        }
                    }
                    "content_block_delta" => {
                        let delta = parsed.get("delta");
                        let dtype = delta
                            .and_then(|d| d.get("type"))
                            .and_then(Value::as_str)
                            .unwrap_or("text_delta");
                        if dtype != "text_delta" {
                            yield Err(Error::Unsupported(format!(
                                "anthropic delta `{dtype}` not supported in v1"
                            )));
                            return;
                        }
                        if let Some(t) = delta
                            .and_then(|d| d.get("text"))
                            .and_then(Value::as_str)
                        {
                            let frame = openai_delta_frame(&model, &id, t);
                            yield Ok(sse_frame(&frame));
                        }
                    }
                    "message_delta" => {
                        if let Some(r) = parsed
                            .get("delta")
                            .and_then(|d| d.get("stop_reason"))
                            .and_then(Value::as_str)
                        {
                            finish = map_stop_reason(Some(r)).map(|s| s.to_string());
                        }
                    }
                    "message_stop" => {
                        let f = finish.clone().unwrap_or_else(|| "stop".to_string());
                        let frame = openai_finish_frame(&model, &id, &f);
                        yield Ok(sse_frame(&frame));
                        yield Ok(Bytes::from_static(b"data: [DONE]\n\n"));
                        done_sent = true;
                    }
                    _ => {}
                }
            }
        }
        if !done_sent {
            // Upstream ended without an explicit message_stop: still terminate
            // the OpenAI stream cleanly.
            let f = finish.unwrap_or_else(|| "stop".to_string());
            let frame = openai_finish_frame(&model, &id, &f);
            yield Ok(sse_frame(&frame));
            yield Ok(Bytes::from_static(b"data: [DONE]\n\n"));
        }
    }
}

/// Encode a JSON value as an OpenAI SSE `data:` frame.
fn sse_frame(value: &Value) -> Bytes {
    let mut s = String::from("data: ");
    s.push_str(&value.to_string());
    s.push_str("\n\n");
    Bytes::from(s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream;

    async fn collect(s: impl Stream<Item = Result<Bytes, Error>>) -> String {
        futures_util::pin_mut!(s);
        let mut out = String::new();
        while let Some(item) = s.next().await {
            out.push_str(std::str::from_utf8(&item.unwrap()).unwrap());
        }
        out
    }

    #[tokio::test]
    async fn translates_content_block_deltas_to_openai_frames() {
        let upstream = "\
event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_9\"}}\n\n\
event: content_block_start\ndata: {\"type\":\"content_block_start\",\"content_block\":{\"type\":\"text\"}}\n\n\
event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"He\"}}\n\n\
event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"llo\"}}\n\n\
event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n\
event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";
        let s = stream::iter(vec![Ok(Bytes::from(upstream))]);
        let out = collect(translate(s, "claude-x".into())).await;
        assert!(out.contains("\"id\":\"msg_9\""));
        assert!(out.contains("He"));
        assert!(out.contains("llo"));
        assert!(out.contains("chat.completion.chunk"));
        assert!(out.contains("\"finish_reason\":\"stop\""));
        assert!(out.trim_end().ends_with("data: [DONE]"));
    }

    #[tokio::test]
    async fn split_frames_across_chunks_are_reassembled() {
        // The delta event arrives split mid-line across two upstream chunks.
        let parts = vec![
            Ok(Bytes::from("event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_d")),
            Ok(Bytes::from("elta\",\"text\":\"hi\"}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")),
        ];
        let s = stream::iter(parts);
        let out = collect(translate(s, "c".into())).await;
        assert!(out.contains("hi"));
        assert!(out.contains("[DONE]"));
    }

    #[tokio::test]
    async fn tool_use_block_stops_stream() {
        let upstream = "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"content_block\":{\"type\":\"tool_use\"}}\n\n";
        let s = stream::iter(vec![Ok(Bytes::from(upstream))]);
        let st = translate(s, "c".into());
        futures_util::pin_mut!(st);
        let first = st.next().await.unwrap();
        assert!(first.is_err());
        assert_eq!(first.unwrap_err().envelope().code, "unsupported");
    }
}
