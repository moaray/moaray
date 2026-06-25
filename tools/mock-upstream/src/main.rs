//! mock-upstream — a tiny fixed-response OpenAI-compatible echo server for the
//! docker-compose quickstart and local testing. It implements just enough of
//! `/v1/chat/completions` to exercise moaray's passthrough path (non-stream and
//! SSE stream) without any real provider credentials.

use axum::body::Body;
use axum::extract::Json;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use serde_json::{json, Value};

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/v1/chat/completions", post(chat));
    let port = std::env::var("PORT").unwrap_or_else(|_| "9000".to_string());
    let addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    println!("mock-upstream listening on {addr}");
    axum::serve(listener, app).await.unwrap();
}

async fn chat(Json(req): Json<Value>) -> Response {
    let model = req
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("mock-model")
        .to_string();
    let stream = req.get("stream").and_then(Value::as_bool).unwrap_or(false);

    if stream {
        let body = format!(
            "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
            json!({"id":"mock-1","object":"chat.completion.chunk","model":model,
                   "choices":[{"index":0,"delta":{"role":"assistant","content":"Hello"},"finish_reason":null}]}),
            json!({"id":"mock-1","object":"chat.completion.chunk","model":model,
                   "choices":[{"index":0,"delta":{"content":" from mock-upstream"},"finish_reason":"stop"}]}),
        );
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/event-stream")
            .header(header::CACHE_CONTROL, "no-cache")
            .body(Body::from(body))
            .unwrap();
    }

    let resp = json!({
        "id": "mock-1",
        "object": "chat.completion",
        "model": model,
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": "Hello from mock-upstream"},
            "finish_reason": "stop"
        }],
        "usage": {"prompt_tokens": 1, "completion_tokens": 4, "total_tokens": 5}
    });
    (StatusCode::OK, axum::Json(resp)).into_response()
}
