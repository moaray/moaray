//! Single place that renders a core [`Error`] into the OpenAI-compatible HTTP
//! envelope `{"error":{"message","type","code","param"}}`. Handlers never write
//! ad-hoc error JSON (error-envelope rule).

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use moaray_core::error::Error;
use serde_json::json;

/// Newtype wrapper so we can implement `IntoResponse` for the core error.
pub struct ApiError(pub Error);

impl From<Error> for ApiError {
    fn from(e: Error) -> Self {
        ApiError(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let env = self.0.envelope();
        let status = StatusCode::from_u16(env.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let body = json!({
            "error": {
                "message": env.message,
                "type": env.error_type,
                "code": env.code,
                "param": null,
            }
        });
        (status, Json(body)).into_response()
    }
}
