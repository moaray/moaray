//! moaray library surface — exposed so integration tests can assemble the axum
//! app in-process against mock upstreams. The binary (`main.rs`) is a thin shell
//! over these modules.

pub mod app;
pub mod auth;
pub mod http_error;
pub mod observe;
pub mod registry;
pub mod runtime;
