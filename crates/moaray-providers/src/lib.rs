//! moaray-providers — upstream adapters implementing the dual-path `Provider`
//! trait from moaray-core: an OpenAI-compatible adapter and an Anthropic adapter
//! (added in P1-5). Shared upstream-client construction and stream-relay helpers
//! live here too. This crate performs all upstream I/O and credential injection;
//! credentials are never logged.

pub mod client;
pub mod common;
pub mod openai;

pub use client::build_client;
pub use openai::OpenAiProvider;
