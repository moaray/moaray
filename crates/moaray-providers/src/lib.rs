//! moaray-providers — upstream adapters implementing the dual-path `Provider`
//! trait from moaray-core: an OpenAI-compatible adapter and an Anthropic adapter
//! that translates OpenAI<->Anthropic on every path. Shared upstream-client
//! construction, stream-relay helpers, and the (pure, unit-tested) Anthropic
//! mapping/SSE-translation live here too. All upstream I/O and credential
//! injection happen in this crate; credentials are never logged.

pub mod anthropic;
pub mod anthropic_map;
pub mod anthropic_sse;
pub mod client;
pub mod common;
pub mod openai;

pub use anthropic::AnthropicProvider;
pub use client::build_client;
pub use openai::OpenAiProvider;
