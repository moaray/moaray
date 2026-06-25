//! Config **DTOs** — the on-disk YAML shape, nothing more.
//!
//! These types are `deny_unknown_fields` so typos and unsupported keys are
//! rejected loudly. They are intentionally separate from the validated runtime
//! descriptor (see `runtime.rs`): the schema is what the user writes, the
//! descriptor is what the server runs. Runtime types never leak into here.

use serde::Deserialize;

/// Root config document.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigDoc {
    #[serde(default)]
    pub server: ServerDoc,
    pub auth: AuthDoc,
    pub models: Vec<ModelDoc>,
    #[serde(default)]
    pub recipes: std::collections::BTreeMap<String, RecipeDoc>,
}

/// Server / transport knobs.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerDoc {
    #[serde(default = "default_bind")]
    pub bind: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
    #[serde(default = "default_max_body_bytes")]
    pub max_body_bytes: usize,
    #[serde(default = "default_shutdown_grace_ms")]
    pub shutdown_grace_ms: u64,
    /// Default max_tokens injected for upstreams that require it (anthropic).
    #[serde(default = "default_max_tokens")]
    pub default_max_tokens: u32,
}

impl Default for ServerDoc {
    fn default() -> Self {
        Self {
            bind: default_bind(),
            port: default_port(),
            request_timeout_ms: default_request_timeout_ms(),
            max_body_bytes: default_max_body_bytes(),
            shutdown_grace_ms: default_shutdown_grace_ms(),
            default_max_tokens: default_max_tokens(),
        }
    }
}

fn default_bind() -> String {
    "0.0.0.0".to_string()
}
fn default_port() -> u16 {
    8080
}
fn default_request_timeout_ms() -> u64 {
    120_000
}
fn default_max_body_bytes() -> usize {
    1024 * 1024 // 1 MiB
}
fn default_shutdown_grace_ms() -> u64 {
    15_000
}
fn default_max_tokens() -> u32 {
    4096
}

/// Inbound auth: a set of API keys, each with a model allowlist.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthDoc {
    pub keys: Vec<KeyDoc>,
}

/// A single inbound API key.
///
/// The secret is never stored in plaintext in config: either reference an env
/// var (`key_env`) or pin a `sha256` hex digest (`key_sha256`). Exactly one of
/// the two must be set. The model allowlist gates which models this key may call.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyDoc {
    /// Non-secret label for logs/metrics/ReqCtx.
    pub id: String,
    /// Env var holding the plaintext key.
    #[serde(default)]
    pub key_env: Option<String>,
    /// Lowercase hex sha256 of the plaintext key.
    #[serde(default)]
    pub key_sha256: Option<String>,
    /// Model names this key may call. Empty is rejected by validate().
    pub allow_models: Vec<String>,
}

/// A configured upstream model.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelDoc {
    /// Public model name callers use.
    pub name: String,
    /// Adapter type.
    pub provider_type: ProviderType,
    /// Upstream base URL (must be http/https).
    pub base_url: String,
    /// Env var holding the upstream API key. Never inline the secret.
    pub api_key_env: String,
    /// Stable id for stateful keying (limiter/breaker). Defaults to `name`.
    #[serde(default)]
    pub upstream_id: Option<String>,
}

/// Supported provider adapter kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProviderType {
    OpenaiCompat,
    Anthropic,
}

/// A MoA recipe. Schema is fully defined in Phase 1; the orchestration logic is
/// activated in Phase 2.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecipeDoc {
    pub proposers: Vec<String>,
    pub aggregator: String,
    pub strategy: Strategy,
    #[serde(default = "default_arm_timeout_ms")]
    pub arm_timeout_ms: u64,
    pub quorum: usize,
}

fn default_arm_timeout_ms() -> u64 {
    60_000
}

/// MoA fusion strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Strategy {
    ConcatSynthesize,
    QuorumJudge,
}
