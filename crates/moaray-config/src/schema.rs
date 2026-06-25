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
    /// Emit the optional `moaray` extension field with per-arm MoA metadata in
    /// responses. **Off by default** (production posture); enable for debugging.
    #[serde(default)]
    pub moa_expose_metadata: bool,
    /// Per-upstream circuit-breaker defaults (applied per `upstream_id`).
    #[serde(default)]
    pub breaker: BreakerDoc,
    /// Upstream retry defaults. Retries are **off** unless explicitly enabled.
    #[serde(default)]
    pub retry: RetryDoc,
}

/// Per-upstream circuit-breaker tuning. State is kept per `upstream_id` and
/// survives config reloads (see runtime `StatefulState`); these knobs only set
/// the thresholds.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BreakerDoc {
    /// Consecutive upstream failures that trip the breaker open.
    #[serde(default = "default_breaker_failure_threshold")]
    pub failure_threshold: u32,
    /// How long the breaker stays open before allowing a half-open probe (ms).
    #[serde(default = "default_breaker_open_ms")]
    pub open_ms: u64,
    /// Consecutive half-open successes required to close the breaker again.
    #[serde(default = "default_breaker_half_open_successes")]
    pub half_open_successes: u32,
}

impl Default for BreakerDoc {
    fn default() -> Self {
        Self {
            failure_threshold: default_breaker_failure_threshold(),
            open_ms: default_breaker_open_ms(),
            half_open_successes: default_breaker_half_open_successes(),
        }
    }
}

fn default_breaker_failure_threshold() -> u32 {
    5
}
fn default_breaker_open_ms() -> u64 {
    30_000
}
fn default_breaker_half_open_successes() -> u32 {
    1
}

/// Upstream retry policy. **Conservative by design** (plan P3-2): even when
/// enabled, retries only ever apply to connection failures that happened before
/// the request was sent, and streaming requests are never retried.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryDoc {
    /// Master switch. Off by default — a generation request is not naturally
    /// idempotent, so opting in is a deliberate choice.
    #[serde(default)]
    pub enabled: bool,
    /// Maximum retry attempts (in addition to the first try).
    #[serde(default = "default_retry_max")]
    pub max_retries: u32,
    /// Base backoff between attempts (ms); doubles each attempt.
    #[serde(default = "default_retry_backoff_ms")]
    pub backoff_ms: u64,
}

impl Default for RetryDoc {
    fn default() -> Self {
        Self {
            enabled: false,
            max_retries: default_retry_max(),
            backoff_ms: default_retry_backoff_ms(),
        }
    }
}

fn default_retry_max() -> u32 {
    2
}
fn default_retry_backoff_ms() -> u64 {
    100
}

/// A token-bucket rate limit: sustained `rps` requests/second with a `burst`
/// allowance. Used for both per-key (inbound) and per-upstream limits.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RateLimitDoc {
    /// Sustained requests per second.
    pub rps: u32,
    /// Burst capacity (max tokens). Defaults to `rps` when omitted.
    #[serde(default)]
    pub burst: Option<u32>,
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
            moa_expose_metadata: false,
            breaker: BreakerDoc::default(),
            retry: RetryDoc::default(),
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
    /// Optional inbound rate limit for this key. When set, requests presenting
    /// this key are token-bucket limited; over-limit yields 429 `rate_limited`.
    #[serde(default)]
    pub rate_limit: Option<RateLimitDoc>,
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
    /// Optional per-upstream rate limit. Shared by passthrough and MoA arms that
    /// resolve to the same `upstream_id` (MoA fan-out cannot bypass it).
    #[serde(default)]
    pub rate_limit: Option<RateLimitDoc>,
    /// Optional per-upstream concurrency cap (in-flight request ceiling). Shared
    /// across passthrough + MoA via `upstream_id`. `None` means unbounded.
    #[serde(default)]
    pub max_concurrency: Option<u32>,
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
