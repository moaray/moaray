//! Validated **runtime descriptor** — what the server actually runs.
//!
//! Produced by `validate()` from the on-disk DTOs. This is where secrets get
//! resolved from the environment (held in memory only) and where invariants are
//! guaranteed: every model has a usable base URL, every recipe references known
//! models, etc. The `Debug` impls here redact secret material.

use std::collections::BTreeMap;
use std::fmt;

use crate::schema::{ProviderType, Strategy};

/// Top-level validated runtime config.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub server: ServerConfig,
    pub keys: Vec<KeyConfig>,
    pub models: BTreeMap<String, ModelConfig>,
    pub recipes: BTreeMap<String, RecipeConfig>,
}

impl RuntimeConfig {
    /// Whether a plain (non-MoA) model name resolves to a configured upstream.
    pub fn is_known_model(&self, name: &str) -> bool {
        self.models.contains_key(name)
    }
}

/// Server knobs (validated copy of `ServerDoc`).
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind: String,
    pub port: u16,
    pub request_timeout_ms: u64,
    pub max_body_bytes: usize,
    pub shutdown_grace_ms: u64,
    pub default_max_tokens: u32,
    /// Whether to emit the optional `moaray` MoA debug extension field. Off by
    /// default (production posture).
    pub moa_expose_metadata: bool,
    /// Per-upstream circuit-breaker thresholds.
    pub breaker: BreakerConfig,
    /// Upstream retry policy (off unless explicitly enabled).
    pub retry: RetryConfig,
    /// Optional persistent usage store (None ⇒ accounting disabled).
    pub usage_store: Option<UsageStoreConfig>,
}

/// Validated persistent usage-store config. Restart-frozen (process-lifetime sink).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageStoreConfig {
    /// SQLite database file path.
    pub path: String,
    /// Bounded channel capacity (hot path → writer thread).
    pub channel_capacity: usize,
    /// Max rows per write transaction.
    pub batch_size: usize,
}

/// Validated per-upstream circuit-breaker thresholds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BreakerConfig {
    pub failure_threshold: u32,
    pub open_ms: u64,
    pub half_open_successes: u32,
}

/// Validated upstream retry policy.
#[derive(Debug, Clone, Copy)]
pub struct RetryConfig {
    pub enabled: bool,
    pub max_retries: u32,
    pub backoff_ms: u64,
}

/// A validated token-bucket rate limit (sustained `rps` + `burst` capacity).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimit {
    pub rps: u32,
    pub burst: u32,
}

/// How a key's secret is verified.
#[derive(Clone)]
pub enum KeySecret {
    /// Plaintext resolved from env (held in memory, redacted in Debug).
    Plain(String),
    /// Lowercase hex sha256 of the plaintext.
    Sha256(String),
}

impl fmt::Debug for KeySecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KeySecret::Plain(_) => f.write_str("KeySecret::Plain(***)"),
            // a hash is not reversible but still treated as sensitive material
            KeySecret::Sha256(_) => f.write_str("KeySecret::Sha256(***)"),
        }
    }
}

/// A validated inbound key.
#[derive(Clone)]
pub struct KeyConfig {
    pub id: String,
    pub secret: KeySecret,
    pub allow_models: Vec<String>,
    /// Optional inbound per-key rate limit.
    pub rate_limit: Option<RateLimit>,
}

impl KeyConfig {
    /// Whether this key may call `model`.
    pub fn allows(&self, model: &str) -> bool {
        self.allow_models.iter().any(|m| m == model)
    }
}

impl fmt::Debug for KeyConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyConfig")
            .field("id", &self.id)
            .field("secret", &self.secret)
            .field("allow_models", &self.allow_models)
            .field("rate_limit", &self.rate_limit)
            .finish()
    }
}

/// A validated upstream model.
#[derive(Clone)]
pub struct ModelConfig {
    pub name: String,
    pub provider_type: ProviderType,
    pub base_url: String,
    /// Env var name holding the upstream key (resolved at provider build time).
    pub api_key_env: String,
    /// Internal **state key** — `provider_type|base_url|api_key_env`. Keys the
    /// per-upstream limiter/concurrency/breaker in `StatefulState`. Derived (never
    /// user-set) so renaming a model keeps its state (base_url/key unchanged) and
    /// two aliases of the same upstream share one bucket (no per-upstream-cap
    /// bypass). **Internal only:** it contains `base_url` and must NEVER reach a
    /// metric label or client response (that is what the low-cardinality
    /// `upstream_id` is for — see no-secret-logging rule).
    pub state_key: String,
    /// Human-facing, low-cardinality observability label (the Prometheus
    /// `upstream_id` label and the client `moaray` debug field). Defaults to
    /// `name`. Decoupled from `state_key`: relabeling it never moves a bucket.
    pub upstream_id: String,
    /// Optional per-upstream rate limit (shared across passthrough + MoA arms).
    pub rate_limit: Option<RateLimit>,
    /// Optional per-upstream concurrency cap. `None` means unbounded.
    pub max_concurrency: Option<u32>,
    /// Prompt-token price as integer nano-USD per 1M tokens (converted from the
    /// `price_prompt_per_mtok_usd` USD float at validation). `None` = unpriced.
    pub price_prompt_nano_per_mtok: Option<i64>,
    /// Completion-token price as integer nano-USD per 1M tokens. `None` = unpriced.
    pub price_completion_nano_per_mtok: Option<i64>,
}

impl ModelConfig {
    /// Build the internal state key from the identity triple
    /// (`provider_type|base_url|api_key_env`). Stable across model renames and
    /// `upstream_id` relabels; changes only when the upstream identity (URL or
    /// account/key env) changes, which correctly forces a fresh bucket/breaker.
    pub fn derive_state_key(
        provider_type: ProviderType,
        base_url: &str,
        api_key_env: &str,
    ) -> String {
        format!("{}|{}|{}", provider_type.as_str(), base_url, api_key_env)
    }
}

impl fmt::Debug for ModelConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // api_key_env is only a var *name*, not a secret, but we keep the struct
        // explicit so future secret fields are never auto-derived into Debug.
        f.debug_struct("ModelConfig")
            .field("name", &self.name)
            .field("provider_type", &self.provider_type)
            .field("base_url", &self.base_url)
            .field("api_key_env", &self.api_key_env)
            .field("state_key", &self.state_key)
            .field("upstream_id", &self.upstream_id)
            .field("rate_limit", &self.rate_limit)
            .field("max_concurrency", &self.max_concurrency)
            .field(
                "price_prompt_nano_per_mtok",
                &self.price_prompt_nano_per_mtok,
            )
            .field(
                "price_completion_nano_per_mtok",
                &self.price_completion_nano_per_mtok,
            )
            .finish()
    }
}

/// A validated MoA recipe.
#[derive(Debug, Clone)]
pub struct RecipeConfig {
    pub name: String,
    pub proposers: Vec<String>,
    pub aggregator: String,
    pub strategy: Strategy,
    pub arm_timeout_ms: u64,
    pub quorum: usize,
}
