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
    pub upstream_id: String,
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
            .field("upstream_id", &self.upstream_id)
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
