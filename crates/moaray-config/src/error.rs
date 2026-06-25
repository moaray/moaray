//! Config load/validate errors.

use thiserror::Error;

/// A configuration error, surfaced at startup (never at request time).
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("failed to parse config: {0}")]
    Parse(String),

    #[error("duplicate model name: {0}")]
    DuplicateModel(String),

    #[error("key `{0}` has an empty allow_models list")]
    EmptyAllowlist(String),

    #[error("key `{0}` must set exactly one of key_env or key_sha256")]
    KeySecretShape(String),

    #[error("model `{model}` base_url scheme `{scheme}` is not allowed (only http/https)")]
    BadBaseUrlScheme { model: String, scheme: String },

    #[error("model `{model}` has an invalid base_url: {reason}")]
    BadBaseUrl { model: String, reason: String },

    #[error("recipe `{recipe}` references unknown {role} model `{model}`")]
    UnknownRecipeModel {
        recipe: String,
        role: &'static str,
        model: String,
    },

    #[error("recipe `{0}` has an empty proposers list")]
    EmptyProposers(String),

    #[error("recipe `{recipe}` quorum {quorum} must be in 1..={max}")]
    BadQuorum {
        recipe: String,
        quorum: usize,
        max: usize,
    },

    #[error("no models configured")]
    NoModels,

    #[error("{scope} `{name}` rate_limit.rps must be >= 1")]
    BadRateLimit { scope: &'static str, name: String },
}
