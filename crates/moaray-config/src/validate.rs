//! DTO -> runtime descriptor validation.
//!
//! All cross-field invariants live here so the rest of the codebase can trust a
//! `RuntimeConfig` unconditionally. Secrets referenced by env are resolved into
//! memory; missing env vars are NOT fatal here (the provider layer resolves the
//! upstream key lazily), but inbound-key plaintext is resolved eagerly so auth
//! has something to compare against.

use std::collections::{BTreeMap, BTreeSet};

use url::Url;

use crate::error::ConfigError;
use crate::runtime::{
    KeyConfig, KeySecret, ModelConfig, RecipeConfig, RuntimeConfig, ServerConfig,
};
use crate::schema::ConfigDoc;

/// Resolver for environment values. Injectable so tests don't touch real env.
pub trait EnvSource {
    fn get(&self, key: &str) -> Option<String>;
}

/// Reads from the real process environment.
pub struct OsEnv;

impl EnvSource for OsEnv {
    fn get(&self, key: &str) -> Option<String> {
        std::env::var(key).ok()
    }
}

fn valid_base_url(model: &str, raw: &str) -> Result<String, ConfigError> {
    let url = Url::parse(raw).map_err(|e| ConfigError::BadBaseUrl {
        model: model.to_string(),
        reason: e.to_string(),
    })?;
    match url.scheme() {
        "http" | "https" => Ok(raw.trim_end_matches('/').to_string()),
        other => Err(ConfigError::BadBaseUrlScheme {
            model: model.to_string(),
            scheme: other.to_string(),
        }),
    }
}

/// Validate a parsed [`ConfigDoc`], producing a [`RuntimeConfig`].
pub fn validate<E: EnvSource>(doc: ConfigDoc, env: &E) -> Result<RuntimeConfig, ConfigError> {
    if doc.models.is_empty() {
        return Err(ConfigError::NoModels);
    }

    // models — reject duplicates, validate base_url scheme.
    let mut models: BTreeMap<String, ModelConfig> = BTreeMap::new();
    for m in doc.models {
        if models.contains_key(&m.name) {
            return Err(ConfigError::DuplicateModel(m.name));
        }
        let base_url = valid_base_url(&m.name, &m.base_url)?;
        let upstream_id = m.upstream_id.clone().unwrap_or_else(|| m.name.clone());
        models.insert(
            m.name.clone(),
            ModelConfig {
                name: m.name,
                provider_type: m.provider_type,
                base_url,
                api_key_env: m.api_key_env,
                upstream_id,
            },
        );
    }
    let known: BTreeSet<&String> = models.keys().collect();

    // keys — exactly one secret shape, non-empty allowlist, known models.
    let mut keys = Vec::new();
    for k in doc.auth.keys {
        let secret = match (k.key_env.as_ref(), k.key_sha256.as_ref()) {
            (Some(env_var), None) => {
                // Resolve now; if absent, store empty plaintext (auth will fail
                // closed). We do not hard-error so the server can boot with a
                // subset of keys provisioned.
                KeySecret::Plain(env.get(env_var).unwrap_or_default())
            }
            (None, Some(hash)) => KeySecret::Sha256(hash.to_lowercase()),
            _ => return Err(ConfigError::KeySecretShape(k.id)),
        };
        if k.allow_models.is_empty() {
            return Err(ConfigError::EmptyAllowlist(k.id));
        }
        // Note: an allowlist entry need NOT be a currently-configured model.
        // The allowlist is authorization policy and is intentionally decoupled
        // from the model registry: a key may be authorized for a model that the
        // gateway does not (yet) serve, which is exactly the 404 model_not_found
        // path. Recipe references, by contrast, must resolve (checked below).
        keys.push(KeyConfig {
            id: k.id,
            secret,
            allow_models: k.allow_models,
        });
    }

    // recipes — proposers/aggregator known, quorum bounds.
    let mut recipes = BTreeMap::new();
    for (name, r) in doc.recipes {
        if r.proposers.is_empty() {
            return Err(ConfigError::EmptyProposers(name));
        }
        for p in &r.proposers {
            if !known.contains(p) {
                return Err(ConfigError::UnknownRecipeModel {
                    recipe: name.clone(),
                    role: "proposer",
                    model: p.clone(),
                });
            }
        }
        if !known.contains(&r.aggregator) {
            return Err(ConfigError::UnknownRecipeModel {
                recipe: name.clone(),
                role: "aggregator",
                model: r.aggregator.clone(),
            });
        }
        if r.quorum < 1 || r.quorum > r.proposers.len() {
            return Err(ConfigError::BadQuorum {
                recipe: name.clone(),
                quorum: r.quorum,
                max: r.proposers.len(),
            });
        }
        recipes.insert(
            name.clone(),
            RecipeConfig {
                name,
                proposers: r.proposers,
                aggregator: r.aggregator,
                strategy: r.strategy,
                arm_timeout_ms: r.arm_timeout_ms,
                quorum: r.quorum,
            },
        );
    }

    let server = ServerConfig {
        bind: doc.server.bind,
        port: doc.server.port,
        request_timeout_ms: doc.server.request_timeout_ms,
        max_body_bytes: doc.server.max_body_bytes,
        shutdown_grace_ms: doc.server.shutdown_grace_ms,
        default_max_tokens: doc.server.default_max_tokens,
    };

    Ok(RuntimeConfig {
        server,
        keys,
        models,
        recipes,
    })
}
