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
    BreakerConfig, KeyConfig, KeySecret, ModelConfig, RateLimit, RecipeConfig, RetryConfig,
    RuntimeConfig, ServerConfig, UsageStoreConfig,
};
use crate::schema::{ConfigDoc, RateLimitDoc};

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

fn valid_rate_limit(
    scope: &'static str,
    name: &str,
    doc: Option<RateLimitDoc>,
) -> Result<Option<RateLimit>, ConfigError> {
    match doc {
        None => Ok(None),
        Some(rl) => {
            if rl.rps < 1 {
                return Err(ConfigError::BadRateLimit {
                    scope,
                    name: name.to_string(),
                });
            }
            // burst defaults to rps (one second of capacity) and is floored at
            // rps so a single allowed request can never be starved by burst=0.
            let burst = rl.burst.unwrap_or(rl.rps).max(rl.rps);
            Ok(Some(RateLimit { rps: rl.rps, burst }))
        }
    }
}

/// Convert an optional USD-per-1M-tokens price float into integer nano-USD per
/// 1M tokens (`round(usd * 1e9)`). Rejects NaN/inf and negative prices. `None`
/// passes through as `None` (unpriced).
fn valid_price(
    model: &str,
    field: &'static str,
    value: Option<f64>,
) -> Result<Option<i64>, ConfigError> {
    match value {
        None => Ok(None),
        Some(v) => {
            if !v.is_finite() || v < 0.0 {
                return Err(ConfigError::BadPrice {
                    model: model.to_string(),
                    field,
                    value: v.to_string(),
                });
            }
            Ok(Some((v * 1_000_000_000.0).round() as i64))
        }
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
        // Internal state key (limiter/breaker identity) is ALWAYS derived from the
        // upstream identity triple — never user-set — so a model rename or an
        // `upstream_id` relabel keeps the same bucket, and two aliases of one
        // upstream share it (no per-upstream-cap bypass). The user-facing
        // `upstream_id` is a low-cardinality observability label only (defaults to
        // the model name); it must never key state and never carry base_url.
        let state_key = ModelConfig::derive_state_key(m.provider_type, &base_url, &m.api_key_env);
        let upstream_id = m.upstream_id.clone().unwrap_or_else(|| m.name.clone());
        let rate_limit = valid_rate_limit("model", &m.name, m.rate_limit)?;
        let price_prompt_nano_per_mtok = valid_price(
            &m.name,
            "price_prompt_per_mtok_usd",
            m.price_prompt_per_mtok_usd,
        )?;
        let price_completion_nano_per_mtok = valid_price(
            &m.name,
            "price_completion_per_mtok_usd",
            m.price_completion_per_mtok_usd,
        )?;
        models.insert(
            m.name.clone(),
            ModelConfig {
                name: m.name,
                provider_type: m.provider_type,
                base_url,
                api_key_env: m.api_key_env,
                state_key,
                upstream_id,
                rate_limit,
                max_concurrency: m.max_concurrency,
                price_prompt_nano_per_mtok,
                price_completion_nano_per_mtok,
            },
        );
    }
    let known: BTreeSet<&String> = models.keys().collect();

    // Per-upstream governance consistency: several models may share one upstream
    // identity (`state_key` = provider_type|base_url|api_key_env) and then share a
    // single token bucket / concurrency semaphore / breaker (`StatefulState` keys
    // per `state_key`). If they declare *divergent* `rate_limit` /
    // `max_concurrency`, `reconcile` keeps the first model by name and silently
    // drops the rest — so renaming a model could weaken or disable a safety limit.
    // Reject the ambiguity fail-fast at startup instead of resolving it
    // order-dependently. (Keyed by `state_key`, not the observability
    // `upstream_id`: it is the identity triple that decides bucket sharing.)
    {
        let mut seen: BTreeMap<&str, &ModelConfig> = BTreeMap::new();
        for m in models.values() {
            match seen.get(m.state_key.as_str()) {
                None => {
                    seen.insert(m.state_key.as_str(), m);
                }
                Some(first) => {
                    if first.rate_limit != m.rate_limit {
                        return Err(ConfigError::ConflictingUpstreamGovernance {
                            upstream_id: m.upstream_id.clone(),
                            first: first.name.clone(),
                            second: m.name.clone(),
                            field: "rate_limit",
                        });
                    }
                    if first.max_concurrency != m.max_concurrency {
                        return Err(ConfigError::ConflictingUpstreamGovernance {
                            upstream_id: m.upstream_id.clone(),
                            first: first.name.clone(),
                            second: m.name.clone(),
                            field: "max_concurrency",
                        });
                    }
                }
            }
        }
    }

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
        let rate_limit = valid_rate_limit("key", &k.id, k.rate_limit)?;
        // Note: an allowlist entry need NOT be a currently-configured model.
        // The allowlist is authorization policy and is intentionally decoupled
        // from the model registry: a key may be authorized for a model that the
        // gateway does not (yet) serve, which is exactly the 404 model_not_found
        // path. Recipe references, by contrast, must resolve (checked below).
        keys.push(KeyConfig {
            id: k.id,
            secret,
            allow_models: k.allow_models,
            rate_limit,
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
        moa_expose_metadata: doc.server.moa_expose_metadata,
        breaker: BreakerConfig {
            failure_threshold: doc.server.breaker.failure_threshold,
            open_ms: doc.server.breaker.open_ms,
            half_open_successes: doc.server.breaker.half_open_successes,
        },
        retry: RetryConfig {
            enabled: doc.server.retry.enabled,
            max_retries: doc.server.retry.max_retries,
            backoff_ms: doc.server.retry.backoff_ms,
        },
        usage_store: doc.server.usage_store.map(|u| UsageStoreConfig {
            path: u.path,
            channel_capacity: u.channel_capacity,
            batch_size: u.batch_size,
        }),
    };

    Ok(RuntimeConfig {
        server,
        keys,
        models,
        recipes,
    })
}
