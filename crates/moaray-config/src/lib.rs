//! moaray-config — the config schema (on-disk DTOs), validation into a runtime
//! descriptor, and YAML loading. The schema is intentionally strict
//! (`deny_unknown_fields`) and validation is total: downstream crates receive a
//! `RuntimeConfig` whose invariants are already guaranteed.

pub mod error;
pub mod runtime;
pub mod schema;
pub mod validate;

pub use error::ConfigError;
pub use runtime::{
    BreakerConfig, KeyConfig, KeySecret, ModelConfig, RateLimit, RecipeConfig, RetryConfig,
    RuntimeConfig, ServerConfig,
};
pub use schema::{ConfigDoc, ProviderType, Strategy};
pub use validate::{validate, EnvSource, OsEnv};

/// Parse YAML into a [`ConfigDoc`] (no validation).
pub fn parse_yaml(yaml: &str) -> Result<ConfigDoc, ConfigError> {
    serde_yaml::from_str(yaml).map_err(|e| ConfigError::Parse(e.to_string()))
}

/// Parse + validate YAML into a [`RuntimeConfig`] using the OS environment.
pub fn load_yaml(yaml: &str) -> Result<RuntimeConfig, ConfigError> {
    let doc = parse_yaml(yaml)?;
    validate(doc, &OsEnv)
}

/// Parse + validate YAML using a custom env source (used in tests).
pub fn load_yaml_with_env<E: EnvSource>(yaml: &str, env: &E) -> Result<RuntimeConfig, ConfigError> {
    let doc = parse_yaml(yaml)?;
    validate(doc, env)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct MapEnv(HashMap<String, String>);
    impl EnvSource for MapEnv {
        fn get(&self, key: &str) -> Option<String> {
            self.0.get(key).cloned()
        }
    }

    fn env() -> MapEnv {
        let mut m = HashMap::new();
        m.insert("OPENAI_KEY".to_string(), "sk-up".to_string());
        m.insert("INBOUND_KEY".to_string(), "sk-in".to_string());
        MapEnv(m)
    }

    const VALID: &str = r#"
server:
  port: 9090
  max_body_bytes: 2048
auth:
  keys:
    - id: team-a
      key_env: INBOUND_KEY
      allow_models: [gpt, opus, "moa/arm-e"]
models:
  - name: gpt
    provider_type: openai-compat
    base_url: https://api.openai.com
    api_key_env: OPENAI_KEY
  - name: opus
    provider_type: anthropic
    base_url: https://api.anthropic.com/
    api_key_env: OPENAI_KEY
recipes:
  arm-e:
    proposers: [gpt, opus]
    aggregator: opus
    strategy: concat-synthesize
    quorum: 2
"#;

    #[test]
    fn parses_and_validates_a_full_config() {
        let cfg = load_yaml_with_env(VALID, &env()).expect("valid");
        assert_eq!(cfg.server.port, 9090);
        assert_eq!(cfg.server.max_body_bytes, 2048);
        // MoA debug extension field defaults OFF (production posture)
        assert!(!cfg.server.moa_expose_metadata);
        // base_url trailing slash trimmed
        assert_eq!(cfg.models["opus"].base_url, "https://api.anthropic.com");
        assert_eq!(cfg.models["gpt"].upstream_id, "gpt");
        assert!(cfg.is_known_model("gpt"));
        assert!(!cfg.is_known_model("nope"));
        let r = &cfg.recipes["arm-e"];
        assert_eq!(r.quorum, 2);
        // inbound key plaintext resolved from env
        match &cfg.keys[0].secret {
            KeySecret::Plain(p) => assert_eq!(p, "sk-in"),
            _ => panic!("expected plain"),
        }
        // Debug must not leak the secret
        let dbg = format!("{:?}", cfg.keys[0]);
        assert!(!dbg.contains("sk-in"), "secret leaked in Debug: {dbg}");
        assert!(dbg.contains("***"));
    }

    fn reject(yaml: &str) -> ConfigError {
        load_yaml_with_env(yaml, &env()).expect_err("should reject")
    }

    #[test]
    fn rejects_duplicate_model() {
        let y = r#"
auth: {keys: [{id: a, key_env: INBOUND_KEY, allow_models: [gpt]}]}
models:
  - {name: gpt, provider_type: openai-compat, base_url: https://x, api_key_env: OPENAI_KEY}
  - {name: gpt, provider_type: openai-compat, base_url: https://y, api_key_env: OPENAI_KEY}
"#;
        assert!(matches!(reject(y), ConfigError::DuplicateModel(_)));
    }

    /// P2 (rework R2): two models sharing one `upstream_id` with divergent
    /// `rate_limit` must be rejected at validation — otherwise `reconcile` keeps
    /// the first-by-name model's limit and silently drops the other, so renaming
    /// a model could weaken/disable the safety limit (order-dependent governance).
    #[test]
    fn rejects_conflicting_shared_upstream_rate_limit() {
        let y = r#"
auth: {keys: [{id: a, key_env: INBOUND_KEY, allow_models: [m1]}]}
models:
  - {name: m1, provider_type: openai-compat, base_url: https://x, api_key_env: OPENAI_KEY, upstream_id: shared, rate_limit: {rps: 1, burst: 1}}
  - {name: m2, provider_type: openai-compat, base_url: https://y, api_key_env: OPENAI_KEY, upstream_id: shared, rate_limit: {rps: 100, burst: 200}}
"#;
        assert!(matches!(
            reject(y),
            ConfigError::ConflictingUpstreamGovernance {
                field: "rate_limit",
                ..
            }
        ));
    }

    /// P2 sibling: divergent `max_concurrency` on a shared `upstream_id` is also
    /// rejected (same silent-drop hazard).
    #[test]
    fn rejects_conflicting_shared_upstream_max_concurrency() {
        let y = r#"
auth: {keys: [{id: a, key_env: INBOUND_KEY, allow_models: [m1]}]}
models:
  - {name: m1, provider_type: openai-compat, base_url: https://x, api_key_env: OPENAI_KEY, upstream_id: shared, max_concurrency: 1}
  - {name: m2, provider_type: openai-compat, base_url: https://y, api_key_env: OPENAI_KEY, upstream_id: shared, max_concurrency: 64}
"#;
        assert!(matches!(
            reject(y),
            ConfigError::ConflictingUpstreamGovernance {
                field: "max_concurrency",
                ..
            }
        ));
    }

    /// Two models may still share an `upstream_id` when their per-upstream
    /// governance is identical — that is the legitimate MoA/passthrough sharing
    /// case and must stay accepted (order-independent).
    #[test]
    fn accepts_shared_upstream_with_identical_governance() {
        let y = r#"
auth: {keys: [{id: a, key_env: INBOUND_KEY, allow_models: [m1]}]}
models:
  - {name: m1, provider_type: openai-compat, base_url: https://x, api_key_env: OPENAI_KEY, upstream_id: shared, rate_limit: {rps: 5, burst: 10}, max_concurrency: 8}
  - {name: m2, provider_type: openai-compat, base_url: https://y, api_key_env: OPENAI_KEY, upstream_id: shared, rate_limit: {rps: 5, burst: 10}, max_concurrency: 8}
"#;
        let cfg = load_yaml_with_env(y, &env()).expect("identical shared governance is valid");
        assert_eq!(cfg.models["m1"].upstream_id, "shared");
        assert_eq!(cfg.models["m2"].upstream_id, "shared");
    }

    #[test]
    fn allowlist_may_reference_an_unconfigured_model() {
        // The allowlist is authorization policy, decoupled from the model
        // registry: a key may be authorized for a model the gateway does not
        // currently serve (that request then 404s at runtime, not at load).
        let y = r#"
auth: {keys: [{id: a, key_env: INBOUND_KEY, allow_models: [ghost, gpt]}]}
models:
  - {name: gpt, provider_type: openai-compat, base_url: https://x, api_key_env: OPENAI_KEY}
"#;
        let cfg = load_yaml_with_env(y, &env()).expect("valid");
        assert!(cfg.keys[0].allows("ghost"));
        assert!(!cfg.is_known_model("ghost"));
    }

    #[test]
    fn rejects_empty_allowlist() {
        let y = r#"
auth: {keys: [{id: a, key_env: INBOUND_KEY, allow_models: []}]}
models:
  - {name: gpt, provider_type: openai-compat, base_url: https://x, api_key_env: OPENAI_KEY}
"#;
        assert!(matches!(reject(y), ConfigError::EmptyAllowlist(_)));
    }

    #[test]
    fn rejects_non_http_base_url() {
        let y = r#"
auth: {keys: [{id: a, key_env: INBOUND_KEY, allow_models: [gpt]}]}
models:
  - {name: gpt, provider_type: openai-compat, base_url: "file:///etc/passwd", api_key_env: OPENAI_KEY}
"#;
        assert!(matches!(reject(y), ConfigError::BadBaseUrlScheme { .. }));
    }

    #[test]
    fn rejects_quorum_gt_proposers() {
        let y = r#"
auth: {keys: [{id: a, key_env: INBOUND_KEY, allow_models: [gpt]}]}
models:
  - {name: gpt, provider_type: openai-compat, base_url: https://x, api_key_env: OPENAI_KEY}
recipes:
  bad: {proposers: [gpt], aggregator: gpt, strategy: quorum-judge, quorum: 5}
"#;
        assert!(matches!(reject(y), ConfigError::BadQuorum { .. }));
    }

    #[test]
    fn rejects_unknown_aggregator() {
        let y = r#"
auth: {keys: [{id: a, key_env: INBOUND_KEY, allow_models: [gpt]}]}
models:
  - {name: gpt, provider_type: openai-compat, base_url: https://x, api_key_env: OPENAI_KEY}
recipes:
  bad: {proposers: [gpt], aggregator: ghost, strategy: concat-synthesize, quorum: 1}
"#;
        assert!(matches!(
            reject(y),
            ConfigError::UnknownRecipeModel {
                role: "aggregator",
                ..
            }
        ));
    }

    #[test]
    fn rejects_unknown_top_level_field() {
        let y = r#"
bogus: true
auth: {keys: [{id: a, key_env: INBOUND_KEY, allow_models: [gpt]}]}
models:
  - {name: gpt, provider_type: openai-compat, base_url: https://x, api_key_env: OPENAI_KEY}
"#;
        assert!(matches!(reject(y), ConfigError::Parse(_)));
    }

    #[test]
    fn rejects_both_key_secret_shapes() {
        let y = r#"
auth: {keys: [{id: a, key_env: INBOUND_KEY, key_sha256: "abc", allow_models: [gpt]}]}
models:
  - {name: gpt, provider_type: openai-compat, base_url: https://x, api_key_env: OPENAI_KEY}
"#;
        assert!(matches!(reject(y), ConfigError::KeySecretShape(_)));
    }

    #[test]
    fn parses_rate_limits_breaker_and_retry() {
        let y = r#"
server:
  breaker: {failure_threshold: 3, open_ms: 5000, half_open_successes: 2}
  retry: {enabled: true, max_retries: 1, backoff_ms: 50}
auth:
  keys:
    - id: a
      key_env: INBOUND_KEY
      allow_models: [gpt]
      rate_limit: {rps: 10}
models:
  - name: gpt
    provider_type: openai-compat
    base_url: https://x
    api_key_env: OPENAI_KEY
    rate_limit: {rps: 5, burst: 20}
    max_concurrency: 4
"#;
        let cfg = load_yaml_with_env(y, &env()).expect("valid");
        // server breaker + retry
        assert_eq!(cfg.server.breaker.failure_threshold, 3);
        assert_eq!(cfg.server.breaker.open_ms, 5000);
        assert_eq!(cfg.server.breaker.half_open_successes, 2);
        assert!(cfg.server.retry.enabled);
        assert_eq!(cfg.server.retry.max_retries, 1);
        // per-key limit; burst defaults to rps when omitted
        let kl = cfg.keys[0].rate_limit.expect("key limit");
        assert_eq!(kl.rps, 10);
        assert_eq!(kl.burst, 10);
        // per-upstream limit + concurrency
        let ml = cfg.models["gpt"].rate_limit.expect("model limit");
        assert_eq!(ml.rps, 5);
        assert_eq!(ml.burst, 20);
        assert_eq!(cfg.models["gpt"].max_concurrency, Some(4));
    }

    #[test]
    fn retry_off_and_no_limits_by_default() {
        let cfg = load_yaml_with_env(VALID, &env()).expect("valid");
        assert!(!cfg.server.retry.enabled);
        assert!(cfg.keys[0].rate_limit.is_none());
        assert!(cfg.models["gpt"].rate_limit.is_none());
        assert!(cfg.models["gpt"].max_concurrency.is_none());
        // sensible breaker defaults present even when unspecified
        assert_eq!(cfg.server.breaker.failure_threshold, 5);
    }

    #[test]
    fn rejects_zero_rps_rate_limit() {
        let y = r#"
auth: {keys: [{id: a, key_env: INBOUND_KEY, allow_models: [gpt], rate_limit: {rps: 0}}]}
models:
  - {name: gpt, provider_type: openai-compat, base_url: https://x, api_key_env: OPENAI_KEY}
"#;
        assert!(matches!(
            reject(y),
            ConfigError::BadRateLimit { scope: "key", .. }
        ));
    }
}
