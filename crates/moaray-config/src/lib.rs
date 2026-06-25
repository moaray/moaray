//! moaray-config — the config schema (on-disk DTOs), validation into a runtime
//! descriptor, and YAML loading. The schema is intentionally strict
//! (`deny_unknown_fields`) and validation is total: downstream crates receive a
//! `RuntimeConfig` whose invariants are already guaranteed.

pub mod error;
pub mod runtime;
pub mod schema;
pub mod validate;

pub use error::ConfigError;
pub use runtime::{KeyConfig, KeySecret, ModelConfig, RecipeConfig, RuntimeConfig, ServerConfig};
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

    #[test]
    fn rejects_unknown_provider_reference_in_allowlist() {
        let y = r#"
auth: {keys: [{id: a, key_env: INBOUND_KEY, allow_models: [ghost]}]}
models:
  - {name: gpt, provider_type: openai-compat, base_url: https://x, api_key_env: OPENAI_KEY}
"#;
        assert!(matches!(reject(y), ConfigError::UnknownAllowModel { .. }));
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
}
