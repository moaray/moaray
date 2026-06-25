//! Registry assembly: turn validated config models into concrete providers.
//!
//! This is the *only* place that depends on `moaray-providers` and decides which
//! adapter implements a given model. Upstream API keys are resolved from the
//! environment here and handed to the provider; they never enter logs.

use std::collections::HashMap;
use std::sync::Arc;

use moaray_config::{ProviderType, RuntimeConfig};
use moaray_core::provider::Provider;
use moaray_providers::{build_client, AnthropicProvider, OpenAiProvider};

/// Build the model-name -> provider map from validated config.
pub fn build_providers(config: &RuntimeConfig) -> HashMap<String, Arc<dyn Provider>> {
    let client = build_client();
    let mut providers: HashMap<String, Arc<dyn Provider>> = HashMap::new();
    for (name, m) in &config.models {
        // Resolve upstream key from env at build time (empty if unset; the
        // upstream will reject, and we never log the value).
        let api_key = std::env::var(&m.api_key_env).unwrap_or_default();
        let provider: Arc<dyn Provider> = match m.provider_type {
            ProviderType::OpenaiCompat => Arc::new(OpenAiProvider::new(
                m.upstream_id.clone(),
                m.base_url.clone(),
                api_key,
                client.clone(),
            )),
            ProviderType::Anthropic => Arc::new(AnthropicProvider::new(
                m.upstream_id.clone(),
                m.base_url.clone(),
                api_key,
                config.server.default_max_tokens,
                client.clone(),
            )),
        };
        providers.insert(name.clone(), provider);
    }
    providers
}
