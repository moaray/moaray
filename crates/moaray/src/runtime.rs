//! Runtime state, split into two deliberately decoupled layers.
//!
//! - [`Runtime`] holds the **hot-swappable** config-derived data: the resolved
//!   models/recipes and the built provider registry. It lives behind an
//!   `ArcSwap` so Phase 3 can atomically replace it on config reload without
//!   touching in-flight requests.
//! - [`StatefulState`] holds **per-upstream / per-key** state (rate limiters,
//!   concurrency semaphores, circuit breakers) keyed by stable `upstream_id`.
//!   It must survive a config reload, so it is intentionally NOT part of
//!   `Runtime`. Phase 1 only establishes the layering; the limiter/breaker
//!   internals arrive in Phase 3.

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use moaray_config::RuntimeConfig;
use moaray_core::provider::Provider;

/// Hot-swappable, config-derived runtime data.
pub struct Runtime {
    pub config: RuntimeConfig,
    /// model name -> provider instance.
    pub providers: HashMap<String, Arc<dyn Provider>>,
}

impl Runtime {
    /// Resolve a model name to its provider.
    pub fn provider(&self, model: &str) -> Option<Arc<dyn Provider>> {
        self.providers.get(model).cloned()
    }
}

/// Per-upstream stateful data that must persist across config reloads.
///
/// Phase 1: an empty, stably-keyed container keyed by `upstream_id`. Phase 3
/// fills each entry with a token-bucket limiter, a concurrency semaphore, and a
/// circuit breaker. Crucially this is NOT rebuilt when `Runtime` is hot-swapped.
#[derive(Default)]
pub struct StatefulState {
    /// Reserved per-upstream slots, keyed by stable `upstream_id`.
    /// Populated in Phase 3 (limiter/semaphore/breaker); declared now to lock
    /// the Runtime/StatefulState split.
    #[allow(dead_code)]
    pub per_upstream: HashMap<String, UpstreamState>,
}

/// Reserved per-upstream stateful slot (limiter/semaphore/breaker land here in
/// Phase 3).
#[derive(Default)]
pub struct UpstreamState {}

/// The whole shared application state handed to handlers.
#[derive(Clone)]
pub struct AppState {
    pub runtime: Arc<ArcSwap<Runtime>>,
    /// Persisted across config hot-swaps; consumed by Phase 3 stateful logic.
    #[allow(dead_code)]
    pub stateful: Arc<StatefulState>,
}

impl AppState {
    /// Build app state from an initial runtime.
    pub fn new(runtime: Runtime) -> Self {
        Self {
            runtime: Arc::new(ArcSwap::from_pointee(runtime)),
            stateful: Arc::new(StatefulState::default()),
        }
    }

    /// Load the current runtime snapshot.
    pub fn runtime(&self) -> arc_swap::Guard<Arc<Runtime>> {
        self.runtime.load()
    }
}
