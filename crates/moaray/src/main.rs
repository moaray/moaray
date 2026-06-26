//! moaray — server entrypoint. Loads config, builds the registry, assembles
//! runtime + stateful layers, and serves the axum app with graceful shutdown.
//!
//! Config hot reload (P3-3) is wired here: a `SIGHUP` triggers
//! [`moaray::reload::ConfigReloader::reload`], which re-reads `MOARAY_CONFIG` and
//! atomically swaps in a new `Runtime` while preserving per-upstream
//! limiter/breaker state. A failed reload (invalid config) is logged and the
//! running config is kept (all-or-nothing).

use std::sync::Arc;
use std::time::Duration;

use moaray::app::{build_router, ServerCtx};
use moaray::observe;
use moaray::registry;
use moaray::reload::ConfigReloader;
use moaray::runtime::{AppState, Runtime, StatefulState};
use moaray_providers::build_client;

use anyhow::Context;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let config_path = std::env::var("MOARAY_CONFIG").unwrap_or_else(|_| "config.yaml".to_string());
    let yaml = std::fs::read_to_string(&config_path)
        .with_context(|| format!("reading config from {config_path}"))?;
    let config =
        moaray_config::load_yaml(&yaml).map_err(|e| anyhow::anyhow!("invalid config: {e}"))?;

    let bind = config.server.bind.clone();
    let port = config.server.port;

    // Build the usage-accounting sink + its writer handle BEFORE AppState. The
    // sink goes into shared state (record-only, non-blocking); the writer handle
    // stays local to main() for the post-serve shutdown flush. No `usage_store`
    // configured => a NullSink (zero-overhead) + no handle.
    let (usage_sink, usage_writer): (
        Arc<dyn moaray_core::usage::UsageSink>,
        Option<moaray_store::UsageWriterHandle>,
    ) = match &config.server.usage_store {
        Some(store) => {
            let (sink, handle) = moaray_store::SqliteSink::new(
                &store.path,
                store.channel_capacity,
                store.batch_size,
            )
            .with_context(|| format!("opening usage store at {}", store.path))?;
            tracing::info!(path = %store.path, "usage accounting enabled");
            (Arc::new(sink), Some(handle))
        }
        None => {
            tracing::info!("usage accounting disabled (no server.usage_store configured)");
            (Arc::new(moaray_store::NullSink), None)
        }
    };

    // Build the reload-surviving stateful layer FIRST so the provider registry
    // can be wrapped against the same per-upstream slots the handlers read.
    let stateful = Arc::new(StatefulState::from_config(&config));
    // Persistent upstream client (connection-pool carrier) — shared across hot
    // reloads so unchanged upstreams never reconnect (P3-3 F5).
    let client = build_client();
    let built = registry::build_providers_with(&config, &stateful, &client, None)?;
    let orchestrator = registry::build_orchestrator_from_built(&config, &built);
    let providers = built
        .iter()
        .map(|(n, b)| (n.clone(), b.provider.clone()))
        .collect();
    let runtime = Runtime {
        config,
        providers,
        orchestrator,
    };
    let state = AppState::with_sink(runtime, stateful, usage_sink);
    let metrics = observe::init_metrics();

    // The reloader owns the live state + persistent client + the last build (for
    // F5 diff-reuse on the next reload).
    let reloader = Arc::new(ConfigReloader::new(
        state.clone(),
        client,
        config_path,
        built,
    ));

    let ctx = ServerCtx { state, metrics };
    let router = build_router(ctx);

    let addr = format!("{bind}:{port}");
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("binding {addr}"))?;
    tracing::info!(%addr, "moaray listening");

    // SIGHUP -> hot reload (Unix). A failed reload keeps the running config.
    spawn_reload_on_sighup(reloader);

    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("server error")?;

    // axum's graceful shutdown has finished draining in-flight requests by the
    // time `.await` returns — so flush the enqueued usage rows NOW, bounded by a
    // timeout (detach-on-timeout so a stuck writer can't hang process exit). The
    // old post-serve `sleep(grace)` was dead code (the drain already happened),
    // so it is intentionally gone — only the bounded flush remains.
    if let Some(handle) = usage_writer {
        let flush_timeout = Duration::from_secs(5);
        tracing::info!(
            timeout_ms = flush_timeout.as_millis() as u64,
            "flushing usage store on shutdown"
        );
        handle.flush_and_join(flush_timeout);
    }
    Ok(())
}

/// Listen for `SIGHUP` and run a config reload on each one. Errors are logged and
/// swallowed: an invalid config never takes down the running server.
#[cfg(unix)]
fn spawn_reload_on_sighup(reloader: Arc<ConfigReloader>) {
    tokio::spawn(async move {
        let mut hup = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "failed to install SIGHUP handler; hot reload disabled");
                return;
            }
        };
        loop {
            hup.recv().await;
            tracing::info!("SIGHUP received — reloading config");
            match reloader.reload().await {
                Ok(outcome) => tracing::info!(?outcome, "config reload applied"),
                Err(e) => tracing::error!(
                    error = %e,
                    "config reload failed — keeping the running config (all-or-nothing)"
                ),
            }
        }
    });
}

#[cfg(not(unix))]
fn spawn_reload_on_sighup(_reloader: Arc<ConfigReloader>) {
    tracing::warn!("SIGHUP-based hot reload is only available on Unix");
}

fn init_tracing() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,moaray=info"));
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(filter)
        .with_current_span(false)
        .init();
}

/// Wait for SIGTERM/SIGINT, then resolve immediately. Draining of in-flight
/// requests is handled by axum's `with_graceful_shutdown`; the usage-store flush
/// happens in `main()` AFTER `serve(...).await` returns (i.e. after the drain).
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("install ctrl_c handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("shutdown signal received, draining in-flight requests");
}
