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
    let shutdown_grace = Duration::from_millis(config.server.shutdown_grace_ms);

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
    let state = AppState::with_stateful(runtime, stateful);
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
        .with_graceful_shutdown(shutdown_signal(shutdown_grace))
        .await
        .context("server error")?;
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

/// Wait for SIGTERM/SIGINT, then allow a bounded drain window.
async fn shutdown_signal(grace: Duration) {
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
    tracing::info!(
        grace_ms = grace.as_millis() as u64,
        "shutdown signal received, draining"
    );
    // axum's graceful shutdown stops accepting new conns and waits for in-flight
    // ones; we cap the wait so a stuck upstream can't block shutdown forever.
    tokio::time::sleep(grace).await;
}
