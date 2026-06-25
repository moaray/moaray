//! moaray — server entrypoint. Loads config, builds the registry, assembles
//! runtime + stateful layers, and serves the axum app with graceful shutdown.

use std::time::Duration;

use moaray::app::{build_router, ServerCtx};
use moaray::runtime::{AppState, Runtime};
use moaray::{observe, registry};

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
    let request_timeout = Duration::from_millis(config.server.request_timeout_ms);
    let max_body_bytes = config.server.max_body_bytes;
    let shutdown_grace = Duration::from_millis(config.server.shutdown_grace_ms);

    let providers = registry::build_providers(&config);
    let runtime = Runtime { config, providers };
    let state = AppState::new(runtime);
    let metrics = observe::init_metrics();

    let ctx = ServerCtx {
        state,
        metrics,
        request_timeout,
        max_body_bytes,
    };
    let router = build_router(ctx);

    let addr = format!("{bind}:{port}");
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("binding {addr}"))?;
    tracing::info!(%addr, "moaray listening");

    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal(shutdown_grace))
        .await
        .context("server error")?;
    Ok(())
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
