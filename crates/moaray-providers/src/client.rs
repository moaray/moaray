//! Shared upstream HTTP client construction.
//!
//! Security defaults that every provider inherits:
//! - automatic redirects **disabled** (an upstream must not bounce us to an
//!   attacker-controlled host that would receive the injected credential),
//! - TLS certificate verification **on** (rustls, no danger flags),
//! - a connection pool with sane idle settings for passthrough throughput.

use std::time::Duration;

use reqwest::Client;

/// Build the shared upstream `reqwest::Client`.
pub fn build_client() -> Client {
    Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .pool_idle_timeout(Duration::from_secs(90))
        .pool_max_idle_per_host(32)
        .tcp_nodelay(true)
        .build()
        .expect("reqwest client builds with static config")
}
