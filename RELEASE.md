# moaray v0.1.0 — Production Release

**Date:** 2026-06-26
**Status:** ✅ Production-ready (v1)

moaray is an open-source **Mixture-of-Agents (MoA) gateway** in Rust: an
OpenAI/Anthropic-compatible reverse proxy that can either passthrough a request
to a single upstream or fan it out across multiple upstreams and aggregate the
results (quorum / concat / judge), with production-grade rate limiting, circuit
breaking, observability, and zero-downtime config hot reload.

## Acceptance gate (run on `main` @ 5e3039f, 2026-06-26)

| Gate | Result |
|---|---|
| `cargo test --workspace` | ✅ **136 passed**, 0 failed |
| `cargo clippy --workspace --all-targets -- -D warnings` | ✅ clean (no warnings) |
| `docker build .` | ✅ success |
| `scripts/load-smoke.sh` (oha, c=50, 20s, mock delay 20ms) | ✅ reproducible; moaray self-latency p50 **0.32 ms** / p95 **1.1 ms** |
| Committed overhead baseline (deploy doc) | added overhead ~**0.29 ms p50 / 0.32 ms p95** |

## What shipped

### Phase 1 — MVP core (PR #3)
- OpenAI/Anthropic-compatible passthrough gateway
- Provider abstraction (OpenAI, Anthropic), error envelope, request-id

### Phase 2 — MoA orchestration (PR #5)
- Fan-out across upstreams; quorum / concat / judge aggregation strategies
- MoA arm stats

### Phase 3 — Production hardening
- **P3-1 Rate limiting** (PR #7): per-key + per-upstream token bucket (governor),
  per-upstream concurrency cap (Semaphore), backpressure. MoA arm and passthrough
  share the **same per-upstream bucket** (by upstream identity) → MoA amplification
  is bounded by the same 429.
- **P3-2 Circuit breaker** (PR #7): per-upstream open/half-open/closed state
  machine + retry backoff. Retries default to connection failures / not-yet-sent
  requests only; **already-dispatched generation requests are not retried, streaming
  is never retried** (no double-billing).
- **P3-4 Observability** (PR #7): passthrough/MoA bucketed latency histograms,
  per-model req/err, MoA arm metrics, full-chain request-id propagation. No
  high-cardinality / secret labels.
- **P3-5 Load-smoke + deploy doc** (PR #7): reproducible benchmark (oha, fixed
  concurrency/payload, mock upstream fixed delay, warmup) + deployment guide
  (env, health check, SIGTERM drain).
- **P3-3 Config hot reload** (PR #8, P0): state-preserving SIGHUP reload.
  - `state_key = provider_type|base_url|api_key_env` keys the StatefulState
    DashMap (limiter/semaphore/breaker); `upstream_id` stays low-cardinality for
    Prometheus labels + client responses.
  - All-or-nothing config validation (invalid → keep old Runtime, stay alive).
  - Added/removed/unchanged upstream diff; unchanged state preserved in place
    (Arc reuse); ArcSwap runtime swap; removed-upstream state GC deferred until
    in-flight drains.
  - All **7 acceptance criteria** covered by dedicated tests `accept1..accept7`
    (rate-limit/breaker survive reload, identity-preserving rename, alias bucket
    sharing, swap-race no-provider-without-limiter, in-flight survival, live
    max-body, invalid-config keep-old-runtime).

## Review trail
- Design reviews: AuditBot YUJ-5948 (P3-3 design, done).
- Implementation: Titan YUJ-5949 (P3-1/2/4/5), YUJ-5951 (P3-3).
- codex review on PR #8: 4 rounds, clean.
- All CI checks green on both PRs (build-test + docker + octospec-lint).

## Deploy
See `docs/deploy.md` (deployment, config, health checks, graceful shutdown,
load-smoke overhead baseline) and `config.example.yaml`.
