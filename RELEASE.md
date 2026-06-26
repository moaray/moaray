# moaray v0.2.0 — Persistent usage/cost accounting

**Date:** 2026-06-26
**Status:** ✅ Production-ready

v0.2.0 adds a persistent, per-arm request store with cost accounting on top of
the v0.1.0 MoA gateway. Every upstream call (each MoA proposer, the aggregator,
each non-stream passthrough) can be written to a SQLite store with raw token
counts, a price snapshot, and a computed nano-USD cost — behind a `UsageSink`
trait so the hot path only enqueues and never blocks. No functional changes to
the gateway data path beyond this opt-in accounting seam.

## New capability — persistent per-arm request store + cost accounting

- **`UsageSink` trait** (`moaray-core`): the accounting seam. `UsageRecord` DTO
  carries raw token counts + a price snapshot + a computed `cost_nano_usd`; all
  fields are `Send` and **carry no secrets** (no api_key / token value / prompt or
  response text / state_key).
- **`moaray-store` crate** (new): `SqliteSink` writes via a **dedicated OS thread**
  (rusqlite is sync — NOT `tokio::spawn`) draining a `crossbeam` bounded channel.
  SQLite runs **WAL** (`synchronous=NORMAL`, `user_version=1`). Ships `NullSink`
  (default, zero overhead) and `VecSink` (test util). `record()` is `try_send`
  only.
- **Cost model**: per-model `price_{prompt,completion}_per_mtok_usd` (USD/Mtok) →
  integer nano-USD/Mtok at config validate; `compute_cost` is a pure helper using
  i128 intermediates with round-half-up, returning `None` when tokens/price are
  absent. Storing raw tokens + the price snapshot on every row means **cost is
  always recomputable** after the fact.
- **MoA boundary preserved**: `moaray-moa` never learns about sinks. `run()`
  returns `MoaRun` so every post-fan-out path carries the arm outcomes; a failed
  arm is recorded (`status=failed`, cost NULL), never silently dropped.

## Posture

**Best-effort, telemetry-grade visibility — NOT an invoice-grade ledger.** The
hot path only `try_send`s onto a bounded channel drained by the OS-thread writer;
under sustained overload rows are **dropped** (`moaray_usage_dropped_total`)
rather than ever blocking or slowing a user request (**drop-not-block**). Because
raw tokens + the price snapshot are persisted on every row, cost can be
recomputed even if prices change later.

## Acceptance gate (run on `release/v0.2.0`, base `main` @ 13ab87b, 2026-06-26)

| Gate | Result |
|---|---|
| `cargo test --workspace` | ✅ **169 passed**, 0 failed |
| `cargo clippy --workspace --all-targets -- -D warnings` | ✅ clean (no warnings) |
| `docker build .` | ✅ success (rusqlite-bundled compiles in the release image) |
| E2E regression — 6 checkpoints (real outbound request, verify SQLite rows land) | ✅ green (YUJ-6037) |

## Config

```yaml
server:
  usage_store:
    path: ./usage.db        # SQLite file path
    channel_capacity: 4096  # bounded channel depth (try_send; full ⇒ drop)
    batch_size: 64          # writer commit batch size
```

`server.usage_store` is **absent by default ⇒ `NullSink` (zero overhead)**. The
block is **restart-frozen**: a hot reload that changes `usage_store` warns and
keeps the running store (prices themselves remain hot-reloadable). Enabling
accounting is opt-in and does not affect the gateway when absent.

## Known limitations

- **Streaming passthrough is not accounted** (usage tap is non-stream only). It is
  made observable via the `moaray_usage_unaccounted_stream_total` counter.
- **`scripts/load-smoke.sh` baseline measurement needs a fix** (tracked in #14);
  the accounting overhead leg is present but the absolute numbers are not yet a
  trustworthy committed baseline.
- **Invoice-grade durability** (synchronous commit, reconciliation, `/v1/usage`
  endpoint, dashboards, quotas, back-fill) is deferred to **v0.2-P2**.

## Review trail
- Plan: PLAN-full v2 (multica YUJ-5991), APPROVED over two /plan-eng-review rounds
  (YUJ-5996 R1, YUJ-6016 R2).
- Implementation: PR #13 (v0.2-P1 persistent accounting), merged to `main`.
- E2E regression: YUJ-6037 (6 checkpoints, real request → SQLite landing verified).
- All CI green (build-test + docker + octospec-lint).

---

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
