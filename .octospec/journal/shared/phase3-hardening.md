---
type: Journal
title: "Journal: phase3-hardening"
description: Production hardening (rate limit, circuit breaker, retry, observability, load-smoke) in PR #7
tags: ["rate-limiting", "circuit-breaker", "retry", "observability", "load-smoke", "resilience"]
timestamp: 2026-06-25T18:16:00Z
slug: phase3-hardening
upstream: moaray/moaray#7
source: backfill
---

# Journal: phase3-hardening

> Backfilled 2026-06-26 from multica YUJ-5910 (Phase3) and PR #7. Scope excluded
> P3-3 hot-reload (P0, delivered separately in PR #8). Finish journal restored here.

## What was done
- **P3-1 Rate limiting**: per-key + per-upstream token bucket (governor),
  per-upstream concurrency cap (Semaphore), backpressure. MoA arm + passthrough
  share the same per-upstream bucket (by upstream identity) → MoA amplification
  bounded by the same 429.
- **P3-2 Circuit breaker**: per-upstream open/half-open/closed + retry backoff.
  Retries default to connection failures / not-yet-sent requests only;
  already-dispatched generation + streaming never retried (no double-billing).
- **P3-4 Observability**: passthrough/MoA bucketed latency histograms, per-model
  req/err, MoA arm metrics, full-chain request-id propagation. No high-cardinality
  / secret labels.
- **P3-5 Load-smoke + deploy doc**: reproducible oha benchmark + deploy guide
  (env, health check, SIGTERM drain).

## Load-bearing touched
`rate-limiting`, `circuit-breaker`, `retry`, `observability`, `resilience` —
rules `no-secret-logging` (P95), `streaming-passthrough`.

## Learning / gotcha
- "no double-billing" is a load-bearing invariant: streaming + already-dispatched
  requests must never be retried. Any future change to the retry path must preserve this.
- SIGTERM drain semantics established here are reused by P3-3 hot-reload and must be
  reused by v0.2-P1 accounting shutdown flush.
