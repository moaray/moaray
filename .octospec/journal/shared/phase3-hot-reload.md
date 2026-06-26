---
type: Journal
title: "Journal: phase3-hot-reload"
description: State-preserving SIGHUP config hot reload (P0) delivered in PR #8
tags: ["hot-reload", "config", "rate-limiting", "circuit-breaker", "resilience", "observability"]
timestamp: 2026-06-26T01:53:00Z
slug: phase3-hot-reload
upstream: moaray/moaray#8
source: backfill
---

# Journal: phase3-hot-reload

> Backfilled 2026-06-26 from multica YUJ-5951, design YUJ-5948 (AuditBot
> plan-eng-review), and PR #8. P0 unit; Finish journal restored here.

## What was done
- State-preserving SIGHUP reload. `state_key = provider_type|base_url|api_key_env`
  keys the StatefulState DashMap (limiter/semaphore/breaker); `upstream_id` stays
  low-cardinality for Prometheus labels + client responses.
- All-or-nothing config validation (invalid → keep old Runtime, stay alive).
- Added/removed/unchanged upstream diff; unchanged state preserved in place
  (Arc reuse); ArcSwap runtime swap; removed-upstream state GC deferred until
  in-flight drains.
- 7 acceptance criteria covered by dedicated tests `accept1..accept7`.

## Load-bearing touched
`hot-reload`, `config`, `rate-limiting`, `circuit-breaker`, `resilience`,
`observability` — rule `no-secret-logging` (P95).

## Learning / gotcha
- **P0-1 design hole caught pre-impl**: `upstream_id` originally double-dutied as
  both state key and observability label. Splitting `state_key` (high-entropy, keys
  StatefulState) from `upstream_id` (low-cardinality, for labels/responses) was the
  load-bearing fix. This is the canonical example of a STOP-on-hole interception —
  the design review flagged it before any code was written.
- no-secret-logging is satisfied by keying state on `api_key_env` (the env var name)
  not the secret value.
