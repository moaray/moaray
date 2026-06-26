---
type: Task
title: "Task: v0.2-usage-accounting"
description: Persistent per-arm request store + cost accounting (rusqlite behind UsageSink, dedicated OS-thread writer, nano-USD cost)
tags: ["usage-accounting", "cost", "persistence", "observability", "moa", "no-secret-logging"]
timestamp: 2026-06-26T00:00:00Z
slug: v0.2-usage-accounting
upstream: moaray/moaray#12
source: self
---

# Task: v0.2-usage-accounting

> moaray v0.2-P1. Load-bearing spec: PLAN-full v2 (multica YUJ-5991, 3rd comment),
> APPROVED via two rounds of /plan-eng-review (YUJ-5996 R1, YUJ-6016 R2). Drift
> stamp re-pinned to c5854e4 (2 docs-only commits past the plan's 8683be2; no .rs
> touched, so all file:line refs stand). Execute the plan faithfully.

## Goal
Persist one row per upstream call (each MoA proposer, the aggregator, each
passthrough) into a SQLite store with raw token counts + a price snapshot +
a computed `cost_nano_usd`, behind a `UsageSink` trait so the hot path only
enqueues (never blocks). Best-effort, telemetry-grade visibility — NOT an
invoice-grade ledger: under overload the accounting channel sheds rows rather
than ever slowing a user request.

## Background
Two response paths: passthrough (raw bytes, non-stream taps usage) and MoA
structured (orchestrator sums usage, tracks ArmOutcome). Emission seam = the app
handlers (`crates/moaray/src/app.rs`). The MoA boundary is load-bearing:
`moaray-moa` must NOT learn about sinks.

## Load-bearing list
- `no-secret-logging` (P95): no api_key/token/prompt-or-response text/state_key in any row or log.
- `streaming-passthrough`: relay stays verbatim/unbuffered (usage tap is non-stream only, inside `collect_response`).
- `moa-partial-failure`: a failed arm is recorded (status=failed, cost NULL), never silently dropped.
- `run()` signature change → `MoaRun` (5 named sites only: orchestrator + its unit tests + tests/trace.rs:129 + run_moa + moa_metadata).
- sync rusqlite on a dedicated OS thread (NOT tokio::spawn); crossbeam bounded channel.

## Out of scope
- `/v1/usage` endpoint, dashboards, quotas, back-fill, invoice reconciliation → v0.2-P2.
- Streaming-passthrough accounting (deferred; made observable via a counter).
- Failed non-stream passthrough books no row (no tokens to count).

## Acceptance
- G1-G9 of plan v2 §3 (loop-uncheatable: manufacture a real billing event first, then assert rows + cost>0 + per-arm separation via injected VecSink).
- `cargo test --workspace` + `cargo clippy --workspace --all-targets -- -D warnings` + `cargo fmt --check` green.
- Docker build (rusqlite-bundled compiles in release image).
