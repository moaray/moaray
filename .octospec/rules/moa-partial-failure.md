---
type: Rule
title: MoA tolerates partial arm failure via quorum
description: A MoA request must not fail just because one proposer arm errors or times out, as long as quorum is met.
tags: ["moa", "resilience"]
timestamp: 2026-06-25T00:00:00Z
id: moa-partial-failure
tier: repo
priority: 90
load_bearing: true
inject_when:
  paths: ["**/moa*.rs", "**/orchestrat*.rs", "**/aggregat*.rs"]
  touches: ["moa", "orchestration", "quorum"]
source: self
supersedes: []
---

# MoA tolerates partial arm failure via quorum

Fan-out arms fail independently (timeout, upstream 5xx, rate limit). The
orchestrator proceeds when the number of successful proposers >= recipe `quorum`.

- Each arm has its own `arm_timeout_ms`; a slow/failed arm must not block others.
- If successful arms < quorum, return a clear gateway error (envelope), not a
  partial silent result.
- Per-arm outcome (model, latency, ok/err) is recorded for metrics; arm errors
  never leak upstream raw error bodies to the client.

## Check

> Test: with N proposers and 1 forced to fail, response still returns when
> survivors >= quorum; with survivors < quorum, returns the error envelope.
