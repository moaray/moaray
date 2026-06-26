---
type: Journal
title: "Journal: phase2-moa-orchestration"
description: MoA orchestrator (fan-out, quorum, concat/judge) delivered in PR #5
tags: ["moa", "orchestration", "quorum", "aggregation"]
timestamp: 2026-06-25T16:50:00Z
slug: phase2-moa-orchestration
upstream: moaray/moaray#5
source: backfill
---

# Journal: phase2-moa-orchestration

> Backfilled 2026-06-26 from the dispatch chain (multica YUJ-5942 + plan YUJ-5910)
> and PR #5. Finish-phase journal skipped at delivery; restored here.

## What was done
- MoA orchestrator (`moaray-moa`): parallel fan-out across upstreams, quorum
  tolerance, concat-synthesize + quorum-judge aggregation strategies.
- `chat()` structured path parses typed `ChatResponse` with `usage`; orchestrator
  sums usage (`sum_usage`) and tracks per-arm `ArmOutcome`.
- Partial-failure handling: a failed arm is recorded, never silently dropped
  (rule `moa-partial-failure`). Per-arm metrics.

## Load-bearing touched
`moa`, `orchestration`, `quorum`, `aggregation` — rule `moa-partial-failure` applies.

## Learning / gotcha
- `ArmOutcome` currently carries `usage_present: bool` only, **no token counts** —
  v0.2-P1 cost accounting needs to extend this to carry actual prompt/completion
  tokens per arm.
- Orchestrator early-returns `Err` on quorum/aggregation failure, which would drop
  proposer accounting rows — flagged as a defect to fix in v0.2-P1 (error path must
  still return outcomes).
