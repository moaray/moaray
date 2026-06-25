---
type: Rule
title: Preserve SSE streaming end-to-end
description: Streamed responses must be forwarded chunk-by-chunk without buffering the full body.
tags: ["streaming", "performance"]
timestamp: 2026-06-25T00:00:00Z
id: streaming-passthrough
tier: repo
priority: 85
load_bearing: true
inject_when:
  paths: ["**/passthrough*.rs", "**/provider*.rs", "**/stream*.rs", "**/handler*.rs"]
  touches: ["streaming", "passthrough"]
source: self
supersedes: []
---

# Preserve SSE streaming end-to-end

When the client requests `stream: true`, moaray must relay upstream SSE chunks as
they arrive — never collect the whole upstream body into memory and re-emit.

- Use a streaming body (e.g. `reqwest` bytes stream → axum `Body`/SSE), bounded
  by backpressure, not `String`/`Vec<u8>` accumulation.
- Propagate the terminal `data: [DONE]` and upstream errors mid-stream.
- The MoA aggregator MAY buffer proposer outputs (it must, to fuse) — this rule
  governs passthrough and the final aggregated emission, not internal arm reads.

## Check

> Passthrough stream path contains no full-body buffering; chunks flow through a
> stream type. Integration test asserts first-chunk latency << full completion.
