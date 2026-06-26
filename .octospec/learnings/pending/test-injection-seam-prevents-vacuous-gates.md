---
type: Learning
title: "Learning: a sink/store needs a test injection seam or its acceptance gates pass vacuously"
description: Default no-op sinks make 'rows landed' assertions true on the empty set; inject an observable test double via a constructor seam
tags: ["testing", "usage-accounting", "observability", "acceptance-gates"]
timestamp: 2026-06-26T08:00:00Z
slug: test-injection-seam-prevents-vacuous-gates
source: v0.2-usage-accounting
status: pending
---

# Learning: a sink/store needs a test injection seam or its gates go vacuous

## Context
v0.2-P1 usage accounting (PR for #12) writes one row per upstream call behind a
`UsageSink` trait. The production default when no store is configured is a
`NullSink` whose `record` is a no-op. Every acceptance gate is phrased as "after a
real request, assert N rows landed with cost > 0". With only `NullSink` reachable
from the app, those assertions would pass **on the empty set** — a green that
proves nothing. The plan's first /plan-eng-review flagged exactly this (R1 P1-①)
as GO-blocking.

## The rule (candidate)
When a feature's output sinks into a swappable component (sink/store/exporter)
whose default impl is a no-op or external, add a **constructor injection seam**
(`AppState::with_sink(..., Arc<dyn Trait>)`) and write the acceptance tests
against an **observable in-memory double** (`VecSink` with a `rows()` accessor).
The seam is not a convenience — it is what makes the gate non-vacuous. A gate that
can be satisfied by the empty set is not a gate.

## Corollary — counter gates
Process-global metric recorders (a `OnceLock` Prometheus handle) make a bare
`counter > 0` satisfiable by an earlier test in the same binary. Scrape
before/after and assert the **delta**, and parse an absent series as 0 (a counter
with no `describe_counter!` is absent from `/metrics` until first incremented, not
rendered as `0`).
