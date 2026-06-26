---
type: Learning
title: "Learning: separate state-key from observability-id for per-upstream runtime state"
description: High-entropy state keys must not double as low-cardinality telemetry labels
tags: ["observability", "config", "hot-reload", "resilience"]
timestamp: 2026-06-26T05:42:00Z
slug: state-key-vs-observability-id
source: phase3-hot-reload
status: pending
---

# Learning: separate state-key from observability-id

## Context
In P3-3 hot-reload (PR #8), `upstream_id` originally served two roles: it keyed
the per-upstream StatefulState (limiter/semaphore/breaker) AND was emitted as a
Prometheus label + client-facing identity. These two roles have opposite
requirements.

## The rule (candidate)
When a runtime value keys mutable per-entity state across a config reload, derive
the **state key from identity-defining fields** (e.g. `provider_type|base_url|api_key_env`)
so that a rename preserves state, while keeping a **separate low-cardinality id**
for telemetry labels and client responses. Never let one field do both:
- A high-entropy state key as a Prometheus label → cardinality explosion.
- A low-cardinality display id as a state key → state lost / wrongly shared on rename.

Corollary (no-secret-logging): key on the env-var *name* (`api_key_env`), never the
secret value.

## Promotion note
Promotion into `rules/` is a separate reviewed PR. This candidate sits in
`learnings/pending/` until reviewed.
