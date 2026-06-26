---
type: Journal
title: "Journal: phase1-mvp-core"
description: MVP passthrough OpenAI/Anthropic-compatible gateway delivered in PR #3
tags: ["passthrough", "provider", "streaming", "auth", "config"]
timestamp: 2026-06-25T13:50:00Z
slug: phase1-mvp-core
upstream: moaray/moaray#3
source: backfill
---

# Journal: phase1-mvp-core

> Backfilled 2026-06-26 from the dispatch chain (multica YUJ-5914 + plan YUJ-5910)
> and PR #3. The Finish phase journal was skipped at delivery time; this record
> restores it for team visibility.

## What was done
- Workspace skeleton: `crates/{moaray,moaray-core,moaray-providers,moaray-moa,moaray-config}` + `tools/mock-upstream`.
- `Provider` trait (`moaray-core`) + openai-compat and anthropic adapters (`moaray-providers`).
- Passthrough path (stream + non-stream): openai-compat forwards raw bytes verbatim
  and never parses `usage` (sub-ms goal); anthropic non-stream parses+translates.
- axum server with `/healthz`, `/metrics`, basic auth, config loading.
- Tests + CI + Docker.

## Load-bearing touched
`passthrough`, `provider`, `streaming`, `auth`, `config` — rules `no-secret-logging`,
`streaming-passthrough` apply.

## Learning / gotcha
- The anthropic adapter's `anthropic_to_openai` **always emits a `usage` object,
  defaulting missing upstream usage to `(0,0)`** — so "usage absent" is erased to
  zeros before the app sees it. This later became a known limitation for v0.2-P1
  cost accounting (anthropic passthrough cannot distinguish absent vs zero usage).
