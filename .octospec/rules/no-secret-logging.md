---
type: Rule
title: Never log or serialize credentials
description: Upstream API keys and inbound bearer tokens must never appear in logs, errors, traces, or responses.
tags: ["security", "logging"]
timestamp: 2026-06-25T00:00:00Z
id: no-secret-logging
tier: repo
priority: 95
load_bearing: true
inject_when:
  paths: ["**/*.rs"]
  touches: ["auth", "provider", "logging", "config"]
source: self
supersedes: []
---

# Never log or serialize credentials

Upstream provider keys and inbound API keys are load-bearing secrets.

- Never `tracing::*!` a value that holds a key/token; redact to `***` or omit.
- `Debug`/`Display` impls on config/auth structs must not print secret fields
  (use `#[derive]` carefully or hand-impl with redaction).
- Never return upstream credentials in any HTTP response or error body.
- Never write secrets into metrics labels.

## Check

> grep the diff: no key/token field reaches a log macro, a serialized response,
> or a metric label. Secret-bearing structs redact in Debug.
