---
type: Rule
title: Errors use the OpenAI-compatible error envelope
description: All HTTP error responses must serialize to the OpenAI error shape so clients/SDKs stay compatible.
tags: ["api", "errors", "compat"]
timestamp: 2026-06-25T00:00:00Z
id: error-envelope
tier: repo
priority: 80
load_bearing: true
inject_when:
  paths: ["**/handler*.rs", "**/http*.rs", "**/api*.rs", "**/error*.rs"]
  touches: ["error-response", "api"]
source: self
supersedes: []
---

# Errors use the OpenAI-compatible error envelope

moaray is a drop-in OpenAI-compatible gateway. Every error returned to a client
must serialize as:

```json
{"error": {"message": "...", "type": "...", "code": "...", "param": null}}
```

- Map internal error variants to this envelope in one place (an `IntoResponse`
  impl), never write ad-hoc JSON from a handler.
- Preserve upstream provider error semantics where reasonable (status + type).

## Check

> Any handler returning an error path produces the envelope above, via the
> shared error type — not a raw string or bespoke JSON.
