# moaray — Design & Production Spec (v1)

> Status: **draft → freezing for v1**. Owner: Yu. This is the load-bearing spec the
> implementation must satisfy to be called "production-ready v1".

## 0. One-liner

A single Rust binary that is **two gateways in one**, selected by the request's
`model` field:

- **Passthrough mode** — a thin, OpenAI-compatible gateway (route / forward /
  stream / rate-limit / observe). Target: sub-millisecond added overhead, on par
  with what the official litellm-Rust effort advertises.
- **MoA mode** — fan-out the same prompt to several models in parallel, then
  **aggregate / fuse / quorum-judge** into a single, higher-quality answer.

The MoA mode is the differentiator: no existing OSS gateway does parallel
fan-out + aggregation + quality judging. moaray does both, so you can "drop-in
replace your gateway and unlock a quality-boosting MoA mode on top."

## 1. Non-negotiable goals for v1 ("production usable")

1. **OpenAI-compatible HTTP API** — `POST /v1/chat/completions` (non-stream + SSE
   stream), `GET /v1/models`, `GET /healthz`, `GET /metrics`.
2. **Passthrough** to any configured OpenAI-compatible upstream by `model` name,
   with streaming preserved end-to-end.
3. **Native provider adapters** for at least: OpenAI-compatible (covers most CN
   gateways incl. the internal mlamp gateway), Anthropic Messages API. Pluggable
   trait so more can be added.
4. **MoA orchestration**: a configurable recipe = {proposers[], aggregator,
   strategy}. Strategies for v1: `concat-synthesize` (aggregator fuses all
   proposer outputs) and `quorum-judge` (judge picks/merges best). Parallel
   fan-out with per-arm timeout + partial-failure tolerance (proceed if ≥ quorum
   arms succeed).
5. **Config-driven** (file + env), hot-reloadable where safe. No code change to
   add a model/provider/recipe.
6. **Auth**: API-key auth on inbound (bearer), upstream credentials kept
   server-side, never logged. Per-key model allowlist.
7. **Rate limiting**: per-key and per-upstream (token-bucket), plus upstream
   concurrency caps + circuit breaker on repeated upstream failures.
8. **Observability**: structured logs (no secrets), Prometheus `/metrics`
   (latency histograms, per-model req/err counts, MoA arm stats), request id
   propagation.
9. **Resilience**: timeouts, retries with backoff (idempotent paths only),
   graceful shutdown, backpressure.
10. **Quality bar**: unit + integration tests (mock upstreams), `cargo clippy`
    clean, CI green, Docker image, deploy doc. Load-smoke showing passthrough
    overhead target.

## 2. Architecture

```
                 ┌────────────────────────────────────────────┐
  client ──HTTP──▶  axum HTTP layer (auth, rate-limit, reqid)  │
                 │            │ route by `model`                │
                 │   ┌────────┴─────────┐                       │
                 │   ▼                  ▼                        │
                 │ passthrough        MoA orchestrator          │
                 │   │            (fan-out → aggregate → judge)  │
                 │   │                  │ (N parallel arms)      │
                 │   └────────┬─────────┘                        │
                 │            ▼                                  │
                 │   provider layer (trait Provider)            │
                 │   openai-compat | anthropic | ...            │
                 │   shared: conn pool, retry, breaker, limiter │
                 └────────────────────────────────────────────┘
                              │ upstream HTTP (reqwest, streaming)
                              ▼
                    OpenAI / Anthropic / mlamp / qwen / GLM / ...
```

### Crate layout (workspace)

- `moaray` (bin) — server entrypoint, wiring, config load.
- `moaray-core` — types (chat req/resp, streaming chunk), Provider trait,
  router, error model.
- `moaray-providers` — openai-compat + anthropic adapters.
- `moaray-moa` — orchestrator: recipe, fan-out, aggregation strategies, judge.
- `moaray-config` — config schema + load/validate + hot-reload.

(Single binary; split crates for testability and clean boundaries.)

### Key tech

- `tokio` + `axum` (HTTP), `reqwest` (upstream, streaming via `bytes` stream),
  `serde`/`serde_json`, `tower` (middleware: timeout, concurrency-limit),
  `tracing` + `tracing-subscriber` (logs), `metrics` + `metrics-exporter-prometheus`,
  `governor` (rate limit), `tokio-util`/`eventsource` for SSE.
- **No litellm dependency** — clean-room.

## 3. Model routing semantics

- `model: "gpt-5.5"` (or any name mapped in config to an upstream) → passthrough.
- `model: "moa/<recipe>"` or `model: "moa-auto"` → MoA mode; recipe resolved
  from config. Unknown model → 404-style OpenAI error.

## 4. MoA recipe (config)

```yaml
recipes:
  arm-e:                      # the validated "臂E" winner
    proposers: [opus, gpt, gemini, glm]
    aggregator: opus
    strategy: concat-synthesize   # or quorum-judge
    arm_timeout_ms: 60000
    quorum: 3                      # proceed if >=3 proposers return
```

MoA response returns one OpenAI-shaped completion; usage = summed; an optional
`moaray` extension field carries per-arm metadata (model, latency, ok/err) for
debugging (toggle via config; off by default in prod).

## 5. Out of scope for v1 (explicit)

- Billing/cost accounting, multi-tenant dashboards, persistent request store,
  embeddings/image/audio endpoints, web UI. (Revisit in v2.)

## 6. Acceptance (definition of done)

- [ ] `chat/completions` non-stream + stream works for passthrough (openai-compat
      + anthropic) against mock + one real upstream.
- [ ] MoA mode runs the `arm-e` recipe end-to-end, tolerates 1 arm failing,
      returns a fused answer.
- [ ] Auth + per-key allowlist + rate limit + circuit breaker enforced (tested).
- [ ] `/metrics` + structured logs + graceful shutdown.
- [ ] Config hot-reload for models/recipes (no restart).
- [ ] Tests (unit + integration w/ mock upstream), clippy clean, CI green.
- [ ] Dockerfile + docker-compose example + README quickstart + deploy doc.
- [ ] Passthrough overhead load-smoke documented.

## 7. Delivery phases

- **Phase 1 — MVP core**: workspace skeleton, config, axum server, Provider
  trait, openai-compat + anthropic adapters, passthrough (stream + non-stream),
  healthz/metrics, basic auth, tests, CI, Docker.
- **Phase 2 — MoA**: orchestrator, recipes, fan-out, concat-synthesize +
  quorum-judge, partial-failure/quorum, per-arm metrics, tests.
- **Phase 3 — Production hardening**: rate limit + circuit breaker + retries,
  hot-reload, full observability, load-smoke, deploy doc, polish.
