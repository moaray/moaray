# moaray

> Open-source **Mixture-of-Agents (MoA) gateway**, written in Rust.

A dual-mode AI gateway:

- **Passthrough mode** — a thin, fast OpenAI-compatible gateway (route / forward / rate-limit). Sub-millisecond overhead target.
- **MoA mode** — fan-out to multiple models in parallel, then aggregate / fuse / quorum-judge the results into a single higher-quality answer.

One Rust binary, two paths, split by the request's `model` field:

```
model: "gpt-5.5"      -> passthrough (traditional gateway)
model: "moa/<recipe>" -> orchestration (fan-out + aggregate)
```

## Why

Today's open-source LLM gateways all do routing + failover. None do **parallel fan-out + aggregation + quality judging**. `moaray` fills that gap — drop-in replace your gateway, and unlock a quality-boosting MoA mode on top.

## Quickstart (Docker Compose)

Phase 1 ships the passthrough gateway with a self-contained mock upstream, so
you can try it end-to-end with no real API keys:

```bash
# build + start moaray and the bundled mock-upstream
docker compose up --build

# health check
curl -s http://localhost:8080/healthz          # -> ok

# passthrough chat completion (auth via the inbound bearer key from compose)
curl -s http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer sk-local-dev" \
  -H "Content-Type: application/json" \
  -d '{"model":"mock-gpt","messages":[{"role":"user","content":"hi"}]}'

# streaming (SSE) — frames relayed end-to-end
curl -N http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer sk-local-dev" \
  -H "Content-Type: application/json" \
  -d '{"model":"mock-gpt","stream":true,"messages":[{"role":"user","content":"hi"}]}'
```

`config.example.yaml` is the reference config — secrets are referenced by env
var, never inlined. Point moaray at your own upstreams by adding entries under
`models:` and the matching `api_key_env` environment variables.

## Build & test (local)

```bash
cargo build --workspace
cargo test --workspace
cargo clippy --workspace -- -D warnings
```

## Status

🚧 Early. Phase 1 (passthrough MVP), Phase 2 (MoA orchestration —
fan-out + concat-synthesize / quorum-judge + quorum tolerance), and most of
Phase 3 production hardening (per-key + per-upstream rate limiting, per-upstream
concurrency caps, circuit breaker, conservative retry, full Prometheus
observability, load-smoke + deploy doc) are implemented. Config hot-reload is the
remaining Phase 3 item. See `DESIGN.md` for the full spec and `docs/DEPLOY.md`
for deployment + the passthrough overhead baseline.

## License

Apache-2.0

