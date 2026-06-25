# moaray — Deployment Guide

Production deployment, configuration, health checks, graceful shutdown, and the
load-smoke overhead baseline. Pair this with `DESIGN.md` (the spec) and
`config.example.yaml` (annotated config).

## 1. Build & run

### Docker (recommended)

```bash
docker build -t moaray:latest .
docker run --rm -p 8080:8080 \
  -e MOARAY_CONFIG=/app/config.yaml \
  -e MOARAY_INBOUND_KEY=sk-... \
  -e OPENAI_KEY=sk-... \
  -v $(pwd)/config.yaml:/app/config.yaml:ro \
  moaray:latest
```

The image runs as a non-root user (`uid 10001`) and contains both `moaray` and
the `mock-upstream` helper. `docker compose up` brings up moaray + the bundled
mock upstream for a self-contained smoke test.

### Binary

```bash
cargo build --release -p moaray
MOARAY_CONFIG=/etc/moaray/config.yaml ./target/release/moaray
```

## 2. Configuration

Config is a single YAML file referenced by `MOARAY_CONFIG` (default
`config.yaml`). Secrets are **never inlined** — keys are referenced by env var
(`key_env` / `api_key_env`) or pinned as a `sha256` digest. See
`config.example.yaml` for every knob; the production-relevant ones:

| Section | Key | Meaning |
|---|---|---|
| `server` | `bind` / `port` | listen address (restart-only) |
| `server` | `request_timeout_ms` | per-request deadline → 504 on breach |
| `server` | `max_body_bytes` | inbound body cap → 413 |
| `server` | `shutdown_grace_ms` | bounded drain window on SIGTERM (see §4) |
| `server.breaker` | `failure_threshold` / `open_ms` / `half_open_successes` | per-upstream circuit breaker |
| `server.retry` | `enabled` / `max_retries` / `backoff_ms` | upstream retry (off by default; connect-failures only) |
| `auth.keys[].rate_limit` | `rps` / `burst` | inbound per-key token bucket → 429 `rate_limited` |
| `models[].rate_limit` | `rps` / `burst` | per-upstream token bucket (shared by passthrough + MoA) |
| `models[].max_concurrency` | | per-upstream in-flight ceiling |

### Environment variables

- `MOARAY_CONFIG` — path to the config file.
- `RUST_LOG` — tracing filter (e.g. `info,moaray=debug`); logs are JSON.
- Any env var named by a `key_env` / `api_key_env` in the config — the inbound
  bearer tokens and upstream credentials. **These are the only place secrets
  live; they never enter logs or `/metrics` labels.**

## 3. Health checks & observability

- **`GET /healthz`** — liveness/readiness probe, returns `200 ok`, unauthenticated.
  Use it for the Kubernetes `livenessProbe` and `readinessProbe`.
- **`GET /metrics`** — Prometheus text, unauthenticated. Exposes:
  - `moaray_requests_total{path,model,status_class}` and `moaray_errors_total{...}`
  - `moaray_request_duration_seconds{path,model}` — latency histogram bucketed by
    **`path` = `passthrough` | `moa`**.
  - `moaray_moa_arm_total{model,upstream_id,status_class}` +
    `moaray_moa_arm_duration_seconds{model,upstream_id}` — per-arm MoA stats.
  - All labels are low-cardinality and non-secret: **no** request-id, API key,
    raw upstream URL, or error-string ever becomes a label.
- **request-id** — accepted from inbound `x-request-id` (or minted as a UUID),
  echoed on the response, and propagated to the upstream `x-request-id` header
  for end-to-end tracing.

Example Kubernetes probes:

```yaml
livenessProbe:
  httpGet: { path: /healthz, port: 8080 }
  initialDelaySeconds: 3
readinessProbe:
  httpGet: { path: /healthz, port: 8080 }
```

## 4. Graceful shutdown (SIGTERM drain)

On `SIGTERM`/`SIGINT` moaray stops accepting new connections and drains in-flight
requests for a **bounded** window (`server.shutdown_grace_ms`, default 15s).
After the window it cancels still-pending futures and closes upstream
connections rather than blocking forever — so a stuck upstream cannot wedge a
rolling deploy past the grace period.

Set `terminationGracePeriodSeconds` in Kubernetes to **≥ `shutdown_grace_ms`**
so the orchestrator does not `SIGKILL` mid-drain:

```yaml
spec:
  terminationGracePeriodSeconds: 20   # >= shutdown_grace_ms/1000
```

Rolling restart: scale the new ReplicaSet up, wait for `readinessProbe`, then let
the old pods receive SIGTERM and drain.

## 5. Resilience knobs in production

- **Rate limiting** — set `auth.keys[].rate_limit` (per tenant/key) and
  `models[].rate_limit` (protect each upstream). The per-upstream bucket is
  **shared by passthrough and MoA arms** resolving to the same `upstream_id`, so
  MoA fan-out cannot amplify traffic past an upstream's cap.
- **Concurrency** — `models[].max_concurrency` caps in-flight requests per
  upstream; over-cap requests queue on a semaphore (and are cancelled on client
  disconnect / timeout).
- **Circuit breaker** — per upstream; opens after `failure_threshold` consecutive
  failures, fails fast with `503 circuit_open`, then half-open probes recover it.
- **Retry** — **off by default.** Even when enabled, retries apply *only* to
  connection failures that happened before the request was sent
  (`upstream_error` from a connect/DNS/TLS failure); a generation request that
  reached the upstream is never retried (no double-charge), and streaming is
  never retried.

## 6. Load-smoke: passthrough added-overhead baseline

`scripts/load-smoke.sh` is a reproducible benchmark of the *added* latency of
routing through moaray vs hitting the upstream directly. It is fully fixed for
reproducibility (override via env for experiments):

- tool: **oha** (`cargo install oha`), JSON output
- concurrency: **50**, duration: **20s/leg**, warmup: **3s** (discarded)
- payload: fixed non-streaming chat request (`scripts/payload.json`)
- upstream: bundled `mock-upstream` with a fixed injected delay
  (`MOCK_DELAY_MS=20`) so the delta reflects moaray's cost, not upstream jitter

Run it:

```bash
cargo install oha          # one-time
./scripts/load-smoke.sh
```

### Baseline result

Measured on the reference dev host (release build, loopback, conditions above):

| leg | p50 (ms) | p95 (ms) |
|---|---|---|
| direct (to mock upstream) | 21.215 | 21.992 |
| via moaray | 21.501 | 22.312 |
| **added overhead** | **0.286** | **0.320** |

**moaray adds ~0.29 ms p50 / ~0.32 ms p95** over a direct upstream call —
sub-millisecond, on target with the DESIGN goal. Numbers are
machine/load-dependent; re-run `scripts/load-smoke.sh` on your own hardware for a
local baseline. The measurement conditions are printed in the script's report
header so any result is self-describing.
