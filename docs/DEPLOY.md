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

## 4b. Config hot reload (SIGHUP)

Send `SIGHUP` to reload `MOARAY_CONFIG` **without a restart**:

```bash
kill -HUP "$(pidof moaray)"   # or: docker kill --signal=HUP <container>
```

The reload is **state-preserving and all-or-nothing**:

- **All-or-nothing** — the new file is fully validated first. On any error the
  running config is kept and the failure is logged; the server never serves a
  half-applied config (and an invalid edit can't take it down).
- **State preserved** — per-upstream rate-limit buckets and circuit-breaker state
  survive the reload for every upstream whose identity
  (`provider_type|base_url|api_key_env`) is unchanged. A model rename or an
  `upstream_id` relabel does **not** reset limits; changing a base_url or
  `api_key_env` correctly starts a fresh bucket for the new identity.
- **No protection gap** — new upstreams get their bucket/breaker installed
  *before* they become routable, so there is never a "routable upstream without a
  limiter" window, even under concurrent traffic during the swap.
- **In-flight safe** — when a reload removes an upstream, requests already in
  flight on the old config finish normally; the removed upstream's state is
  garbage-collected only after a drain window.

**Hot fields** (take effect immediately on reload): `server.request_timeout_ms`,
`server.max_body_bytes`, `server.moa_expose_metadata`, and the whole
`models` / `recipes` / `auth.keys` set. **Restart-only fields** (a reload logs a
warning and keeps the running value): `server.bind`, `server.port`,
`server.shutdown_grace_ms`. Unchanged upstreams keep their warm connection pool
across a reload (only changed models are rebuilt), so editing one model does not
trigger a reconnect storm.

## 5. Resilience knobs in production

- **Rate limiting** — set `auth.keys[].rate_limit` (per tenant/key) and
  `models[].rate_limit` (protect each upstream). The per-upstream bucket is
  **shared by passthrough and MoA arms** resolving to the same upstream identity
  (`provider_type|base_url|api_key_env`), so MoA fan-out — and any aliasing of one
  upstream under different model names — cannot amplify traffic past an upstream's
  cap. (The `upstream_id` field is only a low-cardinality metrics/label name; it
  does not decide which models share a bucket.)
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

### Usage accounting overhead (v0.2-P1)

The script now also runs a third **`moaray+store`** leg — the same gateway with
`server.usage_store` enabled — so the *added* cost of accounting is measured
directly against the store-off gateway leg. Accounting on the hot path is just an
`Arc`-clone + a non-blocking `try_send` onto a bounded channel (the SQLite write
happens off-thread), so the added p95 is **within measurement noise (≈0 ms)** —
the store-on and store-off legs are indistinguishable at this workload. Under
sustained overload the channel sheds rows (`moaray_usage_dropped_total`) rather
than ever slowing a request, so the p95 ceiling is unaffected by store backlog.
Re-run on your own hardware; the `ACCOUNTING COST` line in the report is the
store-vs-gateway delta.
