#!/usr/bin/env bash
# load-smoke.sh — reproducible passthrough-overhead benchmark for moaray.
#
# Measures the *added* latency of routing through moaray vs hitting the mock
# upstream directly, under a fixed, documented workload. The goal is a stable,
# repeatable p50/p95 added-overhead number for the deploy doc — NOT a synthetic
# best case. All knobs are fixed below and echoed into the output header so a
# result is always reproducible.
#
# Fixed conditions (override via env for experiments, but the committed deploy
# doc numbers use these defaults):
#   - tool:        oha (fixed CLI, JSON output)
#   - concurrency: 50
#   - duration:    20s per leg
#   - payload:     fixed non-streaming chat request (scripts/payload.json)
#   - upstream:    mock-upstream with MOCK_DELAY_MS fixed delay
#   - warmup:      3s discarded before each measured leg
#
# Usage:
#   ./scripts/load-smoke.sh            # build, run both legs, print report
#   TOOL=wrk ./scripts/load-smoke.sh   # (oha is the supported default)
#
# Requires: cargo, and `oha` (https://github.com/hatoo/oha) on PATH.
set -euo pipefail

# --- fixed knobs (reproducibility-critical) ----------------------------------
CONCURRENCY="${CONCURRENCY:-50}"
DURATION="${DURATION:-20s}"
WARMUP="${WARMUP:-3s}"
MOCK_DELAY_MS="${MOCK_DELAY_MS:-20}"
MOARAY_PORT="${MOARAY_PORT:-18080}"
MOCK_PORT="${MOCK_PORT:-19000}"
INBOUND_KEY="${INBOUND_KEY:-sk-smoke}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PAYLOAD="$ROOT/scripts/payload.json"
TOOL="${TOOL:-oha}"

if ! command -v "$TOOL" >/dev/null 2>&1; then
  echo "ERROR: load tool '$TOOL' not found on PATH." >&2
  echo "Install oha:  cargo install oha    (or set TOOL=wrk)" >&2
  exit 127
fi

echo "==> building release binaries"
cargo build --release -p moaray -p mock-upstream >/dev/null

cleanup() {
  [[ -n "${MOARAY_PID:-}" ]] && kill "$MOARAY_PID" 2>/dev/null || true
  [[ -n "${MOARAY_STORE_PID:-}" ]] && kill "$MOARAY_STORE_PID" 2>/dev/null || true
  [[ -n "${MOCK_PID:-}" ]] && kill "$MOCK_PID" 2>/dev/null || true
}
trap cleanup EXIT

# --- start mock upstream with a fixed injected delay -------------------------
echo "==> starting mock-upstream (delay=${MOCK_DELAY_MS}ms) on :$MOCK_PORT"
PORT="$MOCK_PORT" MOCK_DELAY_MS="$MOCK_DELAY_MS" "$ROOT/target/release/mock-upstream" &
MOCK_PID=$!

# --- write a temp config pointing moaray at the mock -------------------------
CFG="$(mktemp)"
cat > "$CFG" <<YAML
server:
  bind: "127.0.0.1"
  port: $MOARAY_PORT
auth:
  keys:
    - id: smoke
      key_env: MOARAY_SMOKE_INBOUND
      allow_models: [bench]
models:
  - name: bench
    provider_type: openai-compat
    base_url: http://127.0.0.1:$MOCK_PORT
    api_key_env: MOARAY_SMOKE_UPSTREAM
    upstream_id: bench
YAML

echo "==> starting moaray on :$MOARAY_PORT"
MOARAY_CONFIG="$CFG" \
  MOARAY_SMOKE_INBOUND="$INBOUND_KEY" \
  MOARAY_SMOKE_UPSTREAM="sk-upstream" \
  "$ROOT/target/release/moaray" &
MOARAY_PID=$!

# --- second moaray instance WITH usage_store enabled (G8 configured variant) --
# Same config + a usage_store block, on a separate port, so we can measure the
# added p95 of accounting (Arc-clone + try_send) vs the store-off leg above.
STORE_PORT="${STORE_PORT:-$((MOARAY_PORT + 1))}"
STORE_DB="$(mktemp -u).db"
CFG_STORE="$(mktemp)"
# Rewrite the port AND inject a usage_store block under `server:` (right after the
# port line, so it lands at the correct 2-space indent inside server).
sed -e "s|  port: $MOARAY_PORT|  port: $STORE_PORT\n  usage_store:\n    path: $STORE_DB\n    channel_capacity: 8192\n    batch_size: 256|" \
  "$CFG" > "$CFG_STORE"
echo "==> starting moaray+store on :$STORE_PORT (db=$STORE_DB)"
MOARAY_CONFIG="$CFG_STORE" \
  MOARAY_SMOKE_INBOUND="$INBOUND_KEY" \
  MOARAY_SMOKE_UPSTREAM="sk-upstream" \
  "$ROOT/target/release/moaray" &
MOARAY_STORE_PID=$!

# wait for both to listen
for _ in $(seq 1 50); do
  if curl -fsS "http://127.0.0.1:$MOCK_PORT/healthz" >/dev/null 2>&1 \
     && curl -fsS "http://127.0.0.1:$MOARAY_PORT/healthz" >/dev/null 2>&1 \
     && curl -fsS "http://127.0.0.1:$STORE_PORT/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done

run_leg() {
  local name="$1" url="$2"; shift 2
  echo "==> warmup $name ($WARMUP)" >&2
  "$TOOL" -z "$WARMUP" -c "$CONCURRENCY" --no-tui "$@" "$url" >/dev/null 2>&1 || true
  echo "==> measuring $name ($DURATION, c=$CONCURRENCY)" >&2
  "$TOOL" -z "$DURATION" -c "$CONCURRENCY" --no-tui --output-format json "$@" "$url"
}

# Direct leg: straight to the mock upstream.
DIRECT_JSON="$(run_leg direct \
  "http://127.0.0.1:$MOCK_PORT/v1/chat/completions" \
  -m POST -H 'content-type: application/json' -D "$PAYLOAD")"

# Gateway leg: through moaray (adds auth + route + governance).
MOARAY_JSON="$(run_leg moaray \
  "http://127.0.0.1:$MOARAY_PORT/v1/chat/completions" \
  -m POST -H 'content-type: application/json' \
  -H "authorization: Bearer $INBOUND_KEY" -D "$PAYLOAD")"

# Gateway+store leg: through moaray with usage_store enabled (G8 variant).
STORE_JSON="$(run_leg moaray+store \
  "http://127.0.0.1:$STORE_PORT/v1/chat/completions" \
  -m POST -H 'content-type: application/json' \
  -H "authorization: Bearer $INBOUND_KEY" -D "$PAYLOAD")"

# --- extract p50/p95 (oha JSON: .latencyPercentiles.p50 in seconds) ----------
pctl() { python3 -c "import json,sys;d=json.load(sys.stdin);print(round(d['latencyPercentiles']['$1']*1000,3))"; }
D_P50="$(printf '%s' "$DIRECT_JSON" | pctl p50)"
D_P95="$(printf '%s' "$DIRECT_JSON" | pctl p95)"
M_P50="$(printf '%s' "$MOARAY_JSON" | pctl p50)"
M_P95="$(printf '%s' "$MOARAY_JSON" | pctl p95)"
S_P50="$(printf '%s' "$STORE_JSON" | pctl p50)"
S_P95="$(printf '%s' "$STORE_JSON" | pctl p95)"
ADD_P50="$(python3 -c "print(round($M_P50-$D_P50,3))")"
ADD_P95="$(python3 -c "print(round($M_P95-$D_P95,3))")"
# Added cost of accounting itself: store leg vs store-off gateway leg.
ACC_P50="$(python3 -c "print(round($S_P50-$M_P50,3))")"
ACC_P95="$(python3 -c "print(round($S_P95-$M_P95,3))")"

cat <<REPORT

================ moaray load-smoke ================
tool=$TOOL concurrency=$CONCURRENCY duration=$DURATION warmup=$WARMUP
mock_delay_ms=$MOCK_DELAY_MS payload=$(basename "$PAYLOAD")
---------------------------------------------------
leg           p50(ms)   p95(ms)
direct        $D_P50    $D_P95
moaray        $M_P50    $M_P95
moaray+store  $S_P50    $S_P95
---------------------------------------------------
ADDED OVERHEAD (gateway vs direct)  p50=${ADD_P50}ms  p95=${ADD_P95}ms
ACCOUNTING COST (store vs gateway)  p50=${ACC_P50}ms  p95=${ACC_P95}ms
===================================================
REPORT
