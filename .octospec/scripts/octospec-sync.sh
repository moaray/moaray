#!/usr/bin/env bash
# octospec-sync — vendor the pinned global ("constitution") rules into a
# git-ignored local cache, then sync the shared agent-instruction block into
# the agent-instruction files present in the repo.
#
# Inheritance model: vendor snapshot + version pin (NOT git submodule).
#   - manifest.yaml declares `inherits: octo-spec@<semver>`
#   - this script fetches that version's global/ into .octospec/_global/
#   - _global/ is git-ignored; upgrading = bump the pin + re-run this script.
#
# Agent-instruction sync: one source of truth (the octo-spec checkout's
# templates/octospec-init/AGENT-BLOCK.md) is written, idempotently and
# atomically, between `<!-- octospec:begin -->` / `<!-- octospec:end -->`
# markers into each agent-instruction file that exists (CLAUDE.md, AGENTS.md,
# GEMINI.md, QWEN.md). Marker detection is whole-line and fence-aware, and a
# malformed marker state makes the sync REFUSE that file rather than risk
# clobbering hand-written content (see scripts/octospec_sync_block.py).
#
# Bootstrap: CLAUDE.md and AGENTS.md are the two default entry points — whichever
# is missing is created so BOTH Claude Code (CLAUDE.md) and Codex (AGENTS.md) get
# the block, even when the repo started with only one of them (the common case for
# an existing Claude Code repo that has only CLAUDE.md). GEMINI.md / QWEN.md are
# only updated when they already exist; we never force-create those.
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
OCTOSPEC_DIR="$REPO_ROOT/.octospec"
MANIFEST="$OCTOSPEC_DIR/manifest.yaml"
GLOBAL_CACHE="$OCTOSPEC_DIR/_global"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

[ -f "$MANIFEST" ] || { echo "no $MANIFEST"; exit 1; }

PIN="$(grep -E '^inherits:' "$MANIFEST" | sed -E 's/^inherits:[[:space:]]*//')"
echo "octospec: inherits = $PIN"

# GLOBAL_SRC: path to a checkout of octo-spec at the pinned version.
# Override via env: GLOBAL_SRC=/path/to/octo-spec ./octospec-sync.sh
GLOBAL_SRC="${GLOBAL_SRC:-}"
if [ -z "$GLOBAL_SRC" ]; then
  echo "set GLOBAL_SRC to a checkout of octo-spec (at version: $PIN)" >&2
  echo "  e.g. GLOBAL_SRC=/path/to/octo-spec ./.octospec/scripts/octospec-sync.sh" >&2
  exit 1
fi

# 1) Vendor the global rules.
rm -rf "$GLOBAL_CACHE"
mkdir -p "$GLOBAL_CACHE"
cp -r "$GLOBAL_SRC/global/." "$GLOBAL_CACHE/"
echo "octospec: synced global rules -> $GLOBAL_CACHE"

# Ensure _global/ is git-ignored (with a trailing-newline guard so we never glue
# onto a previous line that lacks a newline).
GITIGNORE="$OCTOSPEC_DIR/.gitignore"
if ! grep -qxF "_global/" "$GITIGNORE" 2>/dev/null; then
  if [ -s "$GITIGNORE" ] && [ -n "$(tail -c1 "$GITIGNORE")" ]; then
    printf '\n' >> "$GITIGNORE"
  fi
  printf '_global/\n' >> "$GITIGNORE"
fi

# 2) Sync the shared agent-instruction block into the instruction files present.
BLOCK_SRC="$GLOBAL_SRC/templates/octospec-init/AGENT-BLOCK.md"
SYNC_PY="$HERE/octospec_sync_block.py"
if [ ! -f "$BLOCK_SRC" ]; then
  echo "octospec: WARNING no AGENT-BLOCK.md at $BLOCK_SRC; skipping instruction sync" >&2
elif [ ! -f "$SYNC_PY" ]; then
  echo "octospec: WARNING no octospec_sync_block.py at $SYNC_PY; skipping instruction sync" >&2
else
  # Two default entry points (CLAUDE.md for Claude Code, AGENTS.md for Codex)
  # are created if missing; the rest are only synced when already present.
  DEFAULTS="CLAUDE.md AGENTS.md"
  OPTIONAL="GEMINI.md QWEN.md"
  rc=0
  # Per-file isolation: one refused/failed file must not abort the rest, but it
  # MUST be reflected in the final exit code.
  for t in $DEFAULTS; do
    if [ -f "$REPO_ROOT/$t" ]; then
      if res="$(python3 "$SYNC_PY" "$REPO_ROOT/$t" "$BLOCK_SRC" 2>&1)"; then
        echo "octospec: $t -> $res"
      else
        echo "octospec: $t -> FAILED: $res" >&2
        rc=1
      fi
    else
      echo "octospec: $t missing; bootstrapping"
      if res="$(python3 "$SYNC_PY" "$REPO_ROOT/$t" "$BLOCK_SRC" --create 2>&1)"; then
        echo "octospec: $t -> $res"
      else
        echo "octospec: $t -> FAILED: $res" >&2
        rc=1
      fi
    fi
  done
  for t in $OPTIONAL; do
    [ -f "$REPO_ROOT/$t" ] || continue
    if res="$(python3 "$SYNC_PY" "$REPO_ROOT/$t" "$BLOCK_SRC" 2>&1)"; then
      echo "octospec: $t -> $res"
    else
      echo "octospec: $t -> FAILED: $res" >&2
      rc=1
    fi
  done
  if [ "$rc" -ne 0 ]; then
    echo "octospec: one or more agent files failed to sync" >&2
    exit "$rc"
  fi
fi

echo "octospec: done."
