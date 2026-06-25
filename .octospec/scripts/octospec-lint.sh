#!/usr/bin/env bash
# octospec-lint — OKF conformance check (thin wrapper around octospec-lint.py).
#
# The real linter is YAML-aware (see octospec-lint.py): it parses each knowledge
# file's frontmatter as YAML and requires a non-empty scalar `type`. This wrapper
# exists so CI, docs, and pre-commit hooks can keep calling a stable .sh entry.
#
# Usage:   scripts/octospec-lint.sh [root]   (default root = ".")
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec python3 "$here/octospec-lint.py" "$@"
