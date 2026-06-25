#!/usr/bin/env python3
"""octospec-lint — OKF conformance check for octo-spec knowledge files.

Verifies that every knowledge .md file is a valid OKF unit: it must start with a
properly terminated YAML frontmatter block that parses as valid YAML and declares
a non-empty scalar `type` (OKF's only required field). This keeps the repository
a valid OKF bundle so any OKF-aware tool or agent can consume it.

Scope is opt-in: only directories that hold knowledge units are linted
(global rules, any */rules/ tree, and per-task briefs / journals). Prose, docs,
fill-in templates, and OKF index/log structural files are not knowledge units and
are never linted.

Usage:  scripts/octospec-lint.sh [root]   (default root = ".")
Exit:   0 = all conformant, 1 = one or more violations, 2 = usage/setup error.
"""
import os
import sys
import glob

try:
    import yaml
except ImportError:
    sys.stderr.write("octospec-lint: PyYAML is required (pip install pyyaml)\n")
    sys.exit(2)


def is_knowledge_file(rel: str) -> bool:
    # rel uses forward slashes, relative to ROOT
    parts = rel.split("/")
    name = parts[-1]
    if not name.endswith(".md"):
        return False
    # Structural / non-knowledge files that may live inside knowledge dirs.
    if name in ("index.md", "log.md"):
        return False
    # Fill-in templates are scaffolds, not knowledge units.
    if name.endswith(".template.md"):
        return False
    # Knowledge-unit locations (opt-in).
    if "global" in parts[:-1] and name not in ("README.md",):
        return True
    if "rules" in parts[:-1]:
        return True
    if "tasks" in parts[:-1] or "journal" in parts[:-1]:
        return True
    return False


def strip_bom(s: str) -> str:
    return s[1:] if s and s[0] == "\ufeff" else s


def check_file(path: str):
    """Return a list of violation strings (empty == conformant)."""
    try:
        with open(path, "r", encoding="utf-8") as fh:
            raw = fh.read()
    except (OSError, UnicodeDecodeError) as e:
        return [f"{path}: cannot read as UTF-8 ({e})"]

    # Normalize BOM + CRLF so well-formed files are not misjudged.
    text = strip_bom(raw).replace("\r\n", "\n").replace("\r", "\n")
    lines = text.split("\n")

    if not lines or lines[0].strip() != "---":
        return [f"{path}: missing YAML frontmatter (file must start with '---')"]

    # Find the closing fence.
    close_idx = None
    for i in range(1, len(lines)):
        if lines[i].strip() == "---":
            close_idx = i
            break
    if close_idx is None:
        return [f"{path}: unterminated YAML frontmatter (missing closing '---')"]

    block = "\n".join(lines[1:close_idx])
    if not block.strip():
        return [f"{path}: empty frontmatter block"]

    try:
        data = yaml.safe_load(block)
    except yaml.YAMLError as e:
        msg = str(e).splitlines()[0] if str(e) else "invalid YAML"
        return [f"{path}: malformed YAML frontmatter ({msg})"]

    if not isinstance(data, dict):
        return [f"{path}: frontmatter is not a YAML mapping"]

    t = data.get("type", None)
    if t is None:
        return [f"{path}: missing required OKF field 'type'"]
    if not isinstance(t, str) or not t.strip():
        return [f"{path}: OKF field 'type' must be a non-empty string (got {t!r})"]

    return []


def main(argv):
    root = argv[1] if len(argv) > 1 else "."
    if not os.path.isdir(root):
        sys.stderr.write(f"octospec-lint: root '{root}' is not a directory\n")
        return 2

    md_files = []
    for dirpath, dirnames, filenames in os.walk(root):
        if ".git" in dirpath.split(os.sep):
            continue
        for fn in filenames:
            if fn.endswith(".md"):
                full = os.path.join(dirpath, fn)
                rel = os.path.relpath(full, root).replace(os.sep, "/")
                if is_knowledge_file(rel):
                    md_files.append(full)
    md_files.sort()

    violations = []
    for f in md_files:
        violations.extend(check_file(f))

    if violations:
        for v in violations:
            print(f"FAIL {v}")
        print("octospec-lint: FAILED — fix the violations above")
        return 1

    # A repo root that yields zero knowledge files means the scope globs drifted
    # (rename, move) — fail closed rather than give a false green.
    if not md_files and os.path.abspath(root) == os.path.abspath("."):
        sys.stderr.write(
            "octospec-lint: no knowledge files found under repo root — "
            "scope globs may have drifted\n"
        )
        return 1

    print(f"octospec-lint: OK ({len(md_files)} knowledge file(s) conform to OKF)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
