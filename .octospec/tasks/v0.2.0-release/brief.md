---
type: Task
title: "Task: v0.2.0-release"
description: Release wrap-up for moaray v0.2.0 — version bump + RELEASE notes + Finish. No functional code change.
tags: ["release", "versioning", "usage-accounting", "cost", "persistence"]
timestamp: 2026-06-26T00:00:00Z
slug: v0.2.0-release
upstream: moaray/moaray
source: self
---

# Task: v0.2.0-release

> Release收口 for moaray **v0.2.0**. The v0.2-P1 persistent usage/cost accounting
> already landed on `main` (13ab87b, PR #13) and passed E2E regression (6
> checkpoints, YUJ-6037). This task is **pure release wrap-up** — it does NOT
> change any functional code.

## Goal
Cut moaray v0.2.0: bump the workspace version `0.1.0 → 0.2.0`, add a v0.2.0
section to `RELEASE.md` (kept on top of the v0.1.0 history), and complete the
octospec Finish gate. No behavior changes.

## Background
v0.2-P1 added a persistent per-arm request store + cost accounting (`UsageSink`
trait, new `moaray-store` crate, `SqliteSink` on WAL behind a dedicated OS-thread
writer, nano-USD cost). That work is on `main`; see task `v0.2-usage-accounting`.
This release票 only stamps the version and documents the release.

## Load-bearing list
- `release-versioning`: workspace `Cargo.toml` `[workspace.package] version` is the
  single source of truth; all crates use `version.workspace = true`, so the bump +
  `cargo build` propagates to `Cargo.lock`.
- `release-notes-accuracy`: RELEASE.md must state the real posture (best-effort
  telemetry-grade, NOT invoice-grade), acceptance gate numbers, default-off config,
  and the known limitations — no overclaiming.

## Out of scope
- Any functional code change (no `.rs` edits, no behavior changes).
- DESIGN.md §5 (already updated to "landed in v0.2.0" by PR #13 — no-op here).
- Tagging / GitHub release (done by human after merge).
- Invoice-grade durability, `/v1/usage`, dashboards, load-smoke baseline fix (#14)
  — all deferred to v0.2-P2.

## Acceptance
- Workspace version is `0.2.0`; `Cargo.lock` shows all moaray crates at `0.2.0`.
- `cargo test --workspace` green (169 passed).
- `cargo clippy --workspace --all-targets -- -D warnings` clean.
- `docker build .` succeeds (CI docker gate).
- RELEASE.md has a v0.2.0 section on top; v0.1.0 history preserved.
- octospec Finish artifacts present (this brief + journal + log entry);
  `octospec-lint` green.
