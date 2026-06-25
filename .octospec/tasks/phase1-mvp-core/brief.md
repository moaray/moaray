---
type: Task
title: "Task: phase1-mvp-core"
description: MVP passthrough OpenAI-compatible gateway (axum + provider trait + openai/anthropic adapters)
tags: ["passthrough", "provider", "streaming", "auth", "config"]
timestamp: 2026-06-25T13:00:00Z
slug: phase1-mvp-core
upstream: moaray/moaray#2
source: self
---

# Task: phase1-mvp-core

> Backfilled from dispatch chain (multica YUJ-5914 + plan YUJ-5910). Delivered in PR #3.

## Goal
可单独上线的最小可用 OpenAI 兼容网关(passthrough):axum server + Provider trait(双路径)
+ openai-compat & anthropic 适配器 + 流式/非流式 + 鉴权 + foundational defaults。

## Background
moaray v1 第一阶段。Load-bearing spec: `DESIGN.md` §1/§5。完整施工图: multica YUJ-5910
(PLAN-FULL,过 codex+gemini 双顾问评审)。8 个工作单元 P1-1..P1-8。

## Load-bearing list
- Provider trait 双路径:passthrough 走原始字节透传(不解析业务字段,保未知字段/usage/vendor),MoA 走结构化 chat()
- 依赖方向:moaray-moa 只依赖 moaray-core,bin 组装 Registry
- ChatRequest/Response/Chunk 用 `#[serde(flatten)] extra` 保未知字段
- SSE 端到端不缓冲(streaming-passthrough rule)
- secret 不进日志/响应/metrics label(no-secret-logging rule)
- 错误统一 OpenAI 信封(error-envelope rule)
- base_url scheme 白名单 / request-id / per-request timeout / body-limit

## Out of scope
- MoA 编排(Phase 2)、限流/熔断/热重载(Phase 3)、embeddings/image/audio、Web UI
- tool-calling/response_format 的解析(passthrough 透传字段但不解析)

## Acceptance
- cargo build/clippy 零警告;cargo tree 无 litellm
- cargo test --workspace 全绿(openai+anthropic wiremock,流式+非流式)
- SSE 端到端逐帧不缓冲(≥2 delta + 终止帧 + text/event-stream)
- 鉴权/错误码矩阵 401/403/404/413;/v1/models allowlist 过滤
- secret redaction 机检:tracing buffer 不含 token/api-key
- docker build + compose up healthz 200 + passthrough curl 经自带 mock-upstream 200
