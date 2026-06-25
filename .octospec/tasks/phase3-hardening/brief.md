---
type: Task
title: "Task: phase3-hardening"
description: Production hardening — rate limiting, circuit breaker, conservative retry, full observability, load-smoke + deploy doc
tags: ["rate-limiting", "circuit-breaker", "retry", "observability", "load-smoke", "resilience"]
timestamp: 2026-06-25T18:00:00Z
slug: phase3-hardening
upstream: moaray/moaray#6
source: self
---

# Task: phase3-hardening

> moaray v1 第三阶段（P3-1/P3-2/P3-4/P3-5 only）。施工图: multica YUJ-5910 Phase3。
> **范围排除 P3-3 config 热重载**（P0 高风险，单独走设计审 YUJ-5948）。本票只为
> P3-3 打 state-preserving 地基（StatefulState reconcile），不实现文件 watcher。

## Goal
为 moaray 补全生产保护能力：per-key + per-upstream 限流（governor token bucket）+
上游并发上限 + 背压、per-upstream 熔断状态机 + 保守重试退避、完整可观测性
（passthrough/MoA 分桶 latency + per-model + MoA arm 指标 + request-id 全链路）、
可复现 load-smoke 基准 + 部署文档。

## Background
Phase 1 (#3) 已落地 Runtime / StatefulState 解耦分层（StatefulState 按 upstream_id
keyed，独立于会被热替换的 Runtime）。Phase 2 (#5) 落地 MoA orchestrator，且 MoA arm
与 passthrough 共享同一 `Arc<dyn Provider>` 实例。本阶段在该地基上填充限流/熔断状态，
并通过 GovernedProvider 装饰器统一施加，使 MoA 与 passthrough 经同一 upstream 桶。

## Load-bearing list
- limiter/breaker 按 upstream_id 组织，存于 reload-surviving StatefulState；
  `StatefulState::reconcile` 保留未变更 upstream/key 的 Arc（P3-3 state-preserving 地基）
- MoA arm 与 passthrough 共用同一 per-upstream 桶/信号量/熔断（GovernedProvider 包裹
  同一 Arc<dyn Provider>，registry 装配）— MoA fan-out 不能绕过上游上限
- 错误码矩阵（moaray-core 单一来源）：gateway 限流→429 `rate_limited`；熔断→503
  `circuit_open`；连接失败→502 `upstream_error`（retry-safe）
- 重试默认关；即便开启也仅重 `UpstreamUnavailable`（请求未发出的连接失败），已发出
  生成请求永不重试，流式永不重试（防重复扣费，codex P1）
- metrics label 无高基数/敏感值：禁 request-id/key/url/error-string；latency 按
  path=passthrough|moa 分桶（no-secret-logging rule）
- secret 不进日志/metrics label；错误走 OpenAI 信封（error-envelope rule）

## Out of scope
- **P3-3 config 热重载文件 watcher / SIGHUP**（单独票；本票仅留 reconcile 地基）
- listen socket / 连接池重建
- MoA 流式聚合（v1 仅非流式 MoA）

## Acceptance
- per-key 超速→429；per-upstream 并发上限生效；MoA 放大流量受同一 upstream 桶约束
  （集成测试断言 MoA 也触发该上游限流，降到 quorum 下 → 503）
- 熔断打开 + 半开恢复（单测）；重试默认不碰已发出生成请求（wiremock expect(1) 断言单次）
- /metrics 含 passthrough/MoA 分桶 latency + per-model + MoA arm 指标；断言无
  request-id/key/url label
- load-smoke 产出 p50/p95 added-overhead 写入 docs/DEPLOY.md；
  `cargo clippy --workspace --all-targets -- -D warnings` 净、CI green
