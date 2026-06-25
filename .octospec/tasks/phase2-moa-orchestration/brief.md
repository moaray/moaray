---
type: Task
title: "Task: phase2-moa-orchestration"
description: MoA orchestrator — parallel fan-out, quorum tolerance, concat-synthesize + quorum-judge
tags: ["moa", "orchestration", "quorum", "aggregation"]
timestamp: 2026-06-25T15:35:00Z
slug: phase2-moa-orchestration
upstream: moaray/moaray#4
source: self
---

# Task: phase2-moa-orchestration

> Backfilled from dispatch chain (multica YUJ-5942 + plan YUJ-5910). Delivered in PR #5.

## Goal
MoA 编排(差异化护城河):orchestrator + 并行 fan-out + concat-synthesize & quorum-judge
策略 + 部分失败/quorum 容错 + per-arm metrics。

## Background
moaray v1 第二阶段。recipe schema 已在 Phase 1 (P1-3) 定义,本阶段只激活逻辑不改 config 结构。
完整施工图: multica YUJ-5910 Phase2。

## Load-bearing list
- orchestrator 仅依赖 moaray-core(Arc<dyn Provider>),不依赖 moaray-providers
- fan-out 用 FuturesUnordered,每 arm 独立 arm_timeout_ms
- 部分失败容错:成功 arm >= quorum 才聚合,否则 moa_quorum_failed (503)(moa-partial-failure rule)
- 每 arm future .instrument(span) 保 trace-id;客户端断连取消所有 arm
- concat/judge 模板固定 + delimiter + 防 prompt injection
- MoA + stream:true → 400 moa_streaming_unsupported
- per-arm metrics label 无高基数;usage = proposers + aggregator 求和

## Out of scope
- MoA 流式聚合(v1 仅非流式 MoA)
- 限流/熔断/热重载(Phase 3)

## Acceptance
- cargo test --workspace 全绿,含 fan-out 容错单测(quorum 满足/不满足/arm 超时/断连取消)
- arm-e(concat-synthesize)端到端返回单个 OpenAI completion,usage 求和正确
- 1 个 arm 失败但 quorum 仍满足时返回融合答案(DESIGN §6 硬指标)
- quorum-judge 集成测试通过;MoA 扩展字段默认 off
- model: moa/* + stream:true → 400;fan-out arm 日志带主请求 request-id
