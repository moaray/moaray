---
type: Task
title: "Task: phase3-hot-reload"
description: Config hot reload (SIGHUP), state-preserving + all-or-nothing, with the upstream_id state_key/observability split (P0)
tags: ["hot-reload", "config", "rate-limiting", "circuit-breaker", "resilience", "observability"]
timestamp: 2026-06-26T00:00:00Z
slug: phase3-hot-reload
upstream: moaray/moaray#6
source: self
---

# Task: phase3-hot-reload

> moaray v1 P3-3 — plan 唯一 P0 高风险工作单元。Load-bearing spec:
> `DESIGN-P3-3.md`(AuditBot plan-eng-review YUJ-5948 审后定稿 + 本 PR 内 P0-1 修订)。
> 依赖 P3-1/P3-2(限流/熔断,PR#7 `f3aadbb`)已落地的 limiter/breaker/GovernedProvider。

## Goal
不重启即可热重载 `MOARAY_CONFIG`(models/recipes/keys + 热字段),**state-preserving**
(未变更上游的限流桶/熔断状态跨 reload 保留)且 **all-or-nothing**(invalid config 保留旧
Runtime,服务存活)。触发用 **SIGHUP**。

## Background
P3-1/P3-2 已把 limiter/breaker 状态放进与可热替换 `Runtime` 解耦的 `StatefulState`,
并由 `GovernedProvider` 包裹同一 `Arc<dyn Provider>`(MoA 与 passthrough 共桶)。本票
在该地基上实现真正的文件→swap 热重载。

**P0-1 修订(经 Coda 工程决策授权,Yu 有否决权):** 原设计把 `upstream_id` 同时当限流
key 和可观测 label。但 main 中 `upstream_id` 经 `observe.rs`(Prometheus label)+
`app.rs::moa_metadata`(客户端字段)流出;把三元组塞进它会让 `base_url` 进 metric
label/客户端响应,打挂已合并 `hardening.rs::metrics_have_buckets_and_no_high_cardinality_labels`
并违反 `no-secret-logging`(load_bearing pri 95)。**裁决:拆分** —— 内部
`state_key = provider_type|base_url|api_key_env` 仅做 state keying;`upstream_id` 保持
低基数继续做 label/客户端字段。

## Load-bearing list
- **P0-1 拆分**:`ModelConfig.state_key`(三元组派生,内部专用,绝不进 label/响应)+
  `upstream_id`(低基数,name 派生,metrics label + `moaray` 字段)。`validate.rs` 的
  per-upstream governance 一致性校验改按 `state_key` 分组。
- **P0-2 发布序**:`StatefulState.per_upstream` 改 `DashMap`;reload 钉死
  validate→`ensure_for_config`(为新身份建桶,保留旧 Arc)→build providers(查 state_key 桶,
  fail-closed)→`ArcSwap.store`→延迟 `retain_for_config`(GC 孤儿)。不变式:可路由上游必有桶。
- **F4**:被删上游 state 延迟 GC;in-flight 请求经其持有的 `Arc<UpstreamState>` 活到完成。
- **F2**:`request_timeout_ms`/`max_body_bytes`/`moa_expose_metadata` 改从 live snapshot
  (ArcSwap Runtime.config)读;`bind`/`port`/`shutdown_grace_ms` reload 时 warn+忽略。
- **F5**:reqwest `Client` 跨 reload 持久共享;未变更 model 复用旧 `Arc<dyn Provider>`(签名
  diff),仅重建变更项(防重连风暴)。
- **F6**:reload 引用但未设的 api_key_env 打 warn(不 reject)。
- **不回归**:`hardening.rs::metrics_have_buckets_and_no_high_cardinality_labels`
  (`!text.contains(&server.uri())`)+ `no-secret-logging` rule 须继续绿。
- secret 不进日志/metrics label;错误走 OpenAI 信封(error-envelope rule)。

## Out of scope
- listen socket / bind / port 热重建(需重启,reload 仅 warn)。
- 连接池重建超出 diff-and-reuse(F5)的范围。
- MoA 流式聚合(v1 仅非流式 MoA)。
- fs-watch / inotify(选 SIGHUP;plan 二选一)。

## Acceptance
machine-checkable,7 条全部来自 DESIGN-P3-3.md,逐条测试(`tests/hot_reload.rs`):
1. 打到 429→reload 不动该上游→仍 429;熔断 OPEN→reload→仍 circuit_open(503)。
2. 改 model 名(base_url 不变)→限流/熔断状态按 state_key 保留。
3. 两别名指向同一三元组→共享同一桶(防绕过)。
4. reload 新增上游→swap 瞬间并发打→永不"有 provider 无 limiter"(无 panic)。
5. reload 删上游 + 旧 Runtime in-flight 打向它→不 panic,state 存活到完成。
6. reload 改 request_timeout/max_body→live 生效(或 warn+忽略,不静默无效)。
7. 部分 invalid config→保留旧 Runtime + 报错 + 服务存活。
- 另:`fmt` clean、`clippy -D warnings` clean、`cargo test --workspace` 全绿。
