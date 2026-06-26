# P3-3 config 热重载 — 设计补遗 (resolved, AuditBot 审后定稿)

> 状态: DESIGN ✅ APPROVED FOR IMPL。AuditBot plan-eng-review (YUJ-5948) 给出
> NEEDS-REVISION + 2 P0 + 3 P1;本补遗把每条决策拍死,作为 P3-3 施工的 load-bearing 约束。
> 决策人: Coda(工程决策),Yu 有否决权。基线 moaray@30fcecb。

## P0-1 (F1) — 稳定 state key 定义(state_key)与可观测 upstream_id 拆分

> **修订 (2026-06-26, 经 Coda 工程决策授权, Yu 有否决权):** 原文把
> `upstream_id` 既当限流/熔断的内部 key,又复用为 Prometheus label + 客户端 `moaray`
> 字段。但在已合并 main (PR#7) 中 `upstream_id` 是**双职责**的可观测值
> (`observe.rs::record_moa_arm` 的 label + `app.rs::moa_metadata` 回客户端)。若按原文把
> 三元组塞进 `upstream_id`,`base_url` 会进入 metric label / 客户端响应,**打挂已合并
> `hardening.rs::metrics_have_buckets_and_no_high_cardinality_labels`(断言
> `!text.contains(&server.uri())`)并违反 `.octospec/rules/no-secret-logging`(load_bearing
> pri 95,"Never write secrets into metrics labels" + 不泄露上游拓扑)**。AuditBot
> plan-eng-review (YUJ-5948) 设计审时这两个出口代码尚未落地,无从对照,故漏。
> 裁决:**拆分两个职责**(下文)。

**决策:新增内部 `state_key`(限流/熔断身份),保留低基数 `upstream_id`(可观测维度)。**

- **`state_key = provider_type + "|" + base_url + "|" + api_key_env`** —
  **派生、废手填**,**仅**用于 `StatefulState` 的 per-upstream 桶/熔断 keying 与
  reconcile 的保留判定。它含 `base_url`,**内部专用,绝不进 metric label / 客户端响应**。
- **`upstream_id`** — 保持低基数、面向人类(`name` 派生或手填),**继续**作为
  Prometheus `upstream_id` label + 客户端 `moaray` 字段。它**不再** key 任何 state。

理由(state_key 三元组采纳 AuditBot 推荐的最稳身份形态):
- base_url 稳定 → 改 model 名/改 upstream_id label 不重置限流/熔断状态(堵住 P3-3 验收②③盲区)
- 含 api_key_env → 同 host 多账户/多配额各自独立桶,不互相挤占
- 不用 model name(代码现状)→ 避免"改名=新桶=状态重置"和"多别名绕过 per-upstream 上限"
- 二者解耦:`upstream_id` 改名 → `state_key` 不变(URL/key_env 没变)→ state 保留;
  base_url/账户变 → `state_key` 变 → state 正确重建。

落地要求:
- `runtime.rs::ModelConfig` 增 `state_key` 字段;`validate.rs` 由三元组**派生**它;
  `upstream_id` 仍 `unwrap_or(name)`(低基数 label)。`schema.rs` 的手填 `upstream_id`
  **降级为可观测 label**(文档标注废弃为 state key,仅作 label 覆盖)。
- per-upstream 一致性校验(`ConflictingUpstreamGovernance`)改按 **`state_key`** 分组
  (同三元组的多 model 共享一桶/熔断,governance 必须一致;同步改 `lib.rs` 相关测试)。
- `registry.rs` 用 `state_key` 查 `StatefulState` 桶;provider 仍以 `upstream_id` 作可观测 id。
- **label/client 出口约束(load-bearing,后审对照):** `state_key` 永不出现在
  `observe.rs` 的任何 label 或 `app.rs::moa_metadata` 的任何字段;
  `hardening.rs::metrics_have_buckets_and_no_high_cardinality_labels` 须继续绿。

## P0-2 (F3) — StatefulState 骨架可变性 + 发布序

**决策:per_upstream 改用 `DashMap`(并发原地插删),并钉死发布序。**

发布序(reload 时严格按此,写进实现 + 测试):
```
1. validate 新 config(total,全或无;不过则保留旧 Runtime,服务存活)
2. diff 出 added / removed / unchanged **state_key**(上游身份三元组)
3. 为 added 上游建 limiter+semaphore+breaker,写入 DashMap (state 先就位)
   —— 实现为 `StatefulState::ensure_for_config`(原地插入,保留已存在 Arc)
4. unchanged 上游的 state entry 原地保留,绝不 touch(`Arc` 复用)
5. ArcSwap.store(new Runtime)  ← 此刻新 model/recipe 才可路由,桶已在位
6. removed 上游的 state 延迟 GC(见 P1-F4)—— `StatefulState::retain_for_config`,
   不在此处同步删
```
关键不变式:**任何时刻"可路由的 upstream 一定有对应 state"**,杜绝"有 provider 无桶"窗口。

> 触发方式(plan 未限定,二选一):落地选 **SIGHUP**(`main.rs` 装 Unix signal handler →
> `ConfigReloader::reload()` 重读 `MOARAY_CONFIG`)。比 fs-watch 稳妥、无新增跨平台依赖面;
> 整个 ensure→build→swap→GC 序由 `ConfigReloader`(`reload.rs`)钉死并串行化。

## P1-F4 — 删除与 in-flight 竞态

**决策:热路径只增不删,孤儿 state 延迟/惰性 GC。**
- 被删 upstream 的 state 必须活过任何解析了旧 Runtime 的 in-flight 请求
- 实现:GC 前等一个 drain 窗口(如 ≥ request_timeout 上界),或引用计数归零再回收
- 绝不同步删(否则删掉正被 in-flight 请求要用的 semaphore/breaker)

## P1-F2 — 服务级字段热更语义

**决策:request_timeout_ms / max_body_bytes / moa_expose_metadata 改为从 live snapshot 读 → 真正可热重载。**
- 请求路径不再读启动期烤进 ServerCtx 的拷贝,改读 ArcSwap 当前 Runtime.config
- bind / port / shutdown_grace_ms 仍为"需重启,reload 时 warn 并忽略"(不可热)
- default_max_tokens:随 provider 重建生效(F5 diff-reuse 后,仅变更项重建),文档说明

## P1-F5 — 连接池保留(diff-and-reuse)

**决策:reload 做 diff-and-reuse,不整体重建。**
- 未变更的 model 复用旧 `Arc<dyn Provider>`(连带其 warm 连接池),仅重建变更项
- 或:reqwest `Client`(连接池载体)跨 reload 持久共享,provider 外壳只换 base_url/key
- 目标:改一行配置不引发全上游重连风暴(护 sub-ms 定位)

## P2/P3 — F7 / F6

- F7:验收③扩成多场景测试矩阵(见 AuditBot 报告"建议的测试骨架"7 条,全部纳入 P3-3 验收)
- F6:reload 时对"config 引用但 env 未设"的 api_key_env 打 warn(不 reject,保持宽松启动),仅补观测

## P3-3 验收(machine-checkable,采纳 AuditBot 测试骨架)
1. [P0盲区] 打到限流(429)→reload 不动该 upstream→断言仍 429;熔断 OPEN→reload→仍 circuit_open(503)
2. [F1] 改 model 名(base_url 不变)→断言限流/熔断状态按 upstream_id 语义保留
3. [F1] 两 model 别名指向同一 (type,base_url,key_env)→断言共享同一桶(防绕过)
4. [F3 发布序] reload 新增引用新 upstream 的 model→swap 瞬间并发打→永不"有 provider 无 limiter"(无 panic)
5. [F4] reload 删 upstream + 旧 Runtime 的 in-flight 请求打向它→不 panic,state 存活到完成
6. [F2] reload 改 request_timeout/max_body→live 生效(或 warn+忽略二选一,不静默无效)
7. [全或无] 推部分 invalid config→保留旧 Runtime 不切换 + 报错 + 服务存活
