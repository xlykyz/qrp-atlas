# System B Pool 旧路径恢复修复方案｜2026-09-11

> 状态：**APPROVED FOR PRE-EXECUTION AUDIT / CODE NOT STARTED**
> 事故等级：**严重开发事故（SEVERE DEVELOPMENT INCIDENT）**
> 修复目标：**恢复 Task06 介入前的 System B Pool 生产执行路径**
> 目标基线：Task06 提交 `8e0b72e` 的父提交 `118acfc35b7ba22d141a85feb4e4d2deb9bd6f4c`
> 当前开发基线：`develop/v1.1`
> 本文是本轮唯一授权范围；**Task06 本轮冻结，不做任何修复、优化、重构或兼容性补偿。**

---

## 1. 事故定性

2026-09-06 Task06-A 提交 `8e0b72e` 在交付新业务 Asset Relative Ranking 时，同时修改了既有 System B Pool production：

1. 将 Pool 的 `_load_market_panel()` 从既有 DuckDB set-based SQL 路径切换为 `load_canonical_market_series()`；
2. 新 loader 对全量历史行情进行 Python/Pandas 层的状态归一化、逐组/逐行处理和复权；
3. Pool 再把生成的 DataFrame 注册回 DuckDB 后继续原有计算；
4. 同一提交还扩展了 Height membership 的 `metrics_json`，写入 Task06 所需的 `height_start_base_close` / `height_since_start_return`。

事故首次真实执行后，`system-b-pool-height-daily` 从历史稳定约 71–72 秒退化为 >27 分钟未完成，单核持续满载、内存升至约 9.4 GB，并拖慢 job heartbeat；最终只能 SIGKILL。

本事故不是单纯“Python 写慢了”。

正式定性为：

> **新业务 Task06 未经 Old Contract Change 标记、独立回归与性能验收，修改了既有 Pool 的生产执行路径、价格输入语义、实际交易观察序列判定及持久化证据。性能灾难是该越界变更首先暴露出的生产后果。**

---

## 2. 本轮修复目标

本轮只做一件事：

> **把 System B Pool 恢复到 Task06 介入前已经长期运行验证的生产路径。**

恢复目标不是“让新的 canonical loader 更快”，也不是“让 Task06 与 Pool 继续共享一套实现”。

目标架构：

```text
OLD BUSINESS

System B Pool
  -> 原有 direct DuckDB SQL market loader
  -> 原有 HEIGHT / CAPACITY / RECOGNITION evaluator
  -> 原有 membership / run persistence


NEW BUSINESS

Task06
  -> 保持现状，冻结
  -> 本轮不修改
  -> 后续另案事故处理
```

**Pool 恢复不得以 Task06 是否继续正常工作作为本轮验收条件。**

如果恢复旧 Pool 后 Task06 出现兼容性问题，该问题属于后续 Task06 事故处理范围；不得为了兼容 Task06 再次修改旧 Pool。

---

## 3. 允许修改范围

原则上只允许修改：

```text
src/qrp_atlas/pipeline/system_b_pools/service.py
tests/pipeline/system_b_pools/*
必要的事故/验收文档
```

### 3.1 恢复 `_load_market_panel()`

恢复到 `118acfc` 的旧逻辑：

```text
system_b_state_observation
JOIN daily_market_snapshot
LEFT JOIN daily_basic (存在时)
LEFT JOIN zt_pool (存在时)
WHERE trade_date <= end_date
ORDER BY asset_id, trade_date
-> DuckDB fetchdf
```

明确移除 Pool 对以下新路径的依赖：

```text
load_canonical_market_series()
CanonicalMarketSeriesError
_system_b_canonical_market_series
DataFrame -> con.register() -> 再 JOIN
```

### 3.2 恢复 Pool membership 原有持久化行为

恢复 `_normalise_membership(result, run_id)` 的原有边界：

- 不接收 `market_panel`；
- 不为 Task06 计算或回填：
  - `height_start_base_close`
  - `height_since_start_return`
- 不解析/改写 Height `metrics_json` 以服务 Task06；
- `build_stock_pool()` 恢复旧调用方式。

### 3.3 清理仅由上述越界改动引入的依赖

如果以下 import/常量仅服务于 Task06 对 Pool 的扩展，应随恢复删除：

```text
math
HEIGHT_START_BASE_CLOSE
HEIGHT_SINCE_START_RETURN
Task06 canonical market-series imports
```

不得顺手做无关重构。

---

## 4. 明确禁止事项

本轮禁止修改：

```text
src/qrp_atlas/pipeline/system_b/market_series.py
src/qrp_atlas/pipeline/system_b_asset_rank/*
src/qrp_atlas/indicators/system_b/asset_ranking.py
Task06 评分公式
Task06 persistence
Task06 schema / contracts
Task06 popularity 逻辑
```

同时禁止：

- 向量化 `market_series.py`；
- 把 canonical loader 改写为 DuckDB SQL；
- 优化 Task06 性能；
- 为 Task06 增加 fallback；
- 为 Task06 修改 Pool schema / metrics；
- 顺手统一 State / Pool / Task06 口径；
- 顺手清理死代码；
- 修改其他既有生产 job；
- 扩大到 Episode / Segment / Theme Rank 等其他事故或模块。

**Task06 本轮只标记、冻结、待处理。**

---

## 5. Task06 事故状态

自 2026-09-11 起，Task06-A 标记为：

```text
SEVERE DEVELOPMENT INCIDENT
FROZEN
PENDING REMEDIATION
NOT PRODUCTION-READY EVIDENCE
```

原因至少包括：

1. 新业务 PR 修改既有 Pool production path，但没有作为 Old Contract Change 单独标记；
2. 将既有 set-based SQL 路径替换为全历史 Python/Pandas materialization；
3. 未做与真实生产数据规模匹配的性能验收；
4. 全量 pytest 通过未覆盖生产级性能退化；
5. 同时改变 Pool 的输入价格语义/actual-observation 判定，存在业务结果变化风险；
6. “减少口径漂移”的架构统一目标越过了 Task06 的必要业务边界。

Task06 的设计、实现、性能与与旧业务边界将在后续独立事故任务中重新审计。

本轮不得处理。

---

## 6. 执行前审计 Gate

**任何代码修改开始前，本地 Agent 必须先完成审计并明确给出 PASS / BLOCK。**

审计项：

### A. 变更范围

必须确认计划 diff 只涉及：

- `system_b_pools/service.py` 的 Task06 越界改动恢复；
- Pool 专属测试；
- 必要文档。

出现任何 Task06 / Episode / State / scheduler / schema 代码修改，默认 **BLOCK**。

### B. 基线对照

逐项对照：

```text
118acfc -> 8e0b72e
```

确认本次只撤销 `8e0b72e` 对 Pool 的以下影响：

1. canonical market loader 接入；
2. Height Task06 metrics 回填；
3. 由上述两项产生的 import / call-site 变化。

不得机械 revert 整个 Task06 commit。

### C. Old Contract 检查

恢复后的 Pool 必须与 Task06 介入前保持：

- execution path；
- input semantics；
- failure domain；
- performance characteristics；
- persisted Pool result semantics。

如发现无法恢复其中任一项，必须 **BLOCK 并报告**，不得自行设计替代方案。

### D. Task06 隔离检查

必须确认：

> 本轮即使发现 Task06 会因为旧 Pool 恢复而失效，也不得修改 Pool 兼容 Task06。

该发现只能记录为 Task06 后续事故项。

---

## 7. 开发验证

代码修改完成后至少执行：

```bash
python -m tools.verify
```

并执行 Pool 专项测试。

必须新增或保留一个回归断言，证明 Pool production 不再调用：

```text
load_canonical_market_series
```

建议同时对 `118acfc` 与修复分支在代表性输入上比较 Pool 业务结果，忽略：

- `completed_run_id`
- `created_at`
- 仅运行实例相关 metadata

重点比较：

- membership key；
- membership_state；
- entry/exit；
- entry_reason / exit_reason；
- Pool 原有 metrics；
- 每日 membership count。

---

## 8. 性能验收

性能验收属于本次恢复的硬门槛，不允许只以 pytest 通过作为完成条件。

生产历史基线：

```text
Pool daily job: 约 71–72 秒
旧 SQL 核心 market path: 约 1.1 秒（事故调查实测）
```

修复后必须在与生产相近的数据规模上验证：

- 不再出现单核长期 100% 的 Python 逐行热点；
- 内存不再持续增长到事故量级；
- heartbeat 正常推进；
- `height / capacity / recognition` 均回到历史同量级执行时间。

如果不能回到历史同量级，**不得重新启用生产调度**。

---

## 9. 生产恢复顺序

当前三个 Pool job 保持：

```text
enabled: false
```

直到以下顺序全部通过：

1. 合入/部署旧路径恢复代码；
2. 保持 scheduler disabled；
3. 手工运行 `HEIGHT` canary；
4. 核对结果、耗时、内存、heartbeat；
5. 手工运行 `CAPACITY`；
6. 手工运行 `RECOGNITION`；
7. 三者均通过后再恢复 scheduler；
8. 恢复后观察首个正式 daily run。

任一 canary 失败：

> 保持三个 Pool job disabled，回滚本次 runtime 变更，不进入 Task06 修复。

---

## 10. 完成标准

本轮只有同时满足以下条件才可关闭：

- Pool 已恢复 Task06 前执行路径；
- 三池业务回归通过；
- `python -m tools.verify` 通过；
- 生产规模性能回到历史同量级；
- heartbeat/timeout 行为无新增异常；
- 三个 Pool job 已安全恢复调度；
- Task06 仍保持 **FROZEN / PENDING REMEDIATION**；
- 本轮没有修改 Task06 代码。

---

## 11. 后续独立事项（本轮不处理）

Task06 后续至少需要单独处理：

1. canonical market-series 实现的性能与算法边界；
2. Task06 是否需要持久化 Height core facts，应该由谁拥有；
3. `height_since_start_return` 的事实 ownership；
4. Task06 对 legacy Pool contract 的依赖策略；
5. set-based SQL vs DataFrame loader 的正式实现；
6. production-size benchmark；
7. 旧业务隔离与 Old Contract Change 审计机制。

这些事项必须另开任务/PR，不得混入本轮 Pool recovery。
