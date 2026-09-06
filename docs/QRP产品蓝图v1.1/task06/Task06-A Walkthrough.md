# Task06-A Walkthrough｜Asset Relative Ranking 交付说明

> 状态：**IMPLEMENTED / SOURCE REVIEW PASSED**  
> 分支：`feature/v1.1-task06-ranking`  
> 设计基线：`Task06 System B 横截面相对评分与排名设计书 v0.1.md`  
> 实现基线：`8e0b72edf20f3611cf0ece53bc478a67a5c439a1`  
> 本文只覆盖 **Task06-A / Asset Relative Ranking**；Task06-B Theme Rank 不在本次交付范围。

---

## 1. 交付目标

Task06-A 的目标是把 System B 已有股票级事实转换为可解释、可回放、可审计的横截面相对评价结果：

```text
Canonical A-share Universe
        +
System B Episode / Pools
        +
Canonical Market Series
        +
Popularity Availability + Snapshots
        ↓
M1 / M2 / M3 Relative Ranking
        ↓
Asset Rank Snapshot + Component Audit
```

正式结果身份：

```text
trade_date + ticker
```

每个目标日 canonical A-share 都物化一行，即使三维均不可计算，也不通过“缺行”表达状态。

---

## 2. 已实现评分语义

### 2.1 统一横截面排名语言

统一使用：

```text
normalized_rank_score
= 100 * (N - rank) / (N - 1)
```

其中：

- `rank` 使用 average business rank；
- 同 raw value 必须获得相同业务 rank；
- ticker 只允许稳定序列化，不得打破业务 tie；
- `N=1` → `INSUFFICIENT_UNIVERSE`，score 为 `NULL`；
- `N>=2` 且全部 raw value 相同 → `NO_VARIATION`，score 为 `NULL`；
- 缺失 raw input 不会从最终 canonical output universe 中消失。

### 2.2 M1｜Capacity Strength

Eligibility：当前日 `CAPACITY / IN_POOL`。

组件：

```text
episode_return  50%
avg5_amount     50%
```

计算：

```text
P_episode = normalized_rank_score(episode_return)
P_amount  = normalized_rank_score(avg5_amount)

M1_raw = 0.5 * P_episode + 0.5 * P_amount
M1_score = normalized_rank_score(M1_raw)
```

M1 保留“组件先横截面标准化 → 加权 → 最终再横截面排名”的双层结构。

### 2.3 M2｜Height Strength

Eligibility：当前日 `HEIGHT / IN_POOL`。

正式指标：

```text
height_since_start_return
```

定义：

```text
close(D) / previous_actual_close(height_start_date) - 1
```

因此 `height_start_date` 当天涨幅被计入；不是从 `close(height_start_date)` 起算。

Height membership evidence 已补齐：

```text
height_start_base_close
height_since_start_return
```

M2 直接对 `height_since_start_return` 做横截面标准化。

### 2.4 M3｜Recognition Strength

Eligibility：当前日 `RECOGNITION / IN_POOL`。

组件：

```text
episode_return   20%
return5          20%
return10         20%
hot_rank_score   40%
```

其中：

```text
return5  = close / shift(4) - 1
return10 = close / shift(9) - 1
```

`shift(4)` / `shift(9)` 基于 canonical actual-trading 序列，不按自然日偏移。

热榜单 snapshot 内：

```text
snapshot_hot_score = 100 * (N - rank_position) / (N - 1)
```

当前生产 contract 保持完整 Top100；单平台日级 score 为所有有效 snapshot 的平均值；双平台最终：

```text
hot_rank_score = AVG(dc_hot_score, ths_hot_score)
```

两平台均 AVAILABLE 且 ticker 未上榜时，业务 score 为 0，而不是 missing。

---

## 3. Canonical Market Series

Task06-A 没有直接读取 raw OHLC 做跨日比较，而是新增统一 System B market-series loader：

```text
src/qrp_atlas/pipeline/system_b/market_series.py
```

正式语义：

```text
FORWARD_ADJUSTED
+
ACTUAL_TRADING only
+
target-date truncated
```

该实现同时被 System B Pool production 复用，从而减少 State / Pool / Task06 对跨日价格序列的口径漂移。

实现过程中复现并修复了一个真实边界：

> forward-adjustment 的 target-normalizing denominator 必须锚定最后一个 actual market observation 对应的 factor，不能消费最后实际交易日之后、非交易日出现的 adjustment factor 变化。

对应测试已覆盖非交易日 factor change 不得改变前一实际交易日 canonical close 的场景。

---

## 4. Popularity Availability 与降级语义

新增一等事实表：

```text
popularity_source_availability
```

身份：

```text
trade_date + source
```

核心字段：

```text
source_status
valid_snapshot_count
snapshot_seqs
input_version
source_provenance
source_pipeline_run_id
```

正式状态：

```text
AVAILABLE
UNAVAILABLE
```

Popularity producer 与 availability 在同一事务内提交，避免“热榜表已经更新、availability 仍指向另一版本”的错位。

Task06-A 对 expected `UNAVAILABLE` 的行为：

```text
M1 → 正常
M2 → 正常
M3 → NULL + INCOMPLETE_COMPONENTS
Task06-A stage → COMPLETED
```

并写入稳定诊断：

```text
DC_HOT_SOURCE_UNAVAILABLE
THS_HOT_SOURCE_UNAVAILABLE
```

以下问题仍 fail-fast，不做业务降级：

- schema 缺失/漂移；
- identity 错误；
- availability 非法或与 snapshot 不一致；
- 非完整 Top100 被标记为 AVAILABLE；
- database/storage 错误；
- pool completion 缺失；
- impossible state。

---

## 5. Canonical A-share Result Universe

目标日 Asset Rank universe 来自 `stock_info` 的 canonical 股票身份与上市/退市有效区间，而不是：

```text
当天 market row
三池并集
热榜股票
```

因此：

- 停牌股票仍物化一行；
- 当日行情缺行股票仍物化一行；
- 三池外股票仍物化一行；
- 非 A-share 身份不会进入正式结果域。

这保证：

```text
NOT_ELIGIBLE / MISSING_INPUT
≠
NOT_COMPUTED
```

---

## 6. Persistence

新增三张正式表：

### 6.1 `system_b_asset_rank_snapshot`

主键：

```text
trade_date + ticker
```

保存：

- M1/M2/M3 score；
- M1/M2/M3 rank；
- dimension status；
- universe size；
- dimension raw；
- diagnostics；
- evidence；
- input provenance；
- calculation version；
- production run id。

### 6.2 `system_b_asset_rank_component_audit`

主键：

```text
trade_date + ticker + dimension + component
```

每只股票固定可物化 7 个 component：

```text
M1: episode_return, avg5_amount
M2: height_since_start_return
M3: episode_return, return5, return10, popularity
```

审计链保存：

```text
raw_value
→ raw_rank
→ normalized_rank_score
→ dimension_raw
→ final_dimension_rank
→ final_dimension_score
```

并保存 tie count、status、source provenance、metadata 与 input snapshot lineage。

### 6.3 `popularity_source_availability`

作为热榜 source/date 的 replayable availability contract，由 popularity producer 维护，Task06-A 只消费。

---

## 7. 写入与重跑语义

Asset Rank 结果采用目标日原子替换：

```text
BEGIN
DELETE target-date snapshot
DELETE target-date component audit
INSERT new snapshot
INSERT new audit
completion checks
COMMIT
```

snapshot 与 component audit 必须在同一事务内完成；任一失败则回滚。

相同输入显式重跑允许产生新的 `production_run_id / created_at`，但业务结果、evidence 所代表的输入事实和 ranking output 应保持稳定。

Popularity late arrival 不会自动静默改写历史 Asset Rank；历史变更应通过显式 rerun 触发。

---

## 8. 主要代码交付

新增：

```text
src/qrp_atlas/indicators/system_b/asset_ranking.py
src/qrp_atlas/pipeline/system_b/market_series.py
src/qrp_atlas/pipeline/system_b_asset_rank/service.py
src/qrp_atlas/pipeline/system_b_asset_rank/__init__.py
src/qrp_atlas/pipeline/system_b_asset_rank_contracts.py
deploy/duckdb/008_system_b_asset_rank.sql
```

并修改：

```text
System B contracts / schema / exports
popularity support
System B pool production
contract catalog / registry integration
相关测试
```

本任务未修改 `web/`、`data/` 或环境配置。

---

## 9. 验收证据

开发环境交付记录：

```text
Task06-A targeted tests: 8 passed
ruff --select F,E9: passed
full regression: 1383 passed, 3 skipped
```

另外完成了远端源码级专项复核，重点确认：

1. average business ties 与输入顺序无关；
2. forward-adjusted actual-trading market series 及非交易日 factor 边界；
3. expected popularity `UNAVAILABLE` 能穿透为 stage `COMPLETED`；
4. snapshot/audit 同事务 target-date replacement；
5. popularity rows 与 availability 同事务版本对齐；
6. canonical A-share 全域物化；
7. M1/M2/M3 公式与设计书一致；
8. 没有实现 Task06-B / Theme Rank 等越界能力。

GitHub 当前没有 commit status / CI check 记录，因此上述 pytest 结果是开发环境本地回归记录，不表述为“GitHub CI passed”。

---

## 10. 部署注意事项

本次实现提交本身未执行服务器部署。

部署到 Linux runtime 时需要：

1. 同步本次后端代码；
2. 执行正式 migration 流程创建：
   - `system_b_asset_rank_snapshot`
   - `system_b_asset_rank_component_audit`
   - `popularity_source_availability`
3. 重载/重启 API 或后台 worker，使新 contract registry 生效；
4. 验收 `validate-contracts = 34`；
5. 对一个目标交易日核对：
   - snapshot 行数 = canonical A-share asset count；
   - component audit 行数 = asset count × 7；
   - expected `UNAVAILABLE` 时 M3 为 NULL / INCOMPLETE_COMPONENTS，M1/M2 不受影响；
   - source provenance 与 availability snapshot version 可追溯。

本任务不包含历史数据修复或自动全量 backfill。

---

## 11. Task06-A 最终边界

Task06-A 已完成：

```text
Asset Relative Ranking
M1 / M2 / M3 scoring
canonical result universe
component audit
popularity degradation
production persistence
explicit rerun semantics
```

Task06-A 明确不包含：

```text
Theme Rank
M4/M5 composite scoring
M6
market authorization
portfolio construction
position sizing
ENTER / HOLD / EXIT
```

因此 Task06-A 的正式终点是：

> **给定交易日 D，QRP 能稳定、可解释、可审计地回答每只 canonical A-share 在 M1/M2/M3 三个 System B 评价维度上的横截面相对位置。**

后续 Theme 层综合评价进入 Task06-B 独立设计与实现。