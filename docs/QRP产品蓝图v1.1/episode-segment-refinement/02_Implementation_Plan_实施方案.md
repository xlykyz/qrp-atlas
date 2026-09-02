# Implementation Plan: System B Episode 统计粒度细化 (Episode Segment)

本方案基于 [`01_System_B_Episode_统计粒度细化设计.md`](01_System_B_Episode_统计粒度细化设计.md) 设计规范，在不改变任何现有状态机、Episode 规则和交易策略的前提下，为 System B 新增 **`system_b_episode_segment`** 纯下游派生层。

## User Review Required

> [!NOTE]
> - **纯下游无损派生（Append-only）**：现有 `system_b_episode` 与 `system_b_episode_observation` 数据、表结构和 API 契约完全保持 100% 向下兼容（Legacy Zero-Change）。
> - **Phase 1 聚焦 Canonical 闭环**：本次工作范围严格限定在 `Contract → Derivation → Persistence → Audit → Tests`。API 端点（`GET /api/v1/system-b/episodes/{episode_id}/segments`）标为 Phase 2，不纳入本次阻塞验收。

---

## Proposed Changes

### 1. 契约与 Schema 层 (Contracts & Schema)

#### [MODIFY] [`src/qrp_atlas/contracts/system_b.py`](../../../src/qrp_atlas/contracts/system_b.py)
- 新增 Segment 标识与字段常量：
  - `SEGMENT_ID`, `SEGMENT_NO`, `SEGMENT_STATE`, `ACTIVE_SPRINT_NO`
  - `ANCHOR_DATE`, `START_DATE`, `END_DATE`, `TRADING_DAYS`
  - `ANCHOR_CLOSE`, `START_CLOSE`, `END_CLOSE`, `SEGMENT_RETURN`
  - `PEAK_CLOSE`, `PEAK_DATE`, `MAX_DRAWDOWN`, `IS_OPEN`
  - `SOURCE_EPISODE_RULE_VERSION`, `SEGMENT_VERSION`
  - `SYSTEM_B_EPISODE_SEGMENT_TABLE = "system_b_episode_segment"`
  - `SYSTEM_B_EPISODE_SEGMENT_VERSION = "system_b_episode_segment@1.0.0"`
- 新增枚举 `SystemBSegmentState`（`ACTIVE`, `NON_ACTIVE`）。

#### [MODIFY] [`src/qrp_atlas/contracts/schema.py`](../../../src/qrp_atlas/contracts/schema.py)
- 新增 `SYSTEM_B_EPISODE_SEGMENT` TableSchema 定义并正式注册进 `ALL_TABLES`，主键为 `(segment_id,)`，完整定义所有 23 个字段类型与非空约束。

#### [MODIFY] [`src/qrp_atlas/contracts/__init__.py`](../../../src/qrp_atlas/contracts/__init__.py)
- 导出上述新增的常量、枚举与 Schema 对象。

#### [MODIFY] [`deploy/duckdb/002_system_b_episode.sql`](../../../deploy/duckdb/002_system_b_episode.sql)
- 追加 `CREATE TABLE IF NOT EXISTS system_b_episode_segment` DDL。

---

### 2. 指标派生层 (Indicators)

#### [NEW] [`src/qrp_atlas/indicators/system_b/segment.py`](../../../src/qrp_atlas/indicators/system_b/segment.py)
- 实现数据结构：`SystemBEpisodeSegmentResult(segments: pd.DataFrame)` 与自定义异常 `SystemBEpisodeSegmentError(ValueError)`。
- 实现核心函数：`calculate_system_b_episode_segments(episodes: pd.DataFrame, observations: pd.DataFrame) -> SystemBEpisodeSegmentResult`：
  1. **输入验证**：严格检查 `episodes` 与 `observations` 所需列，拒绝无效空输入；
  2. **稳定排序与组内 shift 隔离**：
     - `observations.sort_values(["episode_id", "trade_date"], kind="mergesort")`
     - 组内检测 `prev_state = obs.groupby("episode_id", sort=False)["segment_state"].shift()`
     - 组内累加 `segment_no = is_new.groupby(obs["episode_id"], sort=False).cumsum()`
  3. **Anchor 价格精确恢复**：
     - 首个 Segment：`anchor_date = episode_start_date`，`anchor_close = first_close / (1.0 + first_episode_return)`
     - 后续 Segment：`anchor_date = prev_end_date`，`anchor_close = prev_end_close`
  4. **收益与回撤计算**：
     - `segment_return = end_close / anchor_close - 1.0`
     - `peak_return = peak_close / anchor_close - 1.0`
     - `max_drawdown = min(drawdown_path)`（在 `[anchor_close, ...close_path]` 上计算 running peak 得到）
  5. **`is_open` 状态**：
     - 未结束 Episode（`episode_end_date` 为 NaT/None）的最后一个 Segment 置为 `True`，其余置为 `False`。
  6. **返回标准 DataFrame**，与 `SYSTEM_B_EPISODE_SEGMENT` 列严格对齐。

#### [MODIFY] [`src/qrp_atlas/indicators/system_b/__init__.py`](../../../src/qrp_atlas/indicators/system_b/__init__.py)
- 导出 `calculate_system_b_episode_segments`, `SystemBEpisodeSegmentResult`, `SystemBEpisodeSegmentError`。

---

### 3. 数据管道与事务审计层 (Pipeline & Audit)

#### [MODIFY] [`src/qrp_atlas/pipeline/system_b_episode/service.py`](../../../src/qrp_atlas/pipeline/system_b_episode/service.py)
- **Schema 确保**：`ensure_schema` 中加入 `connection.execute(SYSTEM_B_EPISODE_SEGMENT.duckdb_create_sql())`。
- **事务清理顺序**：
  ```sql
  DELETE FROM system_b_episode_segment WHERE segment_version=?;
  DELETE FROM system_b_episode_observation WHERE rule_version=?;
  DELETE FROM system_b_episode WHERE rule_version=?;
  ```
- **批处理计算与写入**：
  - 调用 `calculate_system_b_episode_segments(episode_frame, observation_frame)`；
  - 填入 `created_run_id`, `source_episode_rule_version`, `segment_version`, `created_at`；
  - 批量写入 `system_b_episode_segment` 表；
- **级联 Audit 强化**：在 `audit_episodes` 中增加 Segment 专项质检：
  - `orphan_segments`：孤立 Segment 检查；
  - `segment_no_gaps`：Segment 编号连续无缺口；
  - `adjacent_same_state`：相邻 Segment 状态必须不同；
  - `trading_days_mismatch`：Segment 交易日累加值与 Observation 行数相等；
  - `segment_start_boundary_mismatch` / `segment_end_boundary_mismatch`：首尾日期与 Observation 精确对齐；
  - `first_anchor_mismatch`：首段 anchor_date 等于 episode_start_date；
  - `active_sprint_count_mismatch`：ACTIVE Segment 数等于 `ma5_reentry_count + 1`；
  - `return_closure_violation`：使用 `np.isclose(np.prod(1+segment_return), 1+episode_return, rtol=1e-10, atol=1e-12)` 进行严格连乘闭合校验，失败时抛出结构化字段（`episode_id`, `lhs`, `rhs`, `abs_error`, `rel_error`）。

---

### 4. 自动化测试 (Tests)

#### [NEW] [`tests/indicators/test_system_b_segment.py`](../../../tests/indicators/test_system_b_segment.py)
- 完整覆盖设计文档中定义的 10 个测试用例：
  1. Case 1: 单轮 ACTIVE（`active_sprint_no = 1`）
  2. Case 2: 一次 reentry（`ACTIVE -> NON_ACTIVE -> ACTIVE`，2 个 ACTIVE，1 个重入）
  3. Case 3: 多次 reentry（Sprint 1/2/3）
  4. Case 4: 长时间 NON_ACTIVE 但未结束
  5. Case 5: Episode End 终结日包含在最后一个 NON_ACTIVE Segment
  6. Case 6: Open Episode（`last_segment.is_open = True` 且与最新收益闭合）
  7. Case 7: 首日大涨 Anchor 恢复（`T0=10 -> T1=11` 归入 Seg 1，收益精确闭合）
  8. Case 8: 共享边界与多 Episode 隔离（杜绝跨 Episode 误合并）
  9. Case 9: 交易日自然日 Gap 处理
  10. Case 10: 确定性与重复计算输出一致性

#### [MODIFY] [`tests/pipeline/system_b_episode/test_production.py`](../../../tests/pipeline/system_b_episode/test_production.py)
- 增加对 `system_b_episode_segment` 生产写入、事务回滚、首尾日期边界校验与结构化 Return Closure 诊断的端到端集成测试。

---

## Verification Plan

### Automated Tests
1. **指标单测**：
   ```bash
   pytest tests/indicators/test_system_b_segment.py -v
   pytest tests/indicators/test_system_b_episode.py -v
   ```
2. **数据管道生产集成测试**：
   ```bash
   pytest tests/pipeline/system_b_episode/test_production.py -v
   ```
3. **全量回归测试**（验证旧模块 0 影响）：
   ```bash
   pytest tests/api/test_system_b_monitoring.py -v
   pytest tests/indicators/test_system_b_state_machine_v2.py -v
   pytest tests/indicators/test_system_b_pools.py -v
   ```

### Manual Verification
- 检查 Git 改动范围 `git status --short`，确认无额外无关文件变更。
- 检查所有 Python 语法、类型注解与格式。
