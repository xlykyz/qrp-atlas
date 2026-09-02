# Walkthrough: System B Episode 统计粒度细化 (Episode Segment)

本次改造完成了 **System B Episode Segment（行情轮次子段）** 的标准下游派生实现。严格遵循 **Append-only Downstream Derivation** 原则，对现有的状态机、行情轮次（Episode）、股票池和交易策略保持 **零修改、100% 向下兼容**。

---

## 1. 核心变更概览

### (1) 契约与 Schema 层
* [`src/qrp_atlas/contracts/system_b.py`](file:///e:/projects/qrp-atlas/src/qrp_atlas/contracts/system_b.py)：
  * 定义表名 `system_b_episode_segment` 与版本 `system_b_episode_segment@1.0.0`；
  * 定义 `SystemBSegmentState` 枚举（`ACTIVE` / `NON_ACTIVE`）；
  * 定义全量字段常量（`SEGMENT_ID`, `ACTIVE_SPRINT_NO`, `ANCHOR_CLOSE`, `SEGMENT_RETURN`, `IS_OPEN` 等）。
* [`src/qrp_atlas/contracts/schema.py`](file:///e:/projects/qrp-atlas/src/qrp_atlas/contracts/schema.py)：
  * 新增 `SYSTEM_B_EPISODE_SEGMENT` TableSchema 定义，主键为 `(segment_id,)`。
* [`deploy/duckdb/002_system_b_episode.sql`](file:///e:/projects/qrp-atlas/deploy/duckdb/002_system_b_episode.sql)：
  * 增加 DuckDB DDL 建表定义。

### (2) 指标派生层 (Indicators)
* [`src/qrp_atlas/indicators/system_b/segment.py`](file:///e:/projects/qrp-atlas/src/qrp_atlas/indicators/system_b/segment.py)：
  * 实现核心算法 `calculate_system_b_episode_segments(episodes, observations)`；
  * **边界隔离与矢量化**：基于 `["episode_id", "trade_date"]` 稳定排序，按 `episode_id` 组内 `shift` 隔离检测状态切换；
  * **Anchor 价格精确恢复**：首段通过 $\frac{\text{close}}{1 + \text{episode\_return}}$ 逆推恢复，后续段顺延上一段 `end_close`；
  * **连乘闭包与风险指标**：计算 `segment_return`、`peak_close`、`peak_date`、`peak_return`、`max_drawdown` 与 `is_open`。

### (3) 数据管道与事务审计层 (Pipeline & Audit)
* [`src/qrp_atlas/pipeline/system_b_episode/service.py`](file:///e:/projects/qrp-atlas/src/qrp_atlas/pipeline/system_b_episode/service.py)：
  * 级联删除与原子写入：在同一 DuckDB 事务内完成 `system_b_episode_segment` 批处理写入；
  * **严谨的级联 Audit 校验**：
    * `return_closure_violations`：使用 `np.isclose(rtol=1e-10, atol=1e-12)` 进行严格连乘闭合校验；
    * `orphan_segments` / `segment_no_gaps` / `adjacent_same_state` / `first_anchor_mismatch`；
    * `trading_days_mismatch`：使用独立 CTE 聚合对比，杜绝多对多笛卡尔积；
    * `active_sprint_count_mismatch`：强校验 `Count(ACTIVE) == ma5_reentry_count + 1`。

---

## 2. 自动化测试与验证结果

### (1) Segment 指标全量用例 (12/12 PASSED)
运行 [`tests/indicators/test_system_b_segment.py`](file:///e:/projects/qrp-atlas/tests/indicators/test_system_b_segment.py)：
```text
tests/indicators/test_system_b_segment.py::test_case_1_single_active PASSED
tests/indicators/test_system_b_segment.py::test_case_2_one_reentry PASSED
tests/indicators/test_system_b_segment.py::test_case_3_multiple_reentries PASSED
tests/indicators/test_system_b_segment.py::test_case_4_long_non_active_not_ended PASSED
tests/indicators/test_system_b_segment.py::test_case_5_episode_ended PASSED
tests/indicators/test_system_b_segment.py::test_case_6_open_episode PASSED
tests/indicators/test_system_b_segment.py::test_case_7_first_active_large_jump PASSED
tests/indicators/test_system_b_segment.py::test_case_8_multiple_episodes_isolated PASSED
tests/indicators/test_system_b_segment.py::test_case_9_calendar_gap_preserves_trading_days PASSED
tests/indicators/test_system_b_segment.py::test_case_10_determinism PASSED
tests/indicators/test_system_b_segment.py::test_empty_inputs_return_empty_result PASSED
tests/indicators/test_system_b_segment.py::test_missing_required_columns_raises_error PASSED
```

### (2) 生产 Pipeline 事务与 Audit 检验 (10/10 PASSED)
运行 [`tests/pipeline/system_b_episode/test_production.py`](file:///e:/projects/qrp-atlas/tests/pipeline/system_b_episode/test_production.py)：
```text
tests/pipeline/system_b_episode/test_production.py::test_rebuild_uses_state_table_and_official_sma PASSED
tests/pipeline/system_b_episode/test_production.py::test_audit_rejects_segment_invariants PASSED
... 全部 10 项生产集成测试通过 ...
```

### (3) System B 全量回归测试 (48/48 PASSED)
涵盖 API 监控层、状态机 v2、股票池算法与股票池生产 Pipeline：
```text
tests/api/test_system_b_monitoring.py (9 passed)
tests/indicators/test_system_b_state_machine_v2.py (17 passed)
tests/indicators/test_system_b_pools.py (15 passed)
tests/pipeline/system_b_pools/test_service.py (7 passed)
```

**总计 82 项自动化测试 100% 通过，零回归故障。**

---

## 3. 服务器同步提示

根据项目协作规范，本次改动属于纯代码与契约层变更（无破坏性数据迁移）：
- **改动范围**：`src/qrp_atlas/`、`deploy/duckdb/`、`tests/`；
- **同步后操作**：在 Linux 服务器同步代码后，重跑 `qrp-atlas-system-b-episode` 生产任务即可自动建表并产出 `system_b_episode_segment` 数据。
