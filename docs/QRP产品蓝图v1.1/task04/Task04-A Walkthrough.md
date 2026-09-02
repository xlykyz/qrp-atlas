# Task 04-A Walkthrough (Remote Review Remediation)

## 1. 概述

本轮修改严格对照远端代码审查提出的 13 项整改要求，重构了 Task 04-A 的 StockCollection 领域层、双时间解析模型、M4 指标纯计算层、管道生产与 Lineage 审计，并通过端到端数值等价测试完成了闭环验证。

---

## 2. 变更详情

### A. 领域层与双时间模型
- [src/qrp_atlas/stock_collections/service.py](../../../src/qrp_atlas/stock_collections/service.py)：
  - `create_canonical_theme`：在单事务内原子写入 `stock_collection` 与 `theme`；
  - `add_member`：增加股票市场标的合法性校验与区间重叠校验；
  - `remove_member` & `revise_member_late`：强制由数据库加载不可变身份（`theme_id`, `collection_id`, `asset_id`），禁止调用方篡改；
  - `reenter_member`：校验旧生命周期已终结并分配独立的新 `membership_id`。
- [src/qrp_atlas/stock_collections/adapters/theme.py](../../../src/qrp_atlas/stock_collections/adapters/theme.py) & [src/qrp_atlas/stock_collections/resolver.py](../../../src/qrp_atlas/stock_collections/resolver.py)：
  - 纠偏双时间过滤语义（`available_trade_date <= knowledge_date` 决定可见性，`[effective_from, effective_to)` 决定业务有效性）；
  - 提供集合化批量解析能力 `batch_resolve_members`；
  - `explain_membership` 共享与 Resolver 100% 一致的 PIT 查询结果。

### B. 指标纯计算层
- [src/qrp_atlas/indicators/theme/effective_members.py](../../../src/qrp_atlas/indicators/theme/effective_members.py)：
  - 移除假定规则，严格基于 `confirmed_listing_trading_day_count > 5` 判定；无法证实天数时判定为 `UNCONFIRMED_LISTING_DAYS` 并排除。
- [src/qrp_atlas/indicators/theme/custom_index.py](../../../src/qrp_atlas/indicators/theme/custom_index.py)：
  - 严格全员完备性：有效成员中任一缺失收益率输出 NaN + Gap；0 有效成员输出 NaN，不伪造平盘；Gap 后正确恢复连续复利。
- [src/qrp_atlas/indicators/theme/trend_and_episode.py](../../../src/qrp_atlas/indicators/theme/trend_and_episode.py)：
  - 复用 System B 价格序列趋势状态机；MA5 窗口不完整时不伪造 BASE；首次满足 MA5 为 BASE；连续 2 日站在 MA5 上确立 Episode（CANDIDATE -> ACTIVE）；跌破 MA10 终结 Episode。
- [src/qrp_atlas/indicators/m4/observations.py](../../../src/qrp_atlas/indicators/m4/observations.py)：
  - 统一为标准小数 ratio 收益率；比较 Universe 缺失或无数据时 fail closed，不降级为局部排名。

### C. 管道契约、生产物化与只读 Replay
- [src/qrp_atlas/pipeline/theme/service.py](../../../src/qrp_atlas/pipeline/theme/service.py)：
  - **解耦计算与持久化**：拆分出纯计算核心 `calculate_m4_facts`，100% 内存无副作用执行；
  - **只读 Historical Replay**：新增专用入口 `replay_m4_facts(start_date, end_date, knowledge_date)`，返回不可变契约 `ThemeM4CalculatedFacts`，不写数据库、不产生事务；
  - **修复 Forward Dependency Closure 写回范围**：Canonical 修订写回范围与切片范围严格对齐 `[affected_output_start, affected_output_end]`，保证 `08-11 ~ 08-14` 完整保留并以本次 correction run_id 重新写入，绝不删空；
  - `run_m4_daily` 保证历史连续计算窗口，与 `rebuild_m4_facts` 产生数值等价结果。
- [src/qrp_atlas/pipeline/theme/query.py](../../../src/qrp_atlas/pipeline/theme/query.py)：
  - 集合化 `audit_m4_observation` 服务，追溯当次 `production_run_id`、输入快照、涨停股票与基准排名；
  - 缺省 knowledge_date 自动反查生产记录；
  - 审计重构依赖损坏或异常时严格 fail closed，判定 `is_reproducible = False` 与 `discrepancy_reason = "AUDIT_RECONSTRUCTION_FAILED"`。
- [src/qrp_atlas/pipeline/theme_contracts.py](../../../src/qrp_atlas/pipeline/theme_contracts.py) & [src/qrp_atlas/pipeline/contract_catalog.py](../../../src/qrp_atlas/pipeline/contract_catalog.py)：
  - 正式注册 `THEME_M4_PRODUCTION_CONTRACT`，声明全部 4 张 Canonical 业务事实表并对齐 `rows_written` 指标。
- [src/qrp_atlas/contracts/schema.py](../../../src/qrp_atlas/contracts/schema.py) & [deploy/duckdb/003_stock_collections_and_m4.sql](../../../deploy/duckdb/003_stock_collections_and_m4.sql)：
  - 为 4 张 Theme 生产表补充 `production_run_id` 和 `input_snapshot_id` 列，`theme_limit_up_count` 设为可空并保持 100% DDL 对齐。

### D. 范围清理
- 恢复 `src/qrp_atlas/orchestration/definitions.py`、`src/qrp_atlas/pipeline/system_b/service.py` 与 `tests/conftest.py` 到 `origin/develop/v1.1` 基线，不混入非任务改动。

---

## 3. 测试验证与基准性能实测结果

### 3.1 专项测试结果
执行 Theme 生产与回放专项测试套件：
```bash
pytest tests/pipeline/theme/test_theme_production.py -v
```
输出（12 项测试全部通过）：
```text
tests/pipeline/theme/test_theme_production.py::test_full_replay_vs_daily_production_exact_value_equality PASSED [  8%]
tests/pipeline/theme/test_theme_production.py::test_lineage_audit_and_input_snapshot_traceability PASSED [ 16%]
tests/pipeline/theme/test_theme_production.py::test_targeted_replay_exact_zero_drift_on_overlapping_range PASSED [ 25%]
tests/pipeline/theme/test_theme_production.py::test_option_a_daily_production_physical_scope_and_run_persistence PASSED [ 33%]
tests/pipeline/theme/test_theme_production.py::test_input_snapshot_id_determinism PASSED [ 41%]
tests/pipeline/theme/test_theme_production.py::test_source_drift_detection_in_lineage_audit PASSED [ 50%]
tests/pipeline/theme/test_theme_production.py::test_historical_correction_forward_dependency_closure PASSED [ 58%]
tests/pipeline/theme/test_theme_production.py::test_cross_range_existing_episode_update_on_historical_correction PASSED [ 66%]
tests/pipeline/theme/test_theme_production.py::test_deterministic_replay_with_as_of_and_different_knowledge_dates PASSED [ 75%]
tests/pipeline/theme/test_theme_production.py::test_replay_calculation_equivalence_with_canonical_rebuild PASSED [ 83%]
tests/pipeline/theme/test_theme_production.py::test_audit_reconstruction_failure_fails_closed PASSED [ 91%]
tests/pipeline/theme/test_theme_production.py::test_audit_defaults_to_persisted_production_knowledge_date PASSED [100%]

============================= 12 passed in 4.67s ==============================
```

### 3.2 任务相关全量测试套件
执行 Task 04-A 全部相关测试套件：
```bash
pytest tests/contracts/ tests/indicators/ tests/stock_collections/ tests/pipeline/theme/ tests/pipeline/test_theme_contracts.py -v
```
输出（298 项测试全部通过）：
```text
============================ 298 passed in 17.17s =============================
```
- `tests/contracts/`: 24 passed
- `tests/indicators/`: 240 passed
- `tests/stock_collections/`: 19 passed
- `tests/pipeline/theme/`: 12 passed
- `tests/pipeline/test_theme_contracts.py`: 3 passed

### 3.3 全量 `pytest tests/ -v` 本地环境阻塞说明
- 执行 `pytest tests/ -v` 时，因本机环境为 Windows 原生开发环境，且本任务严格移除非 Task 04-A 越界补丁，在扫描收集外部非修改文件 `src/qrp_atlas/pipeline/system_b/service.py:7` 时遭遇 `ModuleNotFoundError: No module named 'resource'`（`resource` 为 Linux/Unix 专有库）。
- 该阻塞属于外部平台基线约束，Task 04-A 自身涉及的 298 项测试完全独立且 100% 通过。

### 3.4 语法与静态检查
- `python -m compileall -q src tests`：零错误退出（exit code 0）
- `git diff --check`：零空格/换行告警（exit code 0）

### 3.5 代表性生产规模 Benchmark 证据
运行测试脚本：
```bash
python scratch/benchmark_theme_m4.py
```
实测输出：
```text
=== Benchmark Results ===
Themes processed:             50
Stocks per theme:             25
Total membership links:       1,250
Historical inception window:  120 trading days (2026-01-05 to 2026-06-19)
Market fact rows scanned:     60,000
Comparison universe rows:     1,200
Target date outputs written:
  - Daily index rows:         50
  - Trend state rows:         50
  - Episodes tracked:         32
  - M4 observations:          50
Total elapsed time:           24.373 s
Throughput (time per theme):  487.46 ms/theme
Reported write & exec time:   24.371 s
Python memory allocated/peak: 0.43 MB / 60.57 MB
```
在 50 个主题、1,250 个成员、半年回溯窗口及 60,000 条行情事实下，Option A 日间全量计算与落库耗时 **24.37 秒**，峰值内存 **60.57 MB**。
