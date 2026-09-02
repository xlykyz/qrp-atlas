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

### C. 管道契约与生产审计
- [src/qrp_atlas/pipeline/theme/service.py](../../../src/qrp_atlas/pipeline/theme/service.py)：
  - 集合化生产与历史重放服务，批量解析成员无 N+1；单 DuckDB 事务原子写入 4 张表；
  - `run_m4_daily` 保证历史连续计算窗口，与 `rebuild_m4_facts` 产生数值等价结果。
- [src/qrp_atlas/pipeline/theme/query.py](../../../src/qrp_atlas/pipeline/theme/query.py)：
  - 集合化 `audit_m4_observation` 服务，追溯当次 `production_run_id`、输入快照、涨停股票与基准排名。
- [src/qrp_atlas/pipeline/theme_contracts.py](../../../src/qrp_atlas/pipeline/theme_contracts.py) & [src/qrp_atlas/pipeline/contract_catalog.py](../../../src/qrp_atlas/pipeline/contract_catalog.py)：
  - 正式注册 `THEME_M4_PRODUCTION_CONTRACT`。
- [src/qrp_atlas/contracts/schema.py](../../../src/qrp_atlas/contracts/schema.py) & [deploy/duckdb/003_stock_collections_and_m4.sql](../../../deploy/duckdb/003_stock_collections_and_m4.sql)：
  - 为 4 张 Theme 生产表补充 `production_run_id` 和 `input_snapshot_id` 列。

### D. 范围清理
- 恢复 `src/qrp_atlas/orchestration/definitions.py` 与 `src/qrp_atlas/pipeline/system_b/service.py` 到 `origin/develop/v1.1` 基线，不混入非任务改动。

---

## 3. 测试验证结果

执行本地测试套件，全部 117 项测试通过：

```bash
pytest tests/contracts/ tests/stock_collections/ tests/indicators/test_theme_effective_members.py tests/indicators/test_theme_custom_index.py tests/indicators/test_theme_trend_and_episode.py tests/indicators/test_m4_observations.py tests/pipeline/theme/ tests/pipeline/test_pipeline_contract.py tests/pipeline/test_pipeline_registry.py -v
```

输出：
`117 passed in 3.37s`

### 关键专项验证点：
1. `tests/pipeline/theme/test_theme_production.py::test_full_replay_vs_daily_production_exact_value_equality`：验证 Full Replay 与 逐日 Daily 生产生成的 `index_level`, `theme_daily_return`, `trend_state`, `m4_observations` **数值逐行 100% 精确等价**。
2. `tests/pipeline/theme/test_theme_production.py::test_lineage_audit_and_input_snapshot_traceability`：验证 Lineage 审计无 N+1 查询，准确回溯 `production_run_id`、`input_snapshot_id` 及涨停列表。
3. `tests/stock_collections/test_domain_invariants.py`：验证 1:1 原子创建、不可变身份校验及生命周期互斥不变量。
4. `tests/stock_collections/test_resolver.py` & `tests/stock_collections/test_pit_membership.py`：验证双时间维度可见性与业务有效性隔离、Late Revision 与 Re-entry 语义。
5. `tests/pipeline/test_pipeline_contract.py`：验证 CLI 契约静态与动态检验通过（29 个有效契约）。
