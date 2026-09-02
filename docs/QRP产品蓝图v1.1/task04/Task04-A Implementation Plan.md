# Task 04-A Implementation Plan (Remote Review Remediation)

## 1. Context & Goal

Task 04-A 是 QRP v1.1 题材与市场结构事实链的基石子包，涵盖从 `StockCollection` 顶级领域模型、双时间 PIT 解析、M4 有效成分股规则，到等权题材指数、趋势状态与 Episode 状态机、M4 观测值计算及 Pipeline 生产闭环。

针对远端审查指出的 13 项问题，本轮整改聚焦于：
1. **Replay / Daily Production 严格等价与历史 Episode 保护**；
2. **复用 System B 价格序列趋势状态机与 Episode 语义**；
3. **M4 Effective Member 上市事实判定的 Fail-Closed 严谨性**；
4. **Theme Index 全员收益完备性与 Gap 缺失语义**；
5. **M4 收益率统一比例口径与 Universe 映射**；
6. **StockCollection 1:1 原子创建、不可变身份与生命周期非重叠不变量**；
7. **双时间模型纠偏（区分可见性与业务有效性）**；
8. **批量集合化无 N+1 生产与 Lineage 审计**；
9. **正式 PipelineContract 接入与范围清理**。

---

## 2. Architecture & Technical Design

### A. StockCollection 领域层与双时间模型
- **1:1 原子创建**：`create_canonical_theme` 在单一 DuckDB 事务内原子插入 `STOCK_COLLECTION` 与 `THEME` 表，任一失败立即 ROLLBACK。
- **不可变 Identity 保护**：`remove_member` 与 `revise_member_late` 强制由 DB 加载不可变字段（`theme_id`, `collection_id`, `asset_id`），禁止调用方篡改。
- **Re-entry 语义**：重入前严格校验上一 lifecycle 已闭合，并生成全新 `membership_id`。
- **双时间模型**：
  - **可见性（Knowledge Date）**：`available_trade_date <= knowledge_date`；
  - **业务有效性（As-of Date）**：`effective_from <= as_of_date AND (effective_to IS NULL OR as_of_date < effective_to)`；
  - 彻底消除 `available_trade_date > as_of_date` 错误拦截。

### B. 指标纯计算层
- **有效成分股判定**：严格基于 `confirmed_listing_trading_day_count > 5` 事实，缺失时标记 `UNCONFIRMED_LISTING_DAYS` 并不计入有效成员。
- **题材等权指数**：
  - 必须全部有效成员收益率均非 NaN 才计算当日题材收益率；
  - 缺失任一成员收益率输出 NaN + Gap；
  - 0 有效成员输出 NaN，不伪造平盘；
  - Gap 发生时不增加累积连续指数，后续交易日基于前一个有效指数点继续复利。
- **趋势与 Episode 状态机**：
  - 严格复用 System B 价格序列状态机；
  - MA5 窗口不完整时不伪造 BASE；首次达到 MA5 时为 BASE；
  - Episode 确立必须连续 2 日站在 MA5 之上（Day 1: CANDIDATE, Day 2: ACTIVE）；
  - Episode 终结严格以跌破 MA10 为准。

### C. 生产管道与 Lineage 审计
- **Replay / Daily 等价性**：`run_m4_daily` 保证在相同 PIT 上下文与完整历史窗口下计算，与 Full Replay 输出逐行数值 100% 等价。
- **Lineage 追溯**：4 张 Theme 生产表增加 `production_run_id` 与 `input_snapshot_id` 字段，`audit_m4_observation` 服务集合化无 N+1 审计。
- **正式 PipelineContract**：声明 `THEME_M4_PRODUCTION_CONTRACT` 并注册到 `contract_catalog.py`。

---

## 3. Verification Strategy

1. **领域不变量测试**：`tests/stock_collections/test_domain_invariants.py`；
2. **双时间与 Re-entry 解析测试**：`tests/stock_collections/test_resolver.py`, `tests/stock_collections/test_pit_membership.py`；
3. **指标计算单元测试**：`tests/indicators/test_theme_*.py`, `tests/indicators/test_m4_observations.py`；
4. **Full Replay vs Daily Production 数值等价测试**：`tests/pipeline/theme/test_theme_production.py`；
5. **Lineage 审计测试**：`tests/pipeline/theme/test_theme_production.py`；
6. **全量 Contract 规范测试**：`pytest tests/pipeline/test_pipeline_contract.py tests/pipeline/test_pipeline_registry.py -v`。
