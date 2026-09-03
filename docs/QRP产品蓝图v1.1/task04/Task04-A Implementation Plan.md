# Task 04-A Implementation Plan (Semantic Finalization & Production Baseline)

## 1. Context & Goal

Task 04-A 是 QRP v1.1 题材与市场结构事实链的核心生产子包，涵盖从 `StockCollection` 领域模型、双时间 PIT 解析、M4 有效成分股规则，到等权题材指数、趋势状态与 Episode 状态机、M4 观测值计算及 Pipeline 生产闭环。

本方案以业务权威规范 `docs/QRP产品蓝图v1.1/task04/M4 Effective Member Rule v1.0.0.md` (`theme_m4_effective_member@1.0.0`) 及代码审查结论为唯一准绳，确立正式生产语义基线：
1. **M4 有效成分股规则 (`theme_m4_effective_member@1.0.0`)**：盘前准入判定、上市交易天数与停牌严格优先级；
2. **交易日 D 09:00:00 Asia/Shanghai 准入截止 (Admission Cutoff)**：权威 `t` 由系统时钟生成，严禁倒填；
3. **数据库权威交易日历**：从 `trading_calendar` 表获取合法交易日 `D`，严禁伪造或依赖外部网络日历；
4. **`theme_effective_member_daily` 物理持久化**：确立为第 5 张 Canonical 事实表，指数计算严格消费落库事实；
5. **Trading-Day D 级原子事务**：生产原子单位提升为整交易日 D，全题材与全榜单跨 Theme 排名统一提交或整日回滚；
6. **不可变已提交账本 (Immutable Finalized Ledger)**：生产事实一旦落库严禁改写，杜绝任何历史纠偏重述 (No Historical Correction Restatement)；
7. **已落库有限指数锚点 (Finalized Finite Index Anchor)**：Daily 延续从最近有效 finite level 推进，不重算 inception；
8. **连续价格窗口均线 (Contiguous MA Gap Semantics)**：MA5/MA10 严格沿连续有效价格回溯，遇 NULL gap 立即重置窗口；
9. **OPEN Episode 生命周期与 D-1 事实稳定**：状态机推进保留 OPEN 状态，严禁反向 UPDATE 改写 D-1 历史 Observation；
10. **生产与研究回放分离 (Production vs Research Replay)**：Production 锚定 D 09:00 冻结历史；Replay 依据 `(as_of, knowledge)` 纯只读自建历史路径，无 canonical 依赖；
11. **显式 TIMESTAMPTZ 迁移机制**：非自动隐式执行，要求调用方明确已验证的来源时区，严格断言 schema 并 fail-closed。

---

## 2. Architecture & Technical Design

### A. 领域层、时区与准入截止 (StockCollection & Cutoff)
- **不可伪造 Authoritative `t`**：生产领域接口（`create_canonical_theme` / `add_member` / `remove_member` / `revise_member_late`）不再接受外部传入 `ingested_at`，严格由注入的系统时钟 Clock 生成 aware `datetime`。
- **物理时区 TIMESTAMPTZ**：`stock_collection`、`theme`、`theme_membership_history` 的 `ingested_at` 统一为 `TIMESTAMP WITH TIME ZONE`，消除 DuckDB 会话 `SET TimeZone` 导致的比较漂移。
- **显式数据迁移**：`init_stock_collections_database()` 不自动迁移历史数据库。提供独立 `migrate_stock_collections_ingested_at_to_timestamptz(con, source_timezone)`，调用方必须显式指定原始时区（如 `'UTC'`），经校验后通过 `USING timezone(source_timezone, ingested_at)` 显式迁移，并强制断言列类型，失败立即 fail-closed。
- **D 09:00:00 准入判定**：
  - 准入条件：`m.available_trade_date <= d.trade_date AND m.ingested_at < (d.trade_date + INTERVAL 9 HOUR)::TIMESTAMP AT TIME ZONE 'Asia/Shanghai'`；
  - 生产模式下完全解耦调用方传入的 `knowledge_date`，直接以内生交易日 `d.trade_date` 判定准入。
- **Membership Lifecycle Revision**：允许通过新 `revision_id` 修正 `effective_from` 和 `effective_to`，并重新校验所属题材/合集生命周期与资产重叠；不可改写既有 finalized 生产事实。

### B. M4 有效成分股规则与物理事实表
- **第 5 张 Canonical 事实表**：`theme_effective_member_daily`
  - 键：`PRIMARY KEY (collection_id, asset_id, trade_date)`；
  - 核心列：`theme_id`, `is_m4_effective_member`, `exclusion_reason`, `confirmed_listing_trading_day_count`, `is_theme_member`, `production_run_id`, `input_snapshot_id`；
- **判别优先级**：
  1. `confirmed_listing_trading_day_count` 事实缺失或为 `UNRESOLVED_MISSING` -> 判定为 `UNCONFIRMED_LISTING_DAYS` 并排除；
  2. `confirmed_listing_trading_day_count <= 5` -> 判定为 `NEW_LISTING_LE_5` 并排除；
  3. 停牌事实（`suspend_d` 或 `EXPLICIT_NON_TRADING`） -> 判定为 `SUSPENDED` 并排除；
  4. 满足上市交易日 > 5 且非停牌 -> `is_m4_effective_member = TRUE`, `exclusion_reason = NULL`。

### C. 等权指数计算与连续价格窗口均线
- **消费物理落库事实**：生产执行顺序严格为：`Membership -> Admission -> Eligibility -> INSERT theme_effective_member_daily -> SELECT FROM theme_effective_member_daily -> Custom Index`。
- **Inception 与有限累积锚点**：
  - 题材首个交易日以 `DEFAULT_BASE_LEVEL = 1000.0` 为起始点；
  - 日常增量生产向前查询最近一条有效指数：`SELECT index_level FROM theme_custom_index_daily WHERE theme_id = ? AND trade_date < D AND index_level IS NOT NULL ORDER BY trade_date DESC LIMIT 1`；
  - 若所有历史均为 NULL，则回退至 1000.0。
- **Contiguous MA Gap 语义**：
  - 查询历史价格窗口时必须保留 NULL 行；
  - 从 D 沿连续历史向前回溯（contiguous suffix），遇到第一个 NULL 立即终止回溯；
  - 均线窗口遇 gap 严格重置：重新累计 5 个连续有效点才产生 MA5，重新累计 10 个才产生 MA10。

### D. 趋势状态机、OPEN Episode 与 D-1 事实稳定
- **状态机流转**：复用 System B 价格序列状态机；
- **OPEN Episode 生命周期**：
  - 当第 D-1 天满足 `is_above`（突破 MA5 形成 CANDIDATE）、第 D 天继续满足 `is_above` 时，状态确立为 ACTIVE，正式确认 Episode；
  - 新建 Episode 记录：`episode_start_date = D-1`, `episode_confirmed_date = D`, `episode_end_date = NULL`；
  - **严禁回写 D-1 Observation**：删除反向 UPDATE D-1 observation 的逻辑，保证已提交的 D-1 Observation 行 100% bit-stable。

### E. Trading-Day D 级原子事务与 Lineage 闭环
- **整日原子事务**：
  - 单一交易日 D 开启单一事务 `BEGIN TRANSACTION ... COMMIT`；
  - 跨 Theme 排名 `theme_return_rank` 统一计算并落库；
  - 任何表（Effective Member、Custom Index、State、Episode、Observation、Production Run）写入失败立即 `ROLLBACK`；
  - 消除 partial-finalized themes 状态；重试保证全日事实使用同一 `input_snapshot_id` 与 `production_run_id` 提交；
- **零孤儿谱系 (Zero Orphan Lineage)**：`theme_production_run` 记录在同事务内提交，`SELECT DISTINCT production_run_id FROM canonical_facts` 100% 能够关联到 `theme_production_run`。
- **不可变账本 (Immutable Ledger)**：已 finalize 的历史交易日严禁被后续任务覆盖；后续历史数据维护通过新 revision 记录，不触发历史重算。

### F. 生产与研究回放分离 (Production vs Research Replay)
- **Production**：使用 `ExpectedThemes(D)`，严格受 D 09:00 cutoff 限制，消费并维护不可变历史账本。
- **Research Replay (`replay_m4_facts`)**：
  - 纯只读，100% 内存无副作用；
  - 按 `(as_of_date, knowledge_date)` 查询该时点认知下的历史全集，不受 09:00 cutoff 限制；
  - 形成自身独立的历史演进路径，基准指数从 1000.0 起始，**严禁使用 canonical 的上一日 index_level 或 canonical 历史 index 作为路径依赖锚点**。
- **Lineage 审计对齐**：`ThemeQueryService.audit_m4_observation()` 重构输入快照时，Theme Universe 严格使用 `ExpectedThemes(D)`，不得因外部 `knowledge_date` 导致虚假的快照漂移。

---

## 3. Verification Strategy

1. **准入与成分股规则测试**：`tests/pipeline/theme/test_m4_semantic_finalization.py`（38 项端到端覆盖）；
2. **领域不变量与 Revision 测试**：`tests/stock_collections/test_domain_invariants.py`；
3. **双时间与解析测试**：`tests/stock_collections/test_resolver.py`, `tests/stock_collections/test_pit_membership.py`；
4. **指标纯计算单元测试**：`tests/indicators/test_theme_*.py`, `tests/indicators/test_m4_observations.py`；
5. **生产契约与管道审计测试**：`tests/pipeline/theme/test_theme_production.py`, `tests/pipeline/test_theme_contracts.py`；
6. **全量回归基线**：全套 308 项测试 100% 通过。
