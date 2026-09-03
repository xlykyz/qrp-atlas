# Task 04-A Walkthrough (Semantic Finalization & Production Verification)

## 1. 概述与核心成果

Task 04-A 完成了 QRP v1.1 题材与市场结构事实链的正式生产语义收口。根据 `docs/QRP产品蓝图v1.1/task04/M4 Effective Member Rule v1.0.0.md` (`theme_m4_effective_member@1.0.0`) 规范及最终代码审查要求，本任务确立了不可篡改的正式生产账本、严格盘前准入截止、交易日 D 级原子事务与完整血缘追溯。

### 核心设计决策与生产语义基准
1. **M4 有效成分股规则 (`theme_m4_effective_member@1.0.0`)**：
   - 准入严格按照 `available_trade_date <= D` 且 `ingested_at < D 09:00:00 Asia/Shanghai` 过滤；
   - 上市交易日事实严格依赖 System B 市场事实断言 `confirmed_listing_trading_day_count > 5`。缺失或无法证实严格 fail-closed 归入 `UNCONFIRMED_LISTING_DAYS`；天数 `<= 5` 归入 `NEW_LISTING_LE_5`；停牌归入 `SUSPENDED`；
   - 严格排除优先级：`UNCONFIRMED_LISTING_DAYS` > `NEW_LISTING_LE_5` > `SUSPENDED`。
2. **交易日 D 09:00:00 准入截止 (Admission Cutoff)**：
   - 生产领域模型 `create_canonical_theme` / `add_member` / `remove_member` / `revise_member_late` 彻底移除调用方 `ingested_at` 倒填能力，由注入的系统 Clock 生成 aware 时间；
   - 生产准入直接以内生交易日 `d.trade_date` 为准，完全解耦外部传入的 `knowledge_date`。
3. **数据库权威交易日历**：
   - 所有交易日查询严格从数据库 `trading_calendar` 契约表获取，杜绝任何外部网络抓取或静态推算。
4. **`theme_effective_member_daily` 物理持久化（第 5 张 Canonical 事实表）**：
   - 模式包含 `is_m4_effective_member`, `exclusion_reason`, `confirmed_listing_trading_day_count`, `is_theme_member`, `production_run_id`, `input_snapshot_id`；
   - 生产顺序严格为：`Membership -> Admission -> Eligibility -> INSERT theme_effective_member_daily -> SELECT FROM theme_effective_member_daily -> Custom Index`，指数计算消费物理落库事实。
5. **Trading-Day D 级原子事务 (Atomic Daily Unit)**：
   - 因 `theme_return_rank` 依赖跨 Theme 全局事实，生产原子单位提升为整个交易日 D；
   - 事务从 Step 1: `ExpectedThemes(D)` 到 Step 6: `theme_production_run` 统一提交，任一题材或指标失败全日 `ROLLBACK`，杜绝局部提交；重试保证全日事实使用同一 `input_snapshot_id` 与 `production_run_id` 提交。
6. **零孤儿谱系 (Zero Orphan Lineage)**：
   - 所有 5 张 Canonical 事实表的 `production_run_id` 均在同事务内写入 `theme_production_run` 表，`SELECT DISTINCT production_run_id FROM canonical_facts` 100% 能够外键关联。
7. **不可变已提交账本 (Immutable Finalized Ledger)**：
   - 已 finalized 历史事实行 100% 冻结，禁止任何形式的覆盖或重写；
   - 彻底废除历史纠偏导致已提交区间重述（No Historical Correction Restatement）；后期成员维护通过新 `revision_id` 记录，生产历史保持不可篡改。
8. **已落库有限指数锚点 (Finalized Finite Index Anchor)**：
   - 题材日常生产查询历史最近一条非 NULL 的 `index_level` 继续累积复利，不从 inception 全量重算。
9. **连续价格窗口均线 (Contiguous MA Gap Semantics)**：
   - MA5/MA10 回溯保留历史 NULL 行；遇到第一个 NULL 立即终止回溯；任一缺失立即重置均线窗口。
10. **OPEN Episode 生命周期与 D-1 事实稳定**：
    - 状态机推进生成新 Episode 时，`episode_start_date = D-1`, `episode_confirmed_date = D`, `episode_end_date = NULL`；
    - 严禁反向 UPDATE 改写 D-1 历史 Observation，D-1 行 100% bit-stable。
11. **生产与研究回放分离 (Production vs Research Replay)**：
    - Production 锚定 D 09:00 冻结历史；
    - Research Replay (`replay_m4_facts`) 纯只读，按 `(as_of, knowledge)` 自建历史演进路径，基准指数从 1000.0 起始，严禁使用 canonical 的上一日 index_level 或 canonical 历史 index 作为路径依赖锚点。
    - Lineage 审计对齐：`audit_m4_observation()` 重构输入快照时使用 `ExpectedThemes(D)`，杜绝外部 `knowledge_date` 导致的虚假快照漂移。
12. **显式 TIMESTAMPTZ 迁移机制**：
    - `init_stock_collections_database()` 不自动迁移历史数据库；
    - 提供独立 `migrate_stock_collections_ingested_at_to_timestamptz(con, source_timezone)`，强制要求调用方明确已验证的来源时区，严格断言列类型，失败立即 fail-closed。

---

## 2. 变更文件清单

### A. 领域模型与时区迁移
- `src/qrp_atlas/contracts/schema.py`：
  - 完善 `migrate_stock_collections_ingested_at_to_timestamptz`：要求传入显式 `source_timezone` 并校验有效性，执行后严格断言 `TIMESTAMPTZ` 类型，失败 fail-closed；
  - `init_stock_collections_database` 仅负责创建标准 TIMESTAMPTZ 契约表，不自动隐式迁移。
- `deploy/duckdb/004_migrate_ingested_at_to_timestamptz.sql`：
  - 显式独立迁移脚本，附带严格的来源时区核验声明。
- `src/qrp_atlas/stock_collections/service.py`：
  - 移除生产 domain API 的外部 `ingested_at` 参数，全部由系统 Clock 生成 aware 时间；
  - 移除 `MEMBERSHIP_EFFECTIVE_FROM_IMMUTABLE`，允许通过新 revision 修订 `effective_from / effective_to`，并重跑边界与重叠校验；历史 finalized 事实严格不变。
- `src/qrp_atlas/stock_collections/adapters/theme.py` & `src/qrp_atlas/stock_collections/resolver.py`：
  - `batch_resolve_members` 在 `enforce_admission_cutoff=True` 模式下严格以内生交易日 `d.trade_date` 为准，完全解耦外部传入的 `knowledge_date`。

### B. 生产管道、回放与审计
- `src/qrp_atlas/pipeline/theme/service.py`：
  - 将 `_produce_single_day` 提升为 Trading-Day D 级原子事务；
  - 保证事实落库顺序：先写入 `theme_effective_member_daily`，随后 SELECT 落库事实计算 Custom Index；
  - 在同事务中原子写入 `theme_production_run`，消除孤儿谱系；
  - 删除 Episode 确认时反向 UPDATE D-1 Observation 的逻辑，保持 D-1 事实 100% bit-stable；
  - 均线回溯从 D 向过去建立 contiguous suffix，遇到第一个 NULL 立即终止，不跨越 gap；
  - `calculate_m4_facts`：彻底去除对 canonical `theme_custom_index_daily` 的 previous level 和历史路径依赖，自建独立历史演进；
  - `_fetch_replay_canonical_themes` 与 `_fetch_all_canonical_themes` 分离，彻底解耦 Production 冻结宇宙与 Replay 视界。
- `src/qrp_atlas/pipeline/theme/query.py`：
  - `audit_m4_observation` 重建输入快照时严格采用 Production `ExpectedThemes(D)` 准入条件与生命周期，防止盘中晚录主题造成虚假 snapshot drift。

---

## 3. 测试验证与质量证据

### 3.1 语义收口与边界回归套件
执行包含全部 20 项强制边界、8 项审查返修与 3 项收尾测试的专用套件：
```bash
pytest tests/pipeline/theme/test_m4_semantic_finalization.py -v
```
**结果**：38 项测试全部通过（38 passed in 9.19s）。

核心测试场景包括：
1. **08:59:59 录入**：D 日立即生效进入候选；
2. **09:00:00 录入**：D 日不生效，D_next 才生效；
3. **09:00:01 录入**：即使声明 `effective_from <= D`，D 日也不生效；
4. **跨午夜维护与盘前维护**：正常于 D 日 09:00 前生效；
5. **上市天数判定**：事实缺失 fail-closed 为 `UNCONFIRMED_LISTING_DAYS`；`<= 5` 为 `NEW_LISTING_LE_5`；`> 5` 正常通过；
6. **停牌与优先级**：既 unconfirmed 又 suspended 严格优先标记为 `UNCONFIRMED_LISTING_DAYS`；
7. **事实物理落库**：`theme_effective_member_daily` 主键与物理行完整性；
8. **不可变账本**：历史倒填更早成员，已 finalized 历史行与数值绝对不变；
9. **Trading-Day D 原子回滚**：Theme B 插入异常触发整日全量表 ROLLBACK，重试以统一快照完整提交；
10. **零孤儿谱系**：所有 canonical facts 的 `production_run_id` 100% 能够外键关联到 `theme_production_run`；
11. **D-1 事实稳定**：D 日确认 Episode 时，D-1 Observation 行 100% bit-stable；
12. **Replay 路径独立性**：后视镜新增成员导致 Replay D1 变化，D2 指数严格基于 Replay D1 推进，完全独立于 Canonical D1；
13. **Audit 快照对齐**：D 10:00 创建的晚录主题不引起 Audit 虚假 snapshot drift；
14. **显式 TIMESTAMPTZ 迁移**：未传或非法来源时区 fail-closed，明确 UTC 迁移后严格断言 TIMESTAMPTZ 类型。

### 3.2 任务相关全量测试套件
执行 Task 04-A 全部关联模块测试套件：
```bash
pytest tests/stock_collections tests/pipeline/theme tests/pipeline/test_theme_contracts.py tests/indicators
```
**结果**：
```text
============================ 308 passed in 29.42s =============================
```
- `tests/stock_collections/`: 19 passed
- `tests/pipeline/theme/`: 50 passed (`test_m4_semantic_finalization.py` 38 项 + `test_theme_production.py` 12 项)
- `tests/pipeline/test_theme_contracts.py`: 3 passed
- `tests/indicators/`: 236 passed

全量 308 项测试 100% 通过，无截断、无格式告警、无未捕获异常。
