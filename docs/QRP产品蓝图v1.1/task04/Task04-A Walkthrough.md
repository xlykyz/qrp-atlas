# Walkthrough: Task 04-A｜M4 完整事实能力

## 1. 概述与核心交付

本任务完成了从 **THEME StockCollection**、**PIT Theme Membership** 到 **题材等权指数**、**趋势状态 / Episode**、**M4 Raw Observations**、**历史回放与生产查询** 的完整事实闭环。

```text
Theme
  → THEME StockCollection (COLL:THEME:QRP:{SOURCE_KEY})
  → PIT Theme Membership (双时间模型，[effective_from, effective_to) @ available_trade_date)
  → M4 Effective Members (排除实际上市 ≤ 5日新股与停牌股，不污染 Membership 关系)
  → Equal-weight Theme Index (连续复合指数，0成员输出 NULL)
  → Theme Index State / Episode (MA5/MA10/BASE/CANDIDATE/ACTIVE，无新股 warmup 污染)
  → M4 Raw Observations (日收益率、收盘涨停计数、m4_board_universe_v1 排名、固定 NOT_CONFIGURED)
  → Historical Replay / Daily Production / Query / Audit (单事务原子写入、幂等性、可解释性追踪)
```

---

## 2. 改动清单

### (1) 契约与持久化 Schema (`src/qrp_atlas/contracts/`)
- [`stock_collection.py`](file:///e:/projects/qrp-atlas/src/qrp_atlas/contracts/stock_collection.py): 定义 `CollectionType.THEME`, `CollectionScope`, `MembershipModel.INTERVAL` 等枚举与字段常量；
- [`m4.py`](file:///e:/projects/qrp-atlas/src/qrp_atlas/contracts/m4.py): 定义 M4 事实、等权指数与板块对比常量（`m4_board_universe_v1`, `NOT_CONFIGURED`）；
- [`schema.py`](file:///e:/projects/qrp-atlas/src/qrp_atlas/contracts/schema.py): 定义并注册 7 张新表到 `ALL_TABLES` 与 `TABLE_BY_NAME`：
  - `stock_collection`, `theme`, `theme_membership_history`, `theme_custom_index_daily`, `theme_custom_index_state`, `theme_custom_index_episode`, `theme_m4_observation`；
- [`deploy/duckdb/003_stock_collections_and_m4.sql`](file:///e:/projects/qrp-atlas/deploy/duckdb/003_stock_collections_and_m4.sql): 对应的 DDL 迁移脚本。

### (2) StockCollection 顶级领域 (`src/qrp_atlas/stock_collections/`)
- [`identity.py`](file:///e:/projects/qrp-atlas/src/qrp_atlas/stock_collections/identity.py): 稳定身份生成器 `make_collection_id`（`COLL:THEME:QRP:{KEY}`）；
- [`models.py`](file:///e:/projects/qrp-atlas/src/qrp_atlas/stock_collections/models.py): 领域实体、PIT 查询上下文、ResolvedMember 与解释模型；
- [`repository.py`](file:///e:/projects/qrp-atlas/src/qrp_atlas/stock_collections/repository.py): DuckDB 原子写入与 PIT 读取仓储；
- [`adapters/theme.py`](file:///e:/projects/qrp-atlas/src/qrp_atlas/stock_collections/adapters/theme.py): 题材双时间成员解析、反向查询（Reverse Lookup）与可解释性；
- [`resolver.py`](file:///e:/projects/qrp-atlas/src/qrp_atlas/stock_collections/resolver.py): 集合解析中心，严格执行 Scope 与生效时间控制；
- [`service.py`](file:///e:/projects/qrp-atlas/src/qrp_atlas/stock_collections/service.py): 题材生命周期管理（1:1 原子创建、Add/Remove/Late Revision/Re-entry）。

### (3) 指标纯计算层 (`src/qrp_atlas/indicators/theme/` & `m4/`)
- [`effective_members.py`](file:///e:/projects/qrp-atlas/src/qrp_atlas/indicators/theme/effective_members.py): 计算 M4 计算资格，严格将新股上市 $\le 5$ 日与停牌股排除（不修改原成员关系）；
- [`custom_index.py`](file:///e:/projects/qrp-atlas/src/qrp_atlas/indicators/theme/custom_index.py): 算术平均等权指数与连续复利计算，0 有效成员输出 NULL（非 0）；
- [`trend_and_episode.py`](file:///e:/projects/qrp-atlas/src/qrp_atlas/indicators/theme/trend_and_episode.py): 价格序列 MA5/MA10、趋势状态机与 Episode 计算，排除新股 warmup 污染；
- [`observations.py`](file:///e:/projects/qrp-atlas/src/qrp_atlas/indicators/m4/observations.py): 计算 M4 Raw Observations（收盘涨停计数、横截面收益排名、`qualification_status = NOT_CONFIGURED`）。

### (4) 生产管道与查询服务 (`src/qrp_atlas/pipeline/theme/`)
- [`service.py`](file:///e:/projects/qrp-atlas/src/qrp_atlas/pipeline/theme/service.py): 批量历史重放 `rebuild_m4_facts` 与单日增量 `run_m4_daily`，集合化批处理，单事务原子覆盖；
- [`query.py`](file:///e:/projects/qrp-atlas/src/qrp_atlas/pipeline/theme/query.py): 提供指数历史、最新状态、波段 Episode 与 M4 观测查询，以及深度可解释性审计 `audit_m4_observation`。

---

## 3. 验证结果

### (1) Task 04-A 专项测试 (37 项测试 100% PASSED)
- `tests/contracts/test_schema_contracts.py` (24 项)
- `tests/stock_collections/test_identity.py` (4 项)
- `tests/stock_collections/test_pit_membership.py` (2 项)
- `tests/stock_collections/test_resolver.py` (2 项)
- `tests/indicators/test_theme_effective_members.py` (1 项)
- `tests/indicators/test_theme_custom_index.py` (1 项)
- `tests/indicators/test_theme_trend_and_episode.py` (1 项)
- `tests/indicators/test_m4_observations.py` (1 项)
- `tests/pipeline/theme/test_theme_production.py` (1 项)

```text
============================= 37 passed in 0.88s ==============================
```

### (2) 全量回归测试 (1233 项测试 100% PASSED)
```text
================= 1233 passed, 3 skipped in 173.88s (0:02:53) =================
```

### (3) 代码语法与格式检查
- `python -m compileall -q src tests`：通过（Exit code 0）；
- `git diff --check`：通过（无尾部空白或空行问题）。

---

## 4. 生产同步与运维交接事项

根据 `RULE[user_global]` 及 `AGENTS.md`，本次变更涉及后端及数据库，同步到 Linux 生产节点时需注意：
1. **代码与配置同步**：
   * 同步 `src/qrp_atlas/contracts/`, `src/qrp_atlas/stock_collections/`, `src/qrp_atlas/indicators/theme/`, `src/qrp_atlas/indicators/m4/`, `src/qrp_atlas/pipeline/theme/`；
2. **数据库 DDL 迁移**：
   * 需在 Linux 生产 DuckDB 执行 `deploy/duckdb/003_stock_collections_and_m4.sql` 创建 7 张新表；
3. **服务与任务**：
   * 可通过 `ThemePipelineService.rebuild_m4_facts()` 执行全历史题材与 M4 事实重放；
   * 每日生产可通过 `ThemePipelineService.run_m4_daily(trade_date)` 增量更新。
