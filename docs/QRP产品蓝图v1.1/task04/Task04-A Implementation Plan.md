# Implementation Plan: Task 04-A｜M4 完整事实能力

建设从 **THEME StockCollection**、**PIT Theme Membership**，到**题材等权指数**、**趋势状态 / Episode**、**M4 Raw Observations**、**历史回放与生产查询**的完整事实闭环。

---

## 1. 架构定位与核心原则

严格遵守：
- `docs/QRP产品蓝图v1.1/task04/README.md`
- `docs/QRP产品蓝图v1.1/task04/StockCollection 设计书 Final.md`
- `docs/QRP产品蓝图v1.1/02_架构与跨仓边界.md`
- `src/qrp_atlas/AGENTS.md`
- `src/qrp_atlas/indicators/AGENTS.md`

### 核心分层与数据流
```text
Theme (1:1)
  ↓
THEME StockCollection
  ↓
resolve_members(as_of_date, knowledge_date)
  ↓
PIT Theme Members
  ↓
M4 Effective Member Filter (listing_actual_days > 5, NOT suspended)
  ↓
JOIN daily market facts
  ↓
Equal-weight Theme Index
  ↓
Theme Index Trend State / Episode (MA5 / MA10 / BASE / CANDIDATE / ACTIVE)
  ↓
M4 Raw Observations (theme_daily_return, theme_limit_up_count, theme_return_rank)
  ↓
Production Pipeline & Internal Query / Audit Service
```

### 严格的 Invariants
1. **INV-01**: `StockCollection` 仅为统一身份与 PIT 解析层，非万能成员大表（Not Universal Membership Storage）。
2. **INV-02**: 稳定身份由 `collection_type + namespace + source_key` 决定（如 `COLL:THEME:QRP:AI_COMPUTE`），Theme 改名不改变 `collection_id`。
3. **INV-03**: `Theme Membership ≠ M4 Calculation Eligibility`。上市实际交易日数 $\le 5$ 及停牌股票合法属于 Theme（不调出），仅当日不参与 M4 指数与涨停聚合；第 6 交易日及复牌后自动恢复。
4. **INV-04**: 双时间模型（Effective Time `[effective_from, effective_to)` vs Knowledge Time `available_trade_date`）。调出/修正采用 append-only late revision，禁止 hard delete，严禁未来信息泄漏。
5. **INV-05**: 纯事实不冒充决策：未配置阈值时 `qualification_status = NOT_CONFIGURED`，不输出综合评分或伪 `m4_qualified=false`。
6. **INV-06**: 集合化与防 N+1：生产与历史重放均使用 DuckDB 集合化 SQL / 批处理 DataFrame 计算。

---

## 2. 详细变更模块设计

### 模块 A：契约与 Schema 层 (`src/qrp_atlas/contracts/`)

#### 1. `src/qrp_atlas/contracts/stock_collection.py` [NEW]
* 定义枚举：
  * `CollectionType.THEME`（v1.1 第一版仅生产支持 THEME）；
  * `CollectionScope.CANONICAL`, `CollectionScope.USER`, `CollectionScope.RESEARCH`；
  * `MembershipModel.INTERVAL`, `MembershipModel.DAILY_OBSERVATION`；
  * `CollectionStatus.ACTIVE`, `CollectionStatus.INACTIVE`。
* 字段常量：
  * `COLLECTION_ID`, `COLLECTION_TYPE`, `COLLECTION_SCOPE`, `NAMESPACE`, `SOURCE_KEY`, `CANONICAL_NAME`, `MEMBERSHIP_MODEL`, `STATUS`, `EFFECTIVE_FROM`, `EFFECTIVE_TO`, `AVAILABLE_TRADE_DATE`, `REVISION_ID`, `INGESTED_AT`, `SOURCE`, `SOURCE_RECORD_ID`, `MEMBERSHIP_ID`, `THEME_ID` 等。
* 表名常量：
  * `STOCK_COLLECTION_TABLE = "stock_collection"`
  * `THEME_TABLE = "theme"`
  * `THEME_MEMBERSHIP_HISTORY_TABLE = "theme_membership_history"`

#### 2. `src/qrp_atlas/contracts/m4.py` [NEW]
* 字段与表名常量：
  * `THEME_CUSTOM_INDEX_DAILY_TABLE = "theme_custom_index_daily"`
  * `THEME_CUSTOM_INDEX_STATE_TABLE = "theme_custom_index_state"`
  * `THEME_CUSTOM_INDEX_EPISODE_TABLE = "theme_custom_index_episode"`
  * `THEME_M4_OBSERVATION_TABLE = "theme_m4_observation"`
  * 字段：`THEME_DAILY_RETURN`, `THEME_LIMIT_UP_COUNT`, `THEME_RETURN_RANK`, `EFFECTIVE_MEMBER_COUNT`, `TOTAL_MEMBER_COUNT`, `COMPARISON_UNIVERSE_SIZE`, `COMPARISON_UNIVERSE_VERSION`, `QUALIFICATION_STATUS`, `INDEX_LEVEL`, `EXCLUSION_REASON` 等。
  * 版本常量：`COMPARISON_UNIVERSE_VERSION_V1 = "m4_board_universe_v1"`，`QUALIFICATION_STATUS_NOT_CONFIGURED = "NOT_CONFIGURED"`，`M4_CALCULATION_VERSION = "m4_observations@1.0.0"`。

#### 3. `src/qrp_atlas/contracts/schema.py` & `src/qrp_atlas/contracts/__init__.py` [MODIFY]
* 注册 7 张新 TableSchema 并加入 `ALL_TABLES`：
  1. `STOCK_COLLECTION` (PK: `(collection_id, revision_id)`)
  2. `THEME` (PK: `(theme_id, revision_id)`)
  3. `THEME_MEMBERSHIP_HISTORY` (PK: `(membership_id, revision_id)`)
  4. `THEME_CUSTOM_INDEX_DAILY` (PK: `(theme_id, trade_date)`)
  5. `THEME_CUSTOM_INDEX_STATE` (PK: `(theme_id, trade_date)`)
  6. `THEME_CUSTOM_INDEX_EPISODE` (PK: `(episode_id,)`)
  7. `THEME_M4_OBSERVATION` (PK: `(theme_id, trade_date)`)

#### 4. `deploy/duckdb/003_stock_collections_and_m4.sql` [NEW]
* 提供标准 DDL 建表脚本。

---

## 3. StockCollection 顶级领域 (`src/qrp_atlas/stock_collections/`)

#### 1. `src/qrp_atlas/stock_collections/identity.py` [NEW]
* `make_collection_id(collection_type: str, namespace: str, source_key: str) -> str`:
  * 格式：`COLL:{collection_type}:{namespace}:{source_key}`（大写规范化，去除首尾空白）；
  * 校验命名空间和 source_key 合法性，防止注入与碰撞。

#### 2. `src/qrp_atlas/stock_collections/models.py` [NEW]
* 领域数据结构：
  * `StockCollectionRecord`, `ThemeRecord`, `ThemeMembershipRecord`；
  * `ResolvedMember` (包含 `collection_id`, `asset_id`, `as_of_date`, `weight=None`, `source_table`, `source_record_id`, `source_revision_id`)；
  * `MembershipExplanation` (包含生命周期、revision、生效区间、认知日期、来源信息)；
  * `StockCollectionQueryContext` (包含 `as_of_date`, `knowledge_date`, `version_context`, `allowed_scopes`)。

#### 3. `src/qrp_atlas/stock_collections/repository.py` [NEW]
* DuckDB 持久化仓储：
  * 负责 `stock_collection`, `theme`, `theme_membership_history` 的原子事务写入与查询；
  * `create_theme_collection_atomic(theme_data, collection_data)`：1:1 原子创建；
  * `append_membership_revisions(records)`：追加 revision 记录；
  * `get_pit_revisions(...)`：按 PIT 规则读取数据。

#### 4. `src/qrp_atlas/stock_collections/adapters/theme.py` [NEW]
* `ThemeAdapter`：
  * 接收 `StockCollectionQueryContext`；
  * SQL/DataFrame 过滤：
    1. `available_trade_date <= knowledge_date`；
    2. 针对每个 `membership_id` 取最大 `available_trade_date`（即截至 `knowledge_date` 最新的 revision）；
    3. 判定 `effective_from <= as_of_date AND (effective_to IS NULL OR as_of_date < effective_to)`；
  * 映射为 `ResolvedMember` 列表。

#### 5. `src/qrp_atlas/stock_collections/resolver.py` [NEW]
* `StockCollectionResolver`：
  * `resolve_collection(collection_id, context)`
  * `resolve_members(collection_id, as_of_date, knowledge_date, version_context)`
  * `resolve_asset_collections(asset_id, context, collection_types=None)`
  * `explain_membership(collection_id, asset_id, context)`

#### 6. `src/qrp_atlas/stock_collections/service.py` [NEW]
* 领域应用服务：
  * `create_canonical_theme(...)`
  * `add_member(...)`
  * `remove_member(...)`
  * `revise_member_late(...)`
  * `reenter_member(...)`

---

## 4. 指标与事实计算层 (`src/qrp_atlas/indicators/theme/` & `src/qrp_atlas/indicators/m4/`)

纯计算函数，无 DB 依赖：

#### 1. `src/qrp_atlas/indicators/theme/effective_members.py` [NEW]
* `calculate_m4_effective_members(memberships_df, listing_facts_df, suspension_facts_df)`:
  * 规则：`is_m4_effective_member = is_theme_member & (listing_actual_trading_days > 5) & (~is_suspended)`；
  * 标注 `exclusion_reason`: `"NEW_LISTING_LE_5"` / `"SUSPENDED"` / `None`；
  * **验证：原 membership 记录保持只读，不修改其属性**。

#### 2. `src/qrp_atlas/indicators/theme/custom_index.py` [NEW]
* `calculate_theme_equal_weight_index(effective_members_df, market_snapshot_df, base_level=1000.0)`:
  * 聚合每个 Theme 每日有效成员的算术平均收益率：`theme_daily_return = mean(member_returns)`；
  * 若有效成员数为 0，则 `theme_daily_return = np.nan`（缺失处理）；
  * 连续复利指数序列：`index_level_t = index_level_{t-1} * (1 + theme_daily_return_t)`。

#### 3. `src/qrp_atlas/indicators/theme/trend_and_episode.py` [NEW]
* `calculate_theme_index_trend_and_episodes(index_df)`:
  * 复用通用价格序列均线与状态机逻辑（MA5 / MA10 / BASE / CANDIDATE / ACTIVE）；
  * 计算行情波段轮次（Episode）、`ma5_reentry_count` 与 MA10 终结判定；
  * **明确剔除新股 warmup 和股票专属生命周期逻辑**。

#### 4. `src/qrp_atlas/indicators/m4/observations.py` [NEW]
* `calculate_m4_raw_observations(theme_index_df, effective_members_df, market_snapshot_df, comparison_boards_df)`:
  * 1. `theme_daily_return`：取自自定义等权指数；
  * 2. `theme_limit_up_count`：统计当日有效成员中收盘处于涨停状态（`is_limit_up == True`）的数量（仅统计收盘最终涨停）；
  * 3. `theme_return_rank`：在 comparison universe（`m4_board_universe_v1`，包含同花顺行业/概念板块及 QRP 自定义题材指数）中的横截面收益率降序排名（1-based）；
  * 4. 固定输出 `qualification_status = "NOT_CONFIGURED"`。

---

## 5. 生产管道、历史回放与查询服务 (`src/qrp_atlas/pipeline/theme/`)

#### 1. `src/qrp_atlas/pipeline/theme/service.py` [NEW]
* 生产与历史重放：
  * `rebuild_m4_facts(db_con, start_date, end_date, ...)` / `run_m4_daily(db_con, trade_date, ...)`；
  * 集合化 SQL 批量读取并聚合；
  * 事务保障：单 DuckDB 事务原子删除并插入；
  * 幂等性：重复运行输出 byte-for-byte 一致。

#### 2. `src/qrp_atlas/pipeline/theme/query.py` [NEW]
* 内部查询与 Audit 服务：
  * `get_theme_collection(collection_id)`
  * `get_pit_theme_members(theme_id, as_of_date, knowledge_date)`
  * `get_theme_index_history(theme_id, start_date, end_date)`
  * `get_m4_observations(trade_date, theme_id=None)`
  * `audit_m4_observation(theme_id, trade_date)`：追溯有效成员清单、排除原因、各成员收益率、涨停股票清单、排名基准 universe。

---

## 6. 验证计划

### 自动化单元与集成测试套件 (`tests/`)

1. **StockCollection 身份与 Registry 测试** (`tests/stock_collections/test_identity.py`):
   - 稳定 ID 生成与确定性验证；
   - Theme 改名保持 `collection_id` 不变；
   - 身份碰撞防护（Fail-closed）；
   - THEME-only 范围限制验证。
2. **PIT Theme Membership 生命周期与无未来泄漏测试** (`tests/stock_collections/test_pit_membership.py`):
   - Add → Remove → Late Revision → Re-entry 完整生命周期；
   - 对照不同 `knowledge_date` 与 `as_of_date` 组合，断言未来 revision 绝不泄漏。
3. **M4 计算资格与有效成员过滤测试** (`tests/indicators/test_theme_effective_members.py`):
   - 上市实际交易日 $\le 5$ 排除，第 6 交易日自动进入；
   - 停牌排除，复牌自动恢复；
   - 断言计算资格过滤绝不修改原 Theme Membership。
4. **Theme 等权指数计算测试** (`tests/indicators/test_theme_custom_index.py`):
   - 单成员、多成员等权平均计算；
   - 0 有效成员输出 NULL（非 0）；
   - 连续复利指数序列正确性。
5. **Theme 指数趋势与 Episode 测试** (`tests/indicators/test_theme_trend_and_episode.py`):
   - MA5/MA10 计算、BASE/CANDIDATE/ACTIVE 转换；
   - Episode 起止与 `ma5_reentry_count` 重入计数；
   - 确认无新股 warmup 污染。
6. **M4 原始事实与排名测试** (`tests/indicators/test_m4_observations.py`):
   - 收益率、收盘涨停计数（排除盘中触板）；
   - `m4_board_universe_v1` 横截面排名计算；
   - `qualification_status == "NOT_CONFIGURED"` 强校验。
7. **生产管道、历史回放与事务审计测试** (`tests/pipeline/theme/test_theme_production.py`):
   - 全历史重放与单日增量运行结果严格一致性；
   - 幂等性与事务回滚；
   - 集合化执行验证（无 N+1 查询）。
8. **契约全表注册防遗漏测试** (`tests/contracts/test_schema_contracts.py`):
   - 确保所有新增 TableSchema 均 100% 注册至 `ALL_TABLES`。

### 全量回归与格式检查
```bash
pytest tests/ -v
python -m compileall -q src tests
git diff --check
```
