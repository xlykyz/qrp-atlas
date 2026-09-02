# StockCollection 设计书（最终版）

> 项目：QRP Atlas v1.1  
> 文档状态：Final  
> 适用范围：Task 04 第一子包前置基础能力  
> 设计目标：为 Theme / Industry / Index / System Pool 等“股票集合”提供统一身份与统一成员解析契约，并作为 M4 完整事实能力的基础设施。  
> 当前实现边界：v1.1 仅支持股票成员（EQUITY members），不扩展为通用 Asset Domain。

---

## 1. 定义

### 1.1 StockCollection 是什么

**StockCollection 是 QRP 对“股票集合”的标准化领域抽象。**

一个 StockCollection 必须具备：

1. 稳定的逻辑身份；
2. 明确的集合类型与来源；
3. 可在指定历史时点解析其股票成员；
4. 明确的 PIT（Point-in-Time）语义；
5. 可追溯的来源、版本与血缘。

概念上：

```text
StockCollection
=
Unified Identity
+
Unified Membership Resolution Contract
```

它不是：

```text
Universal Membership Storage
```

StockCollection 统一的是：

- “这是什么集合”；
- “在某个 PIT 时点，它有哪些成员”；
- “成员结果来自哪里”；
- “如何解释这条成员关系”。

StockCollection **不要求把所有成员事实复制到同一张表中**。

---

## 2. 与现有数据能力的关系

StockCollection 与原始采集、既有派生股票池、行业成员、指数成分、Theme 等都不冲突。

已有事实继续保留原生业务语义和物理存储：

```text
industry_membership_history
index_component_history
system_b_pool_membership
theme_membership_history
user_collection_membership
```

它们通过 Adapter 接入 StockCollection Resolver。

因此：

```text
原生事实
   ↓
Adapter
   ↓
StockCollection Resolver
   ↓
统一成员输出
```

“纳入 StockCollection”不等于复制数据，而是：

> 获得统一 Collection Identity，并能通过统一 Resolver 被访问。

---

## 3. 核心判断标准

一个对象是否应成为 StockCollection，判断标准是：

> **它能否回答：在给定历史时点，这个对象包含哪些股票？**

例如：

| 对象 | 是否适合 StockCollection |
|---|---|
| 沪深300成分股 | 是 |
| 申万半导体行业 | 是 |
| System B 高度池 | 是 |
| System B 容量池 | 是 |
| AI 算力 Theme | 是 |
| 用户自定义“低空经济观察池” | 是 |
| 某研究临时选出的股票集合 | 是 |
| `daily_market_snapshot` | 否 |
| 个股 MA5 | 否 |
| 某个 Episode | 否 |
| M4 数值 | 否 |

---

## 4. Collection Type

v1.1 第一版支持：

```text
INDUSTRY
INDEX
THEME
SYSTEM_POOL
USER_DEFINED
RESEARCH
```

### 4.1 Collection Scope

```text
CANONICAL
USER
RESEARCH
```

含义：

- `CANONICAL`：QRP 正式领域事实；
- `USER`：用户私有集合；
- `RESEARCH`：研究性临时集合。

生产默认只允许读取 `CANONICAL`，除非调用方显式声明其他 scope。

---

## 5. Membership Model

不同 Collection 的成员事实天然具有不同时间模型，因此不得强制统一为一个万能成员表。

支持两类基础 Membership Model：

```text
INTERVAL
DAILY_OBSERVATION
```

### 5.1 INTERVAL

适用于：

- INDUSTRY
- INDEX
- THEME
- USER_DEFINED

典型语义：

```text
effective_from <= as_of_date
AND
(effective_to IS NULL OR as_of_date < effective_to)
```

### 5.2 DAILY_OBSERVATION

适用于：

- SYSTEM_POOL

例如 System B 高度池、容量池、辨识度池。

它们是某交易日派生出的集合观察，不应人为伪造成连续区间。

---

## 6. Stable Identity

Collection 的稳定身份由：

```text
collection_type
+
namespace
+
source_key
```

确定。

映射为稳定：

```text
collection_id
```

示例：

```text
COLL:INDUSTRY:SW:L2|801080
COLL:INDEX:CSI:000300.SH
COLL:SYSTEM_POOL:SYSTEM_B:HEIGHT
COLL:THEME:QRP:AI_COMPUTE
```

### 6.1 身份约束

以下内容不得成为稳定 ID 的组成部分：

- 显示名称；
- 当前规则版本；
- 当前计算版本；
- 当前成员数量；
- 当前状态标签。

因此：

```text
Theme 改名
≠
collection_id 改变
```

---

## 7. `stock_collection` Registry

推荐逻辑字段：

| 英文字段 | 中文名称 | 说明 |
|---|---|---|
| `collection_id` | 集合 ID | 稳定逻辑身份 |
| `collection_type` | 集合类型 | INDUSTRY / INDEX / THEME / SYSTEM_POOL / USER_DEFINED / RESEARCH |
| `collection_scope` | 集合作用域 | CANONICAL / USER / RESEARCH |
| `namespace` | 命名空间 | 如 SW / CSI / QRP / SYSTEM_B |
| `source_key` | 来源键 | 原始系统稳定业务标识 |
| `canonical_name` | 标准名称 | 当前标准展示名 |
| `membership_model` | 成员模型 | INTERVAL / DAILY_OBSERVATION |
| `status` | 集合状态 | ACTIVE / INACTIVE 等 |
| `effective_from` | 生效日期 | 当前 Registry revision 业务生效起点 |
| `effective_to` | 失效日期 | 当前 Registry revision 业务失效终点 |
| `available_trade_date` | 可用交易日 | 最早允许用于历史决策的交易日 |
| `source` | 数据来源 | 来源系统 |
| `source_record_id` | 来源记录 ID | 原始来源记录标识 |
| `revision_id` | 修订 ID | Registry revision 唯一标识 |
| `ingested_at` | 入库时间 | QRP 实际获知/写入时间 |

### 7.1 Registry PIT

Registry 本身也必须遵守 PIT。

不能使用“今天的名称/状态”覆盖历史查询。

---

## 8. 物理存储原则

### 8.1 不采用 Universal Membership Table

明确拒绝：

```text
stock_collection_membership
```

作为所有 Collection Type 的统一物理成员表。

原因：

- Theme 是 PIT interval relation；
- Index 可能带 source weight / rebalance semantics；
- Industry 带 classification system / level；
- System Pool 是 daily observation + rule_version + run_id；
- 强制统一会制造大量 NULL、伪时间字段和语义污染。

### 8.2 保留原生 Membership

推荐：

```text
INDUSTRY
→ industry_membership_history

INDEX
→ index_component_history

SYSTEM_POOL
→ system_b_pool_membership

THEME
→ theme_membership_history

USER_DEFINED
→ user_collection_membership
```

StockCollection Resolver 通过 Adapter 统一访问。

---

## 9. Theme 与 StockCollection

Theme 是领域对象；StockCollection 是股票集合抽象。

推荐关系：

```text
Theme
  1:1
   ↓
StockCollection
  1:N
   ↓
Theme Membership
```

Theme 负责：

- Theme Identity；
- Theme Definition；
- Theme Version；
- Theme Cycle；
- Theme Alias；
- 产业链关系；
- Evidence；
- Proposal / Review；
- Theme-specific observations。

StockCollection 只负责：

- 集合身份；
- 集合生命周期；
- 成员解析；
- PIT 查询；
- 通用来源解释。

---

## 10. `theme_membership_history`

所有 Theme 共享同一逻辑 Membership 表。

这不是“大杂烩”，因为每一行表达完全相同的业务关系：

> 某个股票，在某个 PIT 时间语义下，属于某个 Theme Collection。

### 10.1 推荐字段

| 英文字段 | 中文名称 | 说明 |
|---|---|---|
| `membership_id` | 成员关系 ID | 一段连续逻辑成员生命周期 |
| `theme_id` | 题材 ID | Theme 领域身份 |
| `collection_id` | 集合 ID | 对应 THEME StockCollection |
| `asset_id` | 股票资产 ID | 当前 v1.1 仅支持股票 |
| `effective_from` | 关系生效日 | 业务关系开始 |
| `effective_to` | 关系失效日 | 业务关系结束，左闭右开 |
| `available_trade_date` | 可用交易日 | QRP 从何时起可使用此认知 |
| `source` | 来源 | 财报/公告/IR/人工确认等 |
| `source_record_id` | 来源记录 ID | 原始来源记录 |
| `revision_id` | 修订 ID | 对当前逻辑 membership 的一次认知修订 |
| `ingested_at` | 入库时间 | 实际入库时间 |

---

## 11. Membership 历史语义

### 11.1 双时间模型

必须区分：

```text
Effective Time
业务上什么时候属于这个集合？

Knowledge Time
QRP 在什么时候知道这件事？
```

对应：

```text
effective_from
effective_to
available_trade_date
ingested_at
revision_id
```

### 11.2 有效区间

统一采用：

```text
[effective_from, effective_to)
```

例如：

```text
effective_from = 2026-08-01
effective_to   = 2026-08-20
```

表示：

- 8 月 1 日开始属于；
- 8 月 20 日开始不再属于。

---

## 12. Member 调入

新增一段 logical membership：

```text
membership_id = TM100
collection_id = COLL:THEME:QRP:ROBOT
asset_id = A

effective_from = 2026-08-10
effective_to = NULL
available_trade_date = 2026-08-10
revision_id = TM100_R1
```

从 2026-08-10 起，在满足 knowledge_date 的情况下 Resolver 可以读取该成员。

---

## 13. Member 调出

禁止 hard delete。

如果 2026-08-25 确认成员关系结束：

```text
TM100_R2

membership_id = TM100
effective_from = 2026-08-10
effective_to = 2026-08-25
available_trade_date = 2026-08-25
```

它是对同一 logical membership 的新 revision。

---

## 14. Late Revision

例如 2026-09-01 才知道：

> 该成员实际上从 2026-08-15 起就已不再属于 Theme。

新增 revision：

```text
TM100_R3

membership_id = TM100
effective_from = 2026-08-10
effective_to = 2026-08-15
available_trade_date = 2026-09-01
```

因此：

```text
as_of_date = 2026-08-20
knowledge_date = 2026-08-20
→ 当时认为仍属于

as_of_date = 2026-08-20
knowledge_date = 2026-09-02
→ 按后来修订认知，已不属于
```

这保证历史回放不使用未来知识。

---

## 15. 调出后重新调入

重新调入应创建新的：

```text
membership_id
```

例如：

```text
TM100
2026-08-10 → 2026-08-25

TM237
2026-09-15 → NULL
```

定义：

```text
membership_id
= 一段连续逻辑成员生命周期

revision_id
= 对该生命周期认知的一次修订
```

---

## 16. Resolver API

核心接口：

```python
resolve_collection(
    collection_id,
    context,
)

resolve_members(
    collection_id,
    as_of_date,
    knowledge_date,
    version_context,
)

resolve_asset_collections(
    asset_id,
    context,
    collection_types=None,
)

explain_membership(
    collection_id,
    asset_id,
    context,
)
```

---

## 17. Query Context

推荐：

```python
CollectionVersionContext(
    rule_versions: Mapping[str, str],
    calculation_versions: Mapping[str, str] = ...
)

StockCollectionQueryContext(
    as_of_date,
    knowledge_date,
    version_context,
    allowed_scopes=(CANONICAL,),
)
```

禁止隐式 latest。

如果版本上下文在某 Collection 类型中属于必要输入，则缺失时 fail closed。

---

## 18. Resolver 标准输出

| 英文字段 | 中文名称 | 说明 |
|---|---|---|
| `collection_id` | 集合 ID | 被解析集合 |
| `collection_type` | 集合类型 | THEME / INDEX / INDUSTRY 等 |
| `asset_id` | 资产 ID | 当前 v1.1 为股票 |
| `as_of_date` | 事实日期 | 查询日期 |
| `weight` | 来源权重 | 仅来源原生存在时提供 |
| `source_table` | 来源表 | 原生 Membership 来源 |
| `source_record_id` | 来源记录 ID | 原生关系标识 |
| `source_revision_id` | 来源修订 ID | PIT revision |
| `source_rule_version` | 来源规则版本 | 派生集合使用 |

### 18.1 Weight 语义

```text
weight = NULL
```

不代表不参与等权计算。

Resolver 只返回来源原生 weight。

是否等权属于下游 Collection Index Calculator 的计算政策。

---

## 19. Theme Membership 与 M4 有效成员必须分离

这是正式 invariant：

> **M4 calculation eligibility does not mutate Theme membership.**

即：

> M4 计算资格变化不得改变 Theme 成员关系。

一个股票可以合法属于 Theme，但在某交易日不参与 M4 聚合。

### 19.1 M4 Effective Member

```text
is_m4_effective_member
=
is_theme_member
AND listing_actual_trading_days > 5
AND NOT is_suspended
```

### 19.2 明确解释

上市实际交易日数 `<= 5` 的新股：

- 可以正常被纳入 Theme；
- 可以存在合法 Theme Membership；
- 只是当天不参与：
  - 题材等权收益；
  - 题材涨跌幅聚合；
  - 涨停数量统计；
  - M4 相关市场指标。

从第 6 个实际交易日起，自动具备参与资格。

观察日停牌股票同理：

- 不调出 Theme；
- 只排除该交易日 M4 计算；
- 复牌后重新参与。

---

## 20. M4 成员资格衍生字段

| 英文字段 | 中文名称 | 说明 |
|---|---|---|
| `trade_date` | 交易日 | 当前观察日 |
| `theme_id` | 题材 ID | 当前 Theme |
| `collection_id` | 集合 ID | 对应 StockCollection |
| `asset_id` | 股票资产 ID | 被检查成员 |
| `is_theme_member` | 是否为 Theme 成员 | PIT 关系是否有效 |
| `listing_actual_trading_days` | 上市实际交易日数 | 截至观察日 |
| `is_suspended` | 当日是否停牌 | 当日状态 |
| `is_m4_effective_member` | 是否为 M4 有效成员 | 是否进入当日 M4 聚合 |
| `exclusion_reason` | 排除原因 | `NEW_LISTING_LE_5` / `SUSPENDED` 等 |

---

## 21. StockCollection 与 M4 的数据消费链

概念流程：

```text
Theme
  ↓
THEME StockCollection
  ↓
resolve_members(T, K)
  ↓
PIT Theme Members
  ↓
JOIN market / suspension / listing facts
  ↓
M4 Effective Member Filter
  ↓
Equal-weight Theme Index
  ↓
Theme Index State / Episode
  ↓
M4 Observation
```

重要：

不得使用 N+1 查询。

禁止：

```text
先得到 30 个 ticker
→ Python 循环 30 次查市场数据
```

推荐：

```text
PIT members
JOIN daily_market_snapshot
JOIN suspend facts
JOIN listing facts
GROUP BY collection_id, trade_date
```

使用 DuckDB 集合化 SQL / DataFrame pipeline。

---

## 22. Logical Table 与 Physical Storage

“所有 Theme 共用一张 `theme_membership_history`”的准确含义是：

> 所有 Theme 共享统一逻辑 Schema 和关系模型。

并不要求未来所有物理数据永远存在单一文件块中。

当数据规模扩大后，可以采用：

- Parquet partition；
- collection hash 分桶；
- 日期分区；
- materialized snapshot；
- cache；
- 冷热分层。

对上层仍保持同一个逻辑关系模型。

---

## 23. 为什么不每个 Theme 一张表

拒绝：

```text
theme_ai_compute_membership
theme_robot_membership
theme_low_altitude_membership
...
```

原因：

### 23.1 业务实例不应成为 Schema

新增 Theme 应当是：

```text
INSERT theme
INSERT stock_collection
```

而不是：

```text
CREATE TABLE
```

### 23.2 Schema Migration

如果新增：

```text
available_trade_date
```

共享逻辑表只迁移一次。

每 Theme 一表则需要迁移大量表。

### 23.3 Reverse Lookup

查询：

> 某股票当前属于哪些 Theme？

共享表可以直接：

```sql
SELECT collection_id
FROM theme_membership_history
WHERE asset_id = ?
```

每 Theme 一表需要扫描动态表集合。

### 23.4 Cross-Theme Calculation

M4 本质上是：

```text
所有 Theme
→ 当日收益
→ 横截面比较
→ rank
```

共享 Theme Membership 模型天然适合集合化批量计算。

---

## 24. 性能与治理权衡

不能简单理解为：

```text
每 Theme 一表 = 高性能
共享表 = 低性能但好治理
```

更准确是：

> 每 Theme 一表用更高 Schema 和治理复杂度换取显式物理隔离；共享逻辑表用统一数据模型换取治理稳定性和跨 Theme 集合计算能力。

性能由：

- 数据布局；
- predicate pushdown；
- zone map；
- partition pruning；
- materialization；
- query plan；

共同决定。

对 QRP M4 的“每日批量计算所有 Theme”场景，共享逻辑模型更符合消费模式。

---

## 25. 推荐数据库边界

如果 `stock_collections` 被正式批准为顶级领域，推荐独立领域库：

```text
stock_collections.duckdb
```

主要 owner：

```text
stock_collection
theme
theme_membership_history
user_collection_membership   # 后续
```

已有事实不迁移：

```text
quant.db
  industry_membership_history
  index_component_history

system_b_pools.duckdb
  system_b_pool_membership
```

Resolver 跨原生 owner 读取。

### 25.1 重要原则

独立 DB 是领域 ownership 选择，不是 Collection 实例隔离方式。

禁止：

```text
每 Collection 一个 DB
每 Theme 一个 DB
```

---

## 26. Module Boundary

v1.1 推荐新增：

```text
src/qrp_atlas/
    stock_collections/
        models.py
        identity.py
        registry.py
        lifecycle.py
        resolver.py
        repository.py
        service.py
        adapters/
            industry.py
            index.py
            system_b_pool.py
            theme.py
```

职责：

- stable identity；
- registry；
- collection lifecycle；
- membership resolution；
- reverse lookup；
- explainability；
- adapter contract。

不得吸收：

- Theme Evidence；
- Theme Cycle；
- Theme Agent；
- M4 calculation；
- M5/M6；
- Strategy authorization；
- API transport；
- external ingestion；
- Collection Index calculation。

---

## 27. Architecture Amendment

当前 v1.1 架构曾规定：

```text
execution 是唯一允许新增的顶级业务模块
```

正式实现 `stock_collections/` 前必须通过 ADR / 架构文档修订解除该限制。

原因：

StockCollection 已具备独立顶级领域的必要条件：

- 稳定领域身份；
- 自己的 lifecycle；
- 自己的 write model；
- 稳定领域操作；
- 多种消费者；
- 不从属于 Theme；
- 后续可扩展 USER_DEFINED。

---

## 28. v1.1 Explicit Non-Goals

当前不实现：

- 通用 Asset Domain；
- Asset Registry；
- asset_type；
- futures / ETF / CB / option / synthetic membership；
- mixed-asset collection；
- 每 Theme 独立表；
- Universal Membership Table；
- 自动 Theme Discovery；
- Theme Agent；
- M5；
- M6；
- Unified Score；
- M1/M2/M3 最终身份；
- 主线判断；
- A/B/C 市场阶段；
- 开仓授权。

虽然字段使用：

```text
asset_id
```

但 v1.1 resolver capability 明确只支持股票成员。

---

## 29. Error Semantics

建议稳定错误码：

```text
COLLECTION_NOT_FOUND
COLLECTION_NOT_AVAILABLE_AS_OF
COLLECTION_SCOPE_NOT_ALLOWED
COLLECTION_VERSION_REQUIRED
COLLECTION_VERSION_UNSUPPORTED
COLLECTION_ADAPTER_NOT_FOUND
COLLECTION_IDENTITY_COLLISION
COLLECTION_PIT_INVARIANT_VIOLATION
COLLECTION_SOURCE_INCONSISTENT
```

### 29.1 Empty 与 Not Found

必须区分：

```text
Collection 存在但 0 个成员
→ EMPTY

Collection 不存在
→ COLLECTION_NOT_FOUND

Collection 在当前 knowledge/as_of 下尚不可见
→ COLLECTION_NOT_AVAILABLE_AS_OF
```

---

## 30. Transaction Semantics

Registry mutation 必须事务化。

Identity collision、PIT invariant failure、source inconsistency 时：

```text
ROLLBACK
```

Theme Membership revision：

- append-only；
- 禁止 hard delete；
- 禁止静默覆盖历史 revision。

---

## 31. Migration Strategy

第一阶段迁移：

```text
1. contracts
2. stock_collection registry
3. schema registration
4. existing collection identity backfill
5. resolver
6. adapters
7. native equivalence audit
8. PIT tests
9. Theme adapter
```

现有：

```text
industry_membership_history
index_component_history
system_b_pool_membership
```

零改写、零复制。

---

## 32. Backfill

### 32.1 Industry / Index

从现有 canonical facts 建立 Registry identity。

不复制 membership。

### 32.2 System B Pool

从稳定 Pool Definition / Enum seed Registry。

即使某个 Pool 当天无成员：

```text
Collection 仍然存在
Members = EMPTY
```

不得因为当天无成员就认为 Collection 不存在。

---

## 33. Tests

### 33.1 Identity

- deterministic collection_id；
- rename 不改变 ID；
- collision fail closed；
- source_key 唯一性。

### 33.2 PIT

- effective time；
- knowledge time；
- late revision；
- historical replay；
- future knowledge 不可见。

### 33.3 Membership Lifecycle

- add member；
- remove member；
- remove 后 re-add 创建新 membership_id；
- revision history append-only。

### 33.4 Adapter

- Industry 原生结果 == Resolver；
- Index 原生结果 == Resolver；
- System Pool 原生结果 == Resolver；
- source provenance 完整。

### 33.5 Weight

- Index source weight 保留；
- Theme weight NULL；
- Resolver 不自动等权。

### 33.6 Scope

- CANONICAL 默认允许；
- USER / RESEARCH 未授权时 fail closed。

### 33.7 Empty

- existing collection + zero members；
- collection not found；
- collection not available as-of；
- 三者严格区分。

### 33.8 M4 Membership Eligibility

必须测试：

```text
Theme member + listing_actual_trading_days <= 5
→ is_theme_member = true
→ is_m4_effective_member = false

Theme member + suspended
→ is_theme_member = true
→ is_m4_effective_member = false

第 6 个实际交易日
→ 自动参与 M4

复牌日
→ 自动恢复参与 M4
```

并验证：

> M4 eligibility filtering never writes or mutates Theme Membership.

---

## 34. Design Invariants

最终冻结：

### INV-01

```text
StockCollection ≠ Universal Membership Storage
```

### INV-02

```text
Collection Identity ≠ Display Name
```

### INV-03

```text
Collection Identity ≠ Rule Version
```

### INV-04

```text
Membership Resolver ≠ Index Calculator
```

### INV-05

```text
weight = NULL
≠
not eligible for equal-weight index
```

### INV-06

```text
M4 Calculation Eligibility
≠
Theme Membership
```

### INV-07

```text
listing_actual_trading_days <= 5
→ exclude from M4 calculation only
→ never auto-remove from Theme
```

### INV-08

```text
Suspension
→ exclude from that day's M4 calculation only
→ never auto-remove from Theme
```

### INV-09

```text
No implicit latest
```

### INV-10

```text
Historical membership
must be reconstructable from
as_of_date + knowledge_date + version_context
```

---

## 35. Final Architecture

```text
                          StockCollection Registry
                                   │
                   ┌───────────────┼────────────────┐
                   │               │                │
                   ↓               ↓                ↓
             Industry Adapter   Index Adapter   System Pool Adapter
                   │               │                │
                   ↓               ↓                ↓
       industry_membership   index_component   system_b_pool
              _history          _history        _membership

                                   │
                                   │
                              Theme Adapter
                                   │
                                   ↓
                        theme_membership_history
                                   │
                                   ↓
                         StockCollection Resolver
                                   │
                                   ↓
                  normalized PIT membership relation
                                   │
                                   ↓
                         downstream indicators
```

M4：

```text
THEME StockCollection
        ↓
resolve_members(as_of_date, knowledge_date)
        ↓
PIT Theme Members
        ↓
M4 eligibility filter
  - listing actual trading days <= 5 : excluded from calculation
  - suspended : excluded from calculation
        ↓
JOIN market facts
        ↓
Equal-weight Theme Index
        ↓
Theme Index State / Episode
        ↓
M4 Raw Observations
```

---

## 36. Final Decision

QRP v1.1 正式采用：

> **StockCollection = 股票集合的统一身份层 + 统一 PIT 成员解析层。**

采用：

- stable `collection_id`；
- per-domain native membership storage；
- Adapter-based Resolver；
- Theme shared logical membership table；
- PIT revision history；
- normalized read contract；
- no implicit latest；
- no Universal Membership Table；
- no per-Theme table；
- no per-Collection database。

Theme Membership 与 M4 计算资格严格分离。

该设计作为后续：

```text
StockCollection
→ Minimal Theme Canonical
→ PIT Theme Membership
→ Equal-weight Theme Index
→ M4 Complete Facts
```

的正式基础。
