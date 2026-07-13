# 任务 03-C：Point-in-time 查询服务

> **状态：✅ 已完成**（2026-07-14）

## 1. 目标

在已有 03-A 版本选择器与 03-B 六张 PIT 表基础上，提供只读历史查询接口：

```text
as_of_date
→ 仅使用当时可用数据
→ 解析财务修订、行业归属、指数快照
→ 返回稳定可复现 DataFrame
```

本任务完成后，**任务 03 标记完成**。  
`corporate_event` / 结构化事件属于任务 05，不在本任务范围。

## 2. 公开接口

模块：`qrp_atlas.backtest.pit_queries`  
并通过 `qrp_atlas.backtest` 再导出。

```python
query_financial_as_of(...)
query_industry_as_of(...)
query_index_components_as_of(...)
summarize_index_components(...)
IndustryMembershipConflictError
```

统一约定：

- 支持 `db_path` / 既有连接；
- DuckDB 默认只读；
- 参数化 SQL；
- 财务表名白名单；
- 返回 `pandas.DataFrame`；
- 不修改数据库与调用者输入；
- 空结果返回空 DataFrame。

## 3. 财务查询

表白名单：

```text
income_statement
balance_sheet
cashflow_statement
financial_indicator
```

可用性：

```text
available_trade_date <= as_of_date
```

版本选择直接复用：

```python
select_latest_available_records(...)
```

业务分组键：

```text
财务三表：
ticker + report_period + report_type + comp_type + end_type

financial_indicator：
ticker + report_period
```

`announcement_date` / `available_trade_date` / `update_flag` / `revision_id` 只作为版本与审计字段。

结果保留各表实际存在的审计字段（不伪造缺失列）。

## 4. 行业归属查询

表：`industry_membership_history`

区间语义（半开）：

```text
effective_from <= as_of_date
AND (effective_to IS NULL OR as_of_date < effective_to)
AND available_trade_date <= as_of_date
```

版本解析先按：

```text
asset_id + classification_system + industry_level + industry_code
```

再要求：

```text
asset_id + classification_system + industry_level
```

在同一 as_of 只能有一个有效行业；否则抛出 `IndustryMembershipConflictError`。

标准研究输出默认 `mask_future_effective_to=True`：  
若底层 `effective_to` 晚于 as_of，则在输出中置空，避免未来退出日期泄露。  
审计场景可传 `mask_future_effective_to=False`。

`include_full_path=True` 时返回稳定的 `l1/l2/l3_code/name`。

## 5. 指数成分查询

表：`index_component_history`（快照模型）

规则：

```text
index_code 固定
snapshot_date <= as_of_date
available_trade_date <= as_of_date
选择最新 snapshot_date
在该快照内按 (index_code, snapshot_date, asset_id) 解析最新版本
```

不使用 `effective_to` 推断成员区间。

`summarize_index_components` 提供成分数量与权重和审计。

## 6. 真实库验收（只读）

数据库：`data/db/quant.db`  
验收前后 SHA256 一致。

| 场景 | 结果 |
|---|---|
| `000001.SZ` income 在 `2024-03-15` | 空 |
| 同票 `2024-03-18` | 返回 2023 年报，available=2024-03-18 |
| 财务四表 `as_of=2024-12-31` | 均可查询 |
| `002028.SZ` 行业：进入前 / 有效期内 / 退出日 | 空 / 三级路径且 exit 屏蔽 / 空 |
| `000001.SZ` 申万三级路径 | 银行 / 股份制银行Ⅱ / 股份制银行Ⅲ |
| `000300.SH` 首快照前、首可用日、两快照之间 | 空 / 2024-01-02×300 / 仍用 2024-01-02 |
| 下一可用日 | 切到 2024-01-31×300 |

## 7. 测试

- 新增：`tests/backtest/test_pit_queries.py`
- 覆盖财务白名单、修订切换、业务分组、行业半开区间、未来退出屏蔽、冲突异常、指数快照边界、多指数隔离等

全量：

```bash
python -m pytest
python -m compileall -q src
git diff --check
```

## 8. 已知限制

1. 查询只覆盖 03-B 已入库的小范围真实样本，不负责全市场回填。
2. 行业冲突检测依赖当前库中的成员记录；若上游入库污染，会显式失败而非静默取一条。
3. 指数为月度权重快照，不提供日频精确调入调出区间。
4. 不提供事件主表 / `corporate_event`（任务 05）。
5. 不提供 HTTP API 或前端入口。
