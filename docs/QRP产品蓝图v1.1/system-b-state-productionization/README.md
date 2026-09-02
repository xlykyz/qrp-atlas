# System B 全市场事实派生状态生产化

> 基线：`develop/v1.1@d64a79cfcf90351b2026cf718a627c92940c8e67`
> 正式计算边界：Linux 本地 DuckDB

## 1. 生产架构

```text
Linux 正式市场事实
→ 一条集合化 SQL 生成观察日与历史统计事实
→ 按完整资产边界有界分批
→ calculate_system_b_2_0_states
→ Parquet staging + 完整性校验
→ 单事务删除目标范围旧物化结果并 INSERT INTO ... SELECT
→ API / CLI 消费
```

状态机保持纯计算，不访问数据库、配置、时钟、文件系统或生产运行管理。

> **System B 只有一张正式状态物化事实表；最新状态通过视图获取，不维护独立 checkpoint 表。**

## 2. 数据来源

| 事实 | 来源 | 语义 |
|---|---|---|
| PIT 股票域 | `stock_info.list_date/delist_date` | 上市前、退市后不生成观察 |
| 市场交易日 | `trading_calendar.is_open` | 只生成市场开市日 |
| 行情 | `daily_market_snapshot` | 有效非零成交量日线为实际交易 |
| 停牌 | `suspend_d` | 明确非交易，不进入实际交易序列 |
| 前复权 | `daily_market_snapshot.close` + `adj_factor_changes` | `FORWARD_ADJUSTED` |
| MA5 | 同一前复权实际交易日序列 | 同一可证明连续片段内完整 5 日窗口 |

市场事实分类：

```text
ACTUAL_TRADING
EXPLICIT_NON_TRADING
UNRESOLVED_MISSING
```

`UNRESOLVED_MISSING` 不伪装成停牌。它切断 MA5 可证明片段；缺口后第 1—4 个确认实际交易日 MA5 为 NULL，第 5 日首次恢复可信 MA5，且 CANDIDATE/ACTIVE 还要求前一实际交易日的 MA5 关系可信。

## 3. 状态事实表

`system_b_state_observation` 保存：

```text
asset_id, trade_date
lifecycle_state
trend_state NULLABLE
previous_trend_state NULLABLE
state_changed NULLABLE
market_fact_status
is_trading_day
listing_trading_day_number
confirmed_listing_trading_day_count
listing_trading_day_number_is_exact
close, ma5, ma5_window_complete, is_above_or_equal_ma5
latest_actual_trade_date
latest_actual_close, latest_actual_ma5
latest_actual_ma5_window_complete
latest_actual_is_above_or_equal_ma5
previous_actual_trade_date
previous_actual_is_above_or_equal_ma5
previous_actual_ma5_window_complete
state_basis_sequence_intact
actual_pair_contiguous
price_adjustment
rule_version_set_id, parameter_set_id
source_rule_ids, diagnostics
production_run_id, input_snapshot_id
calculation_version, created_at
```

`trend_state` 只允许非空值 `BASE/CANDIDATE/ACTIVE`；NULL 是事实不足造成的计算缺失，不是第四状态。逻辑唯一键为 `asset_id + trade_date + rule_version_set_id + parameter_set_id`，`input_snapshot_id` 只保留运行审计用途。`system_b_latest_state` 按资产、规则版本、参数版本选择最新当前物化结果。

## 4. 独立求值

标准 SQL 为每个观察日直接计算最近和前一实际交易事实及连续性证明。状态机不读取状态历史。

- 全历史：输出上市日至目标日全部观察；
- 区间：仍读取完整历史，只按 `start_date` 过滤输出；
- 单日：SQL 返回目标日及前一市场观察日，目标日状态由历史统计事实独立计算，前一行只用于审计比较；
- 每日定点：不加载状态 checkpoint，即使目标库没有任何 System B 状态也能计算任意目标日。

固定种子的随机多资产、多历史日期测试逐点比较单日与完整重放，状态、事实统计和诊断完全一致。

## 5. 初始化与每日运行

```bash
qrp-atlas-system-b initialize --end-date YYYY-MM-DD
qrp-atlas-system-b run-daily --trade-date YYYY-MM-DD
qrp-atlas-system-b readiness --trade-date YYYY-MM-DD
```

`initialize` 对指定范围独立重放并原子覆盖该范围；`start_date` 只是输出和覆盖边界，计算仍读取更早行情事实。`run-daily` 对任意指定市场交易日独立计算并原子新增或覆盖，即使目标库为空或已存在更晚日期结果也可运行。

readiness 分别报告实际交易、明确非交易和 unresolved 数量。System B 不判断行情事实是否正确，也不识别修订范围；unresolved 和不完整 MA5 按契约进入 NULL 计算，不被改写为停牌或有效 MA5。

## 6. 幂等、批量与原子性

- 一条历史或单日事实 SQL；
- 无每资产 SQL、无每日期 SQL；
- 无逐行 INSERT/UPDATE；
- 无每资产事务；
- 资产边界批次默认 100；
- Parquet staging 完整性校验后，在一个事务内删除目标范围旧结果并一次批量导入；
- 全市场覆盖会清除目标范围内已不在新资产集合中的旧行；资产子集运行只替换指定资产；
- 相同输入重复覆盖后的最终事实表完全相同；
- 删除或导入任一步失败均回滚，保留覆盖前完整结果。

## 7. API

```text
GET /api/v1/system-b/states/latest
GET /api/v1/system-b/states?trade_date=...
GET /api/v1/system-b/assets/{asset_id}/history
GET /api/v1/system-b/transitions?trade_date=...
GET /api/v1/system-b/summary?trade_date=...
GET /api/v1/system-b/production-runs/latest
```

API 原样返回 JSON `null`。迁移接口只统计两个非空业务状态之间的审计比较，不生成 `NULL → BASE` 等伪迁移。汇总独立提供 `null_state_count`、生命周期数量、unresolved 数量和诊断数量。

## 8. 版本治理

```text
rule_version_set_id = system_b_2_0_fact_derived_ma5_complete_1__user_20260726
parameter_set_id = system_b_2_0_fact_derived_ma5_complete_1_params_1
calculation_version = system_b_fact_derived_state@2.1.0
```

旧递推版本不静默覆盖。PR #44 未向正式库写入 System B 状态事实；新 schema 应在合并后按独立部署窗口创建。

## 9. 性能证据

正式库只读输入：`/home/claire/data/qrp-atlas/db/quant.db`；输出均位于 `/tmp` 临时 DuckDB 和 Parquet，未修改正式库。

### 每日 2026-07-24

```text
资产数: 5,515
事实输入行数: 11,030（目标日 + 前一观察日）
输出行数: 5,515
业务数据 SQL: 1
读取: 13.583 秒
计算: 0.143 秒
staging: 0.301 秒
原子覆盖导入: 0.057 秒
总耗时: 14.277 秒
峰值内存: 9,415 MB
状态: BASE 4,244 / CANDIDATE 165 / ACTIVE 1,106
```

### 完整历史 1990-12-19 至 2026-07-24

```text
资产数: 5,840
输入/计算/输出行数: 18,533,100 / 18,533,100 / 18,533,100
业务数据 SQL: 1
资产批次: 59
批量大小: 100
读取: 28.965 秒
计算: 205.375 秒
staging: 48.156 秒
单事务原子覆盖导入: 77.046 秒
总耗时: 564.369 秒（9 分 24.4 秒）
峰值内存: 14,697 MB
NULL 状态: 434,222
明确非交易观察: 667,499
UNRESOLVED_MISSING: 328,569
```

新模型完整初始化比旧递推实现的 649.148 秒基准更快。专项测试证明全历史、区间、单日、每日入口和重复覆盖在共同日期的状态、统计证明与诊断一致；多次复权只按相同比例缩放 close/MA5，不改变关系或状态。

## 10. 测试与边界

专项覆盖三个独立谓词、NULL、生命周期、停牌、零成交量、缺口分段 MA5 与恢复、审计三值逻辑、任意日期独立求值、随机定点对比、多次复权不变性、重复覆盖、资产缩集和事务回滚。

```text
System B 状态机/生产/API 专项: 33 passed
旧 detector、system_b_basic 与指标兼容集合: 35 passed
全量测试: 783 passed
python -m compileall -q src tests: passed
OpenAPI System B 路径: 6 passed
git diff --check: passed
```

本任务不实现行情轮次、`episode_status`、MA10、状态池、题材、评分、M1—M6、判断层、组合管理、execution 或 QMT，也不修改外部 `MyTradingSystem`。
