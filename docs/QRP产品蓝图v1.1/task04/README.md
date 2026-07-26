# System B 全市场状态监测实施说明

> 工作包：System B 2.0 基础状态机生产化接入
> 基线：`develop/v1.1@d64a79cfcf90351b2026cf718a627c92940c8e67`
> 正式计算边界：Linux 本地 DuckDB；Windows、Web、Agent 和手机端只读消费

## 1. 架构与边界

```text
Linux DuckDB 正式行情事实
→ 集合化标准输入 SQL
→ 按资产边界的有界内存批次
→ calculate_system_b_2_0_states
→ Parquet staging + 完整性校验
→ 单事务 INSERT INTO ... SELECT
→ 状态查询 API / CLI / 后续模块
```

`src/qrp_atlas/indicators/system_b/state_machine_v2.py` 仍是纯计算组件，本工作包没有向其中加入数据库、配置、系统时间、文件写入、运行管理或调度逻辑。

> **System B 状态历史只有一张正式事实表；最新状态通过视图获取，不维护独立 checkpoint 表。**

本任务新增的 `system_b_production_run` 是生产运行审计表，不保存 checkpoint，也不是第二张状态事实表。

## 2. 数据来源与 PIT 语义

| 标准输入 | 正式来源 | 口径 |
|---|---|---|
| 股票历史域 | `stock_info.list_date / delist_date` + `trading_calendar` | 每个市场交易日按当时上市/退市边界生成，禁止用当前 active 列表回填 |
| 市场交易日 | `trading_calendar.is_open` | 不生成周末和休市日 |
| 实际交易事实 | `daily_market_snapshot` + `suspend_d` | 明确区分 `ACTUAL_TRADING`、`EXPLICIT_NON_TRADING`、`UNRESOLVED_MISSING`；历史域从上市日开始，不能把缺失行情静默解释为停牌 |
| 上市实际交易日序号 | 集合化 SQL 对实际成交日 `row_number()` | 首个实际交易日为 1，停牌不推进，复牌继续 |
| 前复权收盘 | `daily_market_snapshot.close` + `adj_factor_changes` as-of 变更点 | `raw_close * 当日有效因子 / 目标快照最新因子`；首个变更点前及无公司行动资产使用基准因子 1.0，声明 `FORWARD_ADJUSTED` |
| MA5 | 同一前复权实际交易日序列 | 完整 5 个实际交易日窗口；停牌不进入窗口 |

市场事实分类口径：当日存在明确 `suspend_d`，或存在日线且 `volume=0` 时，才允许生成 `is_trading_day=false`；存在非空收盘且成交量大于 0 的日线为 `ACTUAL_TRADING`。历史股票域内、市场开市日没有日线且没有停牌事实，或日线关键字段无法解释时，标记为 `UNRESOLVED_MISSING`，以稳定错误码 `MISSING_DAILY_MARKET_FACT` 整批失败。

2026-07-26 的只读正式库审计位置为 `/home/claire/data/qrp-atlas/db/quant.db`。正式库包含：`stock_info` 5,840 行、`trading_calendar` 8,797 行、`daily_market_snapshot` 19,094,111 行、`adj_factor_changes` 88,598 行、`suspend_d` 604,326 行。代码不写死该路径，生产命令默认读取统一配置 `QRP_DUCKDB_PATH`；该绝对路径仅作为本次性能证据记录。

## 3. 状态表、视图与运行审计

### `system_b_state_observation`

唯一键：

```text
asset_id
+ trade_date
+ rule_version_set_id
+ parameter_set_id
+ input_snapshot_id
```

除状态机完整输出外，保存：

```text
production_run_id
input_snapshot_id
calculation_version
created_at
```

`source_rule_ids` 与 `diagnostics` 以稳定 JSON 文本保存。加入 `input_snapshot_id` 是为了保留上游修订后的新事实，禁止静默覆盖旧快照；相同快照重复运行 no-op。

### `system_b_latest_state`

按：

```text
asset_id + rule_version_set_id + parameter_set_id
```

只从 `SUCCEEDED` 生产运行中选择最新 `trade_date`、最新完成时间的状态。每日 checkpoint 构造按目标日前同版本成功事实批量读取，不为每只股票单独查询。

### `system_b_production_run`

记录初始化/每日运行状态、目标日期、规则和参数版本、输入快照、行数、错误码、阶段耗时、批次与峰值内存。失败运行不会产生部分状态事实。

## 4. 历史初始化

入口：

```bash
qrp-atlas-system-b initialize [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD]
qrp-atlas-system-b initialize --asset 000001.SZ --asset 600000.SH
qrp-atlas-system-b initialize --dry-run
qrp-atlas-system-b initialize --output-database /tmp/system-b.duckdb --keep-staging
```

即使指定 `--start-date`，计算输入仍从每只股票上市第 1 个实际交易日正序开始，只过滤最终持久化日期；不会对截断历史猜测初始状态。

`initialize` 是 bootstrap-only 入口：目标库中不存在同规则、同参数的状态事实时允许首次初始化；相同范围和 `input_snapshot_id` 的成功初始化返回 `IDEMPOTENT_NOOP`；只要目标库已有状态而本次范围或输入快照不同，就在批量导入事务内以 `SYSTEM_B_INITIALIZATION_TARGET_NOT_EMPTY` 拒绝。它不承担历史修订，避免向已有路径追加局部快照。

读取使用一条按 `asset_id, trade_date` 排序的集合化 SQL，通过 DuckDB record-vector 分块读取，并在观察到下一个资产后按完整资产边界切批。`--asset-batch-size` 默认 100，可按内存调整。同一资产不会在无 checkpoint 情况下被截断，多资产状态互不串扰。

每批结果写入独立 Parquet staging part。所有 part 完成后统一检查行数、资产数、重复键、合法状态、规则/参数唯一性以及第 11 个实际交易日后的 MA5。通过后仅执行一次正式状态 `INSERT INTO ... SELECT FROM read_parquet(...)`，并与生产运行成功更新处于同一事务。

历史输入发现无法解释的市场事实时，所有异常行写入 `unresolved-market-facts-*.parquet` 清单，停止计算和正式导入。失败时正式状态表不写入，staging 与 `failure.json` 保留用于诊断；成功默认清理 staging，`--keep-staging` 可保留 manifest 和分片。

## 5. 每日增量

入口：

```bash
qrp-atlas-system-b readiness --trade-date YYYY-MM-DD
qrp-atlas-system-b run-daily --trade-date YYYY-MM-DD
```

流程：

1. 检查目标日市场开市、五张上游表、股票域、市场事实分类、前复权结果和 MA5；
2. 一条集合化 SQL 生成当日全市场输入；
3. 一条集合化 SQL 读取目标日前同规则/参数的每股最近成功状态；
4. 对全部资产一次调用状态机；
5. 完整性校验并写一个 Parquet part；
6. 一个事务批量导入当日状态并标记运行成功。

继续上市的资产缺少上一成功状态时整批以 `MISSING_PREVIOUS_SUCCESS_STATE` 失败；上市实际交易日 1 的新股允许无 checkpoint。目标日非市场交易日以 `TARGET_NOT_MARKET_TRADING_DAY` 失败，缺失日线且没有明确停牌事实以 `MISSING_DAILY_MARKET_FACT` 失败。两类失败都不会写入部分状态或生成成功 production run。

每日修订严格 fail-closed：目标日晚于最新成功状态日时正常续算；等于最新成功状态日时允许从前一日 checkpoint 重算同日；早于最新成功状态日时以 `BACKDATED_RECOMPUTE_REQUIRED` 拒绝，避免修订后的过去状态与旧路径计算的后续状态被拼接。未来的 `recompute-from --start-date` 应从修订日起正向重放至最新日期，并作为一致修订批次提交，本工作包尚未实现该能力。

## 6. 幂等、修订与恢复

输入标准行按稳定顺序计算 SHA-256，形成 `input_snapshot_id`。相同运行类型、日期范围、规则版本、参数版本和快照已有成功运行时返回 `IDEMPOTENT_NOOP`，不重复插入。

上游行情或复权因子修订会形成新的快照 ID，追加一套可追溯事实；查询按最新成功运行选择，不覆盖旧快照。同日最新状态允许通过 `run-daily` 修订；早于最新成功日期的每日修订，以及任何针对非空目标库的非幂等 `initialize`，都必须等待级联重算入口，不能产生非级联历史。失败运行保留稳定错误码、错误详情和 staging，不会被标记成功。

多次复权稳定性测试分别以不同截止日生成前复权历史：共同日期上的 `close` 与 `ma5` 可以随最新复权因子按相同比例缩放，但 `close / ma5`、线上/线下关系、状态、前后状态和连续计数必须逐日一致。该性质说明多次复权需要通过 `input_snapshot_id` 和审计口径治理显示数值版本，不应改变 System B 状态迁移语义。

## 7. API

```text
GET /api/v1/system-b/states/latest
GET /api/v1/system-b/states?trade_date=YYYY-MM-DD
GET /api/v1/system-b/assets/{asset_id}/history
GET /api/v1/system-b/transitions?trade_date=YYYY-MM-DD
GET /api/v1/system-b/summary?trade_date=YYYY-MM-DD
GET /api/v1/system-b/production-runs/latest
```

状态列表和历史接口提供 `limit/offset`，历史接口还支持起止日期。默认规则版本和参数版本为冻结的 System B 2.0 值，也可显式查询其他版本。

## 8. Schema 与部署

Schema 入口：

```bash
qrp-atlas-system-b migrate
```

部署示例位于：

```text
deploy/qrp-atlas-system-b-daily.service
deploy/qrp-atlas-system-b-daily.timer
```

PR 合并前不得安装或启用 timer，也不得在正式库执行 migration 或初始化。合并后建议先运行配置 doctor、readiness、临时输出全量演练，再单独安排正式 migration 和初始化窗口。

## 9. 性能证据

性能演练使用正式行情库只读输入、`/tmp` 临时 DuckDB 输出和临时 Parquet staging；没有修改正式数据库。

### 全历史初始化性能基准

以下成功导入性能数据来自缺失事实 fail-closed 规则加入前的同一批量主链，用于证明集合化读取、分批计算、Parquet staging 和单次导入的吞吐能力；它不再作为当前正式库全历史数据完整性通过证明。

```text
正式输入库: /home/claire/data/qrp-atlas/db/quant.db（只读）
测试范围: 1990-12-19 ~ 2026-07-24
输入资产数: 5,839
输入行数: 18,527,184
输出行数: 18,527,184
标准输入 SQL: 1 次
资产批次数: 59
资产批量大小: 100
数据读取耗时: 91.550 秒
状态计算耗时: 310.878 秒
Parquet staging 写入耗时: 32.632 秒
单事务批量导入耗时: 96.101 秒
总耗时: 649.148 秒
峰值内存: 9,913.637 MB
```

结果分布：`NEW_LISTING_WARMUP=59,029`、`BASE=7,396,530`、`CANDIDATE=1,903,813`、`ACTIVE=9,167,812`，停牌保持 990,152 行。完整性校验和一次正式批量导入均在临时输出库完成。

### 缺失事实治理实测

加入 fail-closed 分类和读取计时校准后，对同一正式库执行只读全历史演练：一条集合化 SQL 扫描 5,840 个资产、18,533,100 行，59 个资产批次，识别出 328,569 条 `UNRESOLVED_MISSING`，涉及 2,520 个资产和 1990-12-19 至 2014-04-21，写出 38 个异常 Parquet 分片后以 `MISSING_DAILY_MARKET_FACT` 失败；状态计算为 0，正式导入为 0。数据读取耗时 25.411 秒，异常 staging 写入 1.232 秒，峰值内存 10,031.99 MB。校准后的读取计时覆盖 `fetch_df_chunk()` 在生成器首次 `yield` 前发生的实际取数时间。

该结果证明当前实现不会把历史行情缺口自动归类为停牌。正式初始化前必须治理异常清单或补齐权威停牌/行情事实；不得绕过该检查。

### 每日增量实测

2026-07-23 的真实输入、临时输出运行：

```text
资产/输入/输出: 5,515 / 5,515 / 5,515
集合化数据查询: 2 次（当日输入 + checkpoint）
批次数: 1
数据读取耗时: 4.175 秒
状态计算耗时: 0.126 秒
staging 写入耗时: 0.152 秒
批量导入耗时: 1.163 秒
总耗时: 6.463 秒
峰值内存: 3,847.535 MB
```

每日结果与全历史初始化中同一日期的 5,515 行逐列比较，双向 `EXCEPT` 均为 0。2026-07-24 相同输入的幂等重跑返回既有成功运行，不重复计算或插入。

实现级证明：

- 历史标准输入数据 SQL：1 次；
- 每日标准输入数据 SQL：1 次；
- 每日 checkpoint 数据 SQL：1 次（同日修订时允许一次受影响资产历史回退查询）；
- 无每资产 SQL；
- 无每日期 SQL；
- 无逐行 `INSERT`；
- 无逐行 `UPDATE`；
- 无每资产事务；
- 正式状态导入：每次运行 1 次批量 `INSERT INTO ... SELECT`。

上述查询次数统计大体量业务数据读取，不包含 schema/readiness 元数据检查、staging 完整性聚合和生产运行审计语句。

## 10. 未实现边界

本工作包不实现：行情轮次、`episode_status`、MA10 轮次结束、涨停状态池、高度池、容量池、辨识度池、题材、评分、M1—M6 身份、判断层、组合管理、execution、QMT 实盘连接，也不修改外部业务规则仓库 `MyTradingSystem`。

## 11. 测试与兼容性证据

```text
System B 状态生产/API 专项: 17 passed
System B 契约、2.0 状态机与旧版兼容集合: 59 passed
全量测试: 795 passed
python -m compileall -q src tests: passed
OpenAPI System B 路径检查: 6 paths passed
git diff --check: passed
```

`state_machine_v2.py`、旧 `detector.py` 和 `system_b_basic@1.0.0` 均未修改。项目未配置独立 lint、type-check 或 migration-check 工具；本工作包没有新增运行依赖。
