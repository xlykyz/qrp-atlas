# 基础数据 Pipeline 性能基线

本文件区分两类性能证据：可重复的内部处理基线，以及运行时的端到端保护阈值。内部基线不使用生产数据库、生产凭据或真实外部接口请求，不能被解释为网络端到端耗时证据。

## 测量方法

- 命令：`.venv/bin/pytest -q -s tests/pipeline/test_market_data_contracts.py::test_equivalent_daily_scale_benchmark_records_metrics_for_all_six_contracts`
- 数据库：pytest `tmp_path` 中新建的 DuckDB，使用仓库现有 `init_database` 表契约。
- 日期：`2026-07-29`，交易日历包含相邻交易日和周末自然日记录。
- 市场、daily_basic、adj_factor、suspend_d：5,000 条同日记录。
- 指数：4 个既有核心指数，每个响应包含目标日前一日和目标日。
- 涨跌停池：涨停、跌停各 200 条。
- 外部接口：完全 mock；因此结果不把网络等待混入算法或数据库预算。

## 2026-07-30 基线

| Pipeline | 输入行数 | 输出行数 | 总耗时（秒） | 数据库写入 | 标准范围 |
| --- | ---: | ---: | ---: | --- | --- |
| `market_daily_update` | 5,000 | 5,000 | 0.630 | target-date replace | 5,000 股票、历史 enrich 查询、目标日替换 |
| `adj_factor_daily` | 5,000 | 5,000 | 0.232 | target-date replace | 5,000 市场 ticker 的全集校验、变化点比较和完整目标日变化集替换 |
| `daily_basic_update` | 5,000 | 5,000 | 0.276 | target-date replace | 5,000 股票 daily_basic 清洗与替换 |
| `index_daily_update` | 4 | 4 | 0.165 | 4-key UPSERT | 四条既有核心指数响应过滤到目标日 |
| `zt_dt_pool_daily` | 400 | 400 | 0.231 | 双表原子 replace | 涨停、跌停各 200 条 |
| `suspend_d_ingest` | 5,000 | 5,000 | 0.197 | target-date replace | 5,000 停复牌事件 |

所有结果由该专项测试中的 `PipelineResult` 直接输出，包含 `rows_read`、`rows_written`、`api_requests`、阶段耗时和 `database_write_seconds`。该测试只证明集合计算、清洗、覆盖校验和 DuckDB 事务在等价本地数据规模下的处理性能；不包含 Tushare、AkShare 或 Eastmoney 的网络等待。

## 端到端阈值

当前 worktree 没有可审计的 Hermes 历史耗时副本，且开发隔离规则禁止读取 `~/.hermes`。因此本次没有把未核验的生产运行时长写成性能基线。Contract 中的端到端字段是运行时保护阈值，不是“已证明的正常耗时”：它们由现有代码可验证的外部调用数量、Tushare 默认节流/重试配置，以及 Eastmoney 的显式 15 秒请求 timeout 推导。

| Pipeline | 告警阈值 | 运行时端到端阈值 | 强制 timeout | 可复核依据 |
| --- | ---: | ---: | ---: | --- |
| `market_daily_update` | 30 秒 | 60 秒 | 120 秒 | 一次 Tushare `daily` 调用；默认间隔 0.6 秒、客户端默认重试 0 次；随后为本地集合处理和单日事务。 |
| `adj_factor_daily` | 30 秒 | 60 秒 | 120 秒 | 一次 Tushare `adj_factor` 调用；同上；随后为集合变化点比较和单日替换。 |
| `daily_basic_update` | 30 秒 | 60 秒 | 120 秒 | 一次 Tushare `daily_basic` 调用；同上；随后为集合覆盖校验和单日事务。 |
| `suspend_d_ingest` | 30 秒 | 60 秒 | 120 秒 | 一次 Tushare `suspend_d` 调用；同上；随后为单日事务。 |
| `index_daily_update` | 60 秒 | 120 秒 | 240 秒 | 四次既有 AkShare 指数请求，全部完成后才写入四行。 |
| `zt_dt_pool_daily` | 60 秒 | 120 秒 | 300 秒 | 两个 Eastmoney 池按返回 `total` 全部分页；每次 `urlopen` 明确 timeout 15 秒，300 秒对应至多 20 个连续受限请求的运行时保护窗口。 |

上述阈值的作用是让超时、重试和性能告警可由现有 runtime 执行和记录，不能作为“日常运行应在一分钟或两分钟内完成”的历史结论。若实际运行持续超过端到端阈值，应先根据结构化结果中的 `api_requests`、`batches`、阶段耗时和 `database_write_seconds` 区分外部接口、SQL 扫描、全量重算或数据库使用问题；不得仅提高 timeout 掩盖退化。

外部接口实际等待由每次运行的 `api_requests`、`batches`、阶段耗时和 runtime wall duration 记录。获得可只读审计且允许访问的历史 runtime SQLite 记录后，必须按 Pipeline、输入规模和分页数提取真实端到端分位数，再更新本文件和 Contract 的端到端阈值；本地基线与历史端到端证据必须继续分开记录。
