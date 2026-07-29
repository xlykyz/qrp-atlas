# 基础数据 Pipeline 性能基线

本文件是六条正式基础数据 Pipeline 的可重复性能证据。它只记录计算、清洗、集合写入和本地 DuckDB 的基线；不使用生产数据库、生产凭据或真实外部接口请求。

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
| `market_daily_update` | 5,000 | 5,000 | 0.571 | target-date replace | 5,000 股票、历史 enrich 查询、目标日替换 |
| `adj_factor_daily` | 5,000 | 5,000 | 0.215 | target-date UPSERT | 5,000 市场 ticker 的全集校验与变化点比较 |
| `daily_basic_update` | 5,000 | 5,000 | 0.181 | target-date replace | 5,000 股票 daily_basic 清洗与替换 |
| `index_daily_update` | 4 | 4 | 0.150 | 4-key UPSERT | 四条既有核心指数响应过滤到目标日 |
| `zt_dt_pool_daily` | 400 | 400 | 0.195 | 双表原子 replace | 涨停、跌停各 200 条 |
| `suspend_d_ingest` | 5,000 | 5,000 | 0.164 | target-date replace | 5,000 停复牌事件 |

所有结果由该专项测试中的 `PipelineResult` 直接输出，包含 `rows_read`、`rows_written`、`api_requests`、阶段耗时和 `database_write_seconds`。

## 预算与判定

六条 Contract 统一采用：警告阈值 1 秒，正常预算 2 秒，runtime 硬 timeout 60 秒。

2 秒正常预算是上述最大 0.571 秒本地基线的 3.5 倍，覆盖同规模的本地计算和 DuckDB 写入波动。60 秒只给外部 provider 与 runtime retry 留出受控上限，不改变正常预算；超过 2 秒必须在结构化结果中留下性能告警，并从 API、SQL 扫描范围、全量重算或数据库使用方式中定位原因，不能通过提高正常预算掩盖退化。

外部接口实际等待由每次运行的 `api_requests`、阶段耗时和 runtime wall duration 记录。将来要调整预算，必须重新运行上述等价规模基准，或引用保留在 runtime SQLite 中的真实同规模运行记录，并同步更新本文件和 Contract `baseline_source`。
