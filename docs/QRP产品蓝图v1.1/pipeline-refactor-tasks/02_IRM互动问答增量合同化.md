# 任务提示词｜IRM 互动问答增量合同化

## 工作目标

将现有 `irm_qa_incremental` 正式改造为 PipelineContract，并验证其适合未来由 Job Orchestration 高频运行。

该任务当前由 Hermes 在 08:00–21:59 每 5 分钟触发。源码合同化完成后仍不得切换生产调度权。

## 调查范围

重点调查：

- `src/qrp_atlas/pipeline/irm_qa/`
- 互动易 / P5W provider 调用、分页、清洗、去重与入库代码
- `quant.db.irm_interaction_qa` 对应 contracts、映射和测试
- `deploy/pipeline/pipeline-registry.json` 中 `irm_qa_incremental`
- 仓库内可审计的 Hermes wrapper 与运行文档
- Pipeline 正式开发规则与已有六条正式 Contract

不得仅依据“每 5 分钟执行”推测增量窗口、游标或完整性规则。先从现有代码和数据键确定真实语义。

## 必须完成

1. 建立稳定 ID 为 `irm_qa_incremental` 的正式 `PipelineContract` 和进程内 executor。
2. 明确任务的时间窗口或增量范围来源：当前时间、业务日期、最新记录、水位线或 provider 参数必须有源码事实支持。
3. 明确 provider 分页、总数、终止条件、重复页面和部分返回的处理；能证明完整性时必须 fail-closed，不能证明时要在 Contract 和文档中写清边界。
4. 明确 `irm_interaction_qa` 的真实唯一键、重复问答、修订记录、空增量和同一窗口重跑语义。
5. 高频任务必须使用 `overlap_policy=FORBID`，并写 `quant.db` 时使用 `quant_db_writer`；不得依靠 cron 错峰或进程内布尔值保证互斥。
6. 将同一个 `ExecutionControl` 贯穿 provider 请求、分页循环、重试等待、清洗批次和写事务；取消或到期后不得继续翻页或开始写事务。
7. 形成结构化 `PipelineResult`，准确报告 API 请求数、分页数、读取行数、去重后行数、写入行数和数据库写入耗时。
8. 给出适合 5 分钟触发场景的 timeout、retry 和性能预算。预算基于 mock 的等价页数测试与现有 provider timeout，不夸大为生产历史证据。
9. 完整验收后加入 source catalog，并更新注册表状态、完成标记、幂等和性能证据。

## 测试要求

至少覆盖：

- 正常单页与多页增量；
- 无新增数据时的明确 NOOP 或成功零写入语义；
- 同一窗口重复执行不产生重复有效记录；
- provider 返回缺字段、错误结构、重复页、页数变化、部分失败和网络异常；
- 已有活动 run 时无法再次 claim；
- 取消或 deadline 在请求前、分页中、provider 返回后和写事务前生效；
- 数据库写入失败不会把部分结果标记为成功；
- API 请求数、batches、rows_read、rows_written 与实际行为一致；
- 当前全部正式 Contract 注册校验不回归。

测试不得访问真实 P5W 或生产数据库。

## 边界

- 不设计新的消息队列、分布式游标或流式采集框架。
- 不为了理论上的海量数据提前分库或改表。
- 不修改 Hermes 调度区间和频率。
- 不触及 CNINFO、研究报告、手动入库或 System B。
- 发现历史实现存在数据正确性缺陷时，只做当前任务运行必需的最小修复，并单独记录。

## 完成后汇报

汇报真实增量算法、唯一键与幂等策略、分页完整性边界、锁与 overlap、ExecutionControl 传播、性能证据、测试结果、修改文件和仍未解决事实。不得部署或合并目标分支。

## 后续变更（2026-08，fix/v1.1-irm-dedicated-db）

IRM 已从共享主库 `quant.db` 完全迁移到独立数据库 `irm_qa.duckdb`：

- 新配置项 `settings.paths.irm_qa_duckdb_path`（环境变量 `QRP_IRM_QA_DUCKDB_PATH`，默认 `<数据根目录>/db/irm_qa.duckdb`）；
- Contract 输出资源改为 `irm_qa_db`，写锁改为 `irm_qa_writer`，location 为 `settings.paths.irm_qa_duckdb_path`；
- 主库 bootstrap（`init_database`）不再创建可写 IRM 表，IRM 表由 `init_irm_database` 在独立库创建；
- 一次性迁移工具 `scripts/migrate_irm_qa_to_dedicated_db.py`（幂等、fail-closed）；
- 旧主库 `quant.db.irm_interaction_qa` 保留为迁移时点冻结回滚副本，不再写入。
