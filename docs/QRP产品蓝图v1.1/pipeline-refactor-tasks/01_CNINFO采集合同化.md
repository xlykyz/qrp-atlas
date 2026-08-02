# 任务提示词｜CNINFO 采集合同化

## 工作目标

将 `src/qrp_atlas/pipeline/cninfo/` 中的**机构调研数据采集业务能力**正式接入 PipelineContract 与 Job Orchestration。

本任务与 Hermes cron 无关。不得把 `main`、`noon`、`afternoon` 等历史 Job 名称、触发时刻或 wrapper 日期拼接方式建模为正式 Pipeline 身份。

当前源码显示的核心业务能力是：对显式目标自然日，从 Eastmoney 获取机构调研记录，完成分页、清洗、契约校验和幂等入库。默认应收口为一条正式业务 Contract；只有源码和业务规则证明存在与调度时间无关的不同算法、输入或输出时，才允许拆分。

## 调查范围

重点调查：

- `src/qrp_atlas/pipeline/cninfo/`
- 现有 CNINFO tests、contracts、映射和表结构
- 旧 CLI 对 `--date`、多日期和 `--incremental` 的真实行为
- `docs/QRP产品蓝图v1.1/12_Pipeline正式开发规则.md`
- 运行现状文档和注册表仅用于区分历史调度事实，不用于决定 Contract 数量

先回答：CNINFO 模块有几项独立业务生产逻辑？历史三个 Hermes Job 是否只是同一采集逻辑的不同触发方式？当前证据若不能证明业务差异，必须采用单一 Contract。

## 必须完成

1. 建立一条稳定业务身份的正式 `PipelineContract`，名称表达“CNINFO / 机构调研采集入库”，不得包含 `main`、`morning`、`noon`、`afternoon`、`evening` 等调度标签。
2. Contract 对一个显式目标自然日执行采集。目标日期属于业务调用参数或 `PipelineInvocation`，不得由 Hermes wrapper 语义决定。
3. 不在本任务中决定每天几点运行、一天运行几次、是否额外回补前一交易日；这些属于未来部署与调度设计。
4. 明确 Eastmoney provider 输入、返回字段、分页终止条件、总页完整性、重复页、部分响应、空结果语义和稳定错误码。
5. 明确 `quant.db.cninfo_research_visits` 的真实唯一键、清洗去重、写入模式、完成标记、同一日期/范围重跑和失败恢复语义。
6. 写 `quant.db` 时统一声明 `quant_db_writer`；只有实际读取的表才进入 `resource_reads`。如果业务 Contract 不需要 trading calendar，不得仅因历史 wrapper 使用它而建立输入依赖。
7. 将同一个 `ExecutionControl` 传入 provider 请求、重试、分页循环、清洗批次和写事务；请求前后、继续分页和开始/继续写事务前执行 `check()`。
8. 给出基于当前 provider 行为和本地等价规模测试的 timeout、retry 与性能预算，不引用 Hermes wrapper timeout 伪装为 Pipeline 性能基线。
9. 完整通过后加入 source catalog。新增正式 Contract 数量应与独立业务能力一致，默认从 6 条增至 7 条，而不是按三个 Hermes Job 增至 9 条。
10. 更新注册表和权威文档时，明确区分：
   - 一条正式 CNINFO 业务 Contract；
   - 三个历史 Hermes 调度 Job 仅作为迁移前运行事实，可映射到同一业务能力。

## 测试要求

至少覆盖：

- 单一 Contract 的注册、描述、稳定业务 ID 和显式目标日期/范围解析；
- 自然日目标，包括交易日、周末和节假日；CNINFO 公告日期不应被股票交易日历无依据地拒绝；
- 单日期、多日期/范围、空结果、重复 provider 记录和同一目标重跑；
- 缺字段、错误公告日期、provider 异常、分页中断、重复页和部分响应；
- 数据库事务失败时不留下被误判为完成的半成品；
- 已取消或 deadline 到期时不继续请求、不继续翻页、不进入新写事务；
- `quant_db_writer`、输出指标和实际入库行数一致；
- 既有六条市场数据 Contract 继续注册和校验通过，默认正式 Contract 总数与实际新增业务能力一致。

测试只允许使用 mock Eastmoney、临时 DuckDB 和临时文件。

## 边界

- 不修改 Hermes cron、生产 wrapper、systemd、部署选择或任何触发时间。
- 不为了复刻历史 wrapper 而在 Contract 中实现“前一交易日 + 今天”等调度补偿策略。
- 不改变 CNINFO 表契约和既有采集业务规则，除非当前实现会直接产生错误数据；这种修复必须单独说明。
- 不建设通用分页框架、错误码注册中心或新的 provider 平台，除非当前 CNINFO 范围确实需要且能显著减少重复。
- 不触及研究报告、IRM、System B 或其他工作包。

## 完成后汇报

汇报独立业务能力判断、最终 Contract 数量和稳定 ID、目标日期/范围参数、输入输出、分页完整性、幂等与事务、ExecutionControl、测试结果、修改文件、注册表映射、未解决事实和非阻塞未来建议。不得合并目标分支或执行部署。
