# 任务提示词｜PIT、基本面与业绩预告合同化

## 工作目标

将以下三条现有人工数据能力正式接入 PipelineContract：

- `pit_backfill`
- `fundamentals_ingest`
- `earnings_forecast_ingest`

这是本轮工作量最大的手动入库包。允许在一个工作包内按上述顺序分阶段完成，但最终必须统一验收。不得把 dated systemd 示例或历史修复脚本变成生产 Pipeline。

## 调查范围

重点调查：

- `src/qrp_atlas/pipeline/pit_backfill/`
- `src/qrp_atlas/pipeline/fundamentals/`
- `src/qrp_atlas/pipeline/earnings_forecast/`
- 对应 contracts、PIT 表、财务表、revision/run 表、CLI 和测试
- deploy 中 dated PIT backfill 示例，只用于理解历史调用，不作为新部署依据
- `deploy/pipeline/pipeline-registry.json`
- Pipeline 正式开发规则与现有正式 Contract

必须从代码确认每条能力的真实参数、数据范围、修订语义、完成标记和恢复方式；不得把多个历史脚本的行为拼成未经验证的新规则。

## 必须完成

1. 为三个稳定 ID 分别建立正式 `PipelineContract`，全部保持人工触发，`manual_execution_allowed=true`，无默认生产 schedule。
2. 将现有显式 ticker、日期、报告期、数据集、文件或恢复范围整理为 `ParameterContract`；隐藏路径和凭据只通过统一 settings 提供。
3. `pit_backfill` 必须明确其通用正式能力与 dated 一次性部署材料的边界，不能把某个历史日期或机器路径固化进 Contract。
4. `fundamentals_ingest` 必须明确各财务表的输入、输出、报告期、公告/可用时间和同一报告修订语义，保持 point-in-time。
5. `earnings_forecast_ingest` 必须明确 revision 与 ingestion run 记录的关联、唯一键、重复采集和修订保留规则。
6. 对每条任务明确写入模式、完成条件、空结果、部分证券/期间失败、重跑和中断恢复。成功退出不能替代 completion 检查。
7. 写 `quant.db` 统一使用 `quant_db_writer`；读依赖按表进入 `resource_reads`，不制造伪业务依赖。
8. 将同一个 `ExecutionControl` 传入 Tushare 请求、retry/backoff、证券/期间批次和写事务；取消或超时后不得继续下一个批次。
9. 对长范围任务给出可查询的结构化指标：请求数、批次、资产、期间、读取/写入行数、临时磁盘和数据库写入时间。
10. 根据现有规模建立可重复本地性能基线。发现几 GB 数据操作出现不合理的分钟级退化时，应定位算法或 SQL 问题，不能只提高 timeout。
11. 验收通过后加入 source catalog，并更新注册表的正式状态、参数、完成标记、PIT/修订和性能证据。

## 测试要求

至少覆盖：

- 三条 Contract 的注册、人工执行权限和参数类型；
- ticker、日期、报告期和范围的正常、边界与非法组合；
- 同一数据重复运行、旧 revision、新 revision 和历史保留；
- provider 空结果、缺字段、部分证券失败、批次中断和重试耗尽；
- 本地历史文件输入存在、缺失、损坏和身份不一致；
- 事务或 staging 中断后没有半成品被视为成功；
- 取消/deadline 在请求前、批次中、provider 返回后和写事务前生效；
- `PipelineResult` 指标、输出和 completion 一致；
- 标准规模性能测试记录扫描范围和写入耗时；
- 全部已注册正式 Contract 继续校验通过。

测试只使用临时文件、mock provider 和临时 DuckDB。

## 边界

- 不重写财务或 PIT 业务模型，不新增第二套版本系统。
- 不迁移、删除或执行 dated systemd 服务。
- 不把历史修复脚本、数据迁移脚本和验证工具全部注册为 Pipeline。
- 不自动调度人工 backfill，不修改 Hermes。
- 不触及成员关系、研究报告、IRM 或 System B。
- 只修复会导致当前合同化结果写错、丢历史、不可恢复或无法运行的问题。

## 完成后汇报

逐条汇报参数模型、PIT/修订语义、唯一键、事务与恢复、性能基线、测试结果、修改文件、注册表变化、未解决事实和非阻塞建议。不得部署或合并目标分支。
