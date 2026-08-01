# 任务提示词｜CNINFO 采集合同化

## 工作目标

将现有 CNINFO 采集能力正式接入 PipelineContract 与 Job Orchestration，使以下三个稳定身份达到可注册、可测试、可手动运行、可被未来部署选择引用的正式条件：

- `cninfo_main_update`
- `cninfo_incremental_noon`
- `cninfo_incremental_afternoon`

当前生产仍由 Hermes 调度。本任务只完成源码合同化和离线验收，不切换调度权。

## 调查范围

重点调查：

- `src/qrp_atlas/pipeline/cninfo/`
- 现有 CNINFO tests、contracts 和表结构
- `deploy/pipeline/pipeline-registry.json` 中三条 CNINFO 记录
- 仓库内可审计的历史 wrapper、文档与调用入口
- `docs/QRP产品蓝图v1.1/12_Pipeline正式开发规则.md`

先确认三条任务在目标日期、采集范围、增量窗口、重复处理和完成语义上是否确实不同。默认保留三个稳定 `pipeline_id`；可以共享底层实现，但不得仅因代码相似而擅自合并业务身份。

## 必须完成

1. 为范围内三个 ID 建立正式 `PipelineContract`，或在现有事实证明某个 ID 只是部署别名时给出可审计结论并采用不丢失历史身份的最小方案。
2. 将 shell 动态日期参数替换为统一 `PipelineInvocation` / `TargetDatePolicy` 语义，禁止业务 executor 自行使用 `date.today()` 推断目标日。
3. 明确 CNINFO provider 输入、返回字段、分页或批次完整性、空结果语义和稳定错误码。
4. 明确 `quant.db.cninfo_research_visits` 的真实唯一键、写入模式、完成标记、重复执行和失败恢复语义；不得把“CLI 退出 0”作为唯一完成依据。
5. 写 `quant.db` 时统一声明 `quant_db_writer`；实际只读表按需进入 `resource_reads`。
6. 将同一个 `ExecutionControl` 传入 provider 请求、重试、分页循环和写事务；请求前后及继续分页、开始写事务前执行 `check()`。
7. 给出基于现有调用次数和本地处理规模的 timeout、retry 与性能预算；没有证据时保守设置保护阈值，不伪造历史性能结论。
8. 完整通过后加入 source catalog，并更新注册表中这三条记录的模块证据、合同状态、完成语义和仍未解决事实。

## 测试要求

至少覆盖：

- 三个稳定 ID 的注册、描述和目标日期解析；
- 正常增量、无新增数据、重复返回和同日重跑；
- 缺字段、错误日期、provider 异常、分页中断和部分响应；
- 数据库事务失败时不留下被误判为完成的半成品；
- 已取消或 deadline 到期时不继续请求、不进入新写事务；
- 三条 Contract 均使用 `quant_db_writer`；
- 当前六条市场数据 Contract 继续注册和校验通过。

测试只允许使用 mock CNINFO、临时 DuckDB 和临时文件。

## 边界

- 不修改 Hermes cron、生产 wrapper、systemd 或部署启用项。
- 不改变 CNINFO 表契约和既有采集业务规则，除非当前实现会直接产生错误数据；这种修复必须单独说明。
- 不建设通用分页框架、错误码注册中心或新的 provider 抽象层，除非当前 CNINFO 范围确实需要且能显著减少重复。
- 不触及研究报告、IRM、System B 或其他工作包。

## 完成后汇报

汇报三个 ID 的真实语义差异、Contract 数量与共享实现方式、输入输出、幂等与事务、测试结果、修改文件、注册表变化、未解决事实和非阻塞未来建议。不得合并目标分支或执行部署。
