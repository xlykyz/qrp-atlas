# Pipeline 正式开发规则

## 1. 地位与范围

本文件是 QRP Atlas v1.1 数据 Pipeline 的权威开发规则。任何新增 Pipeline，或任何会改变 Pipeline 生产语义的修改，必须同时满足本文件、源码 `PipelineContract` 和公共契约测试。

本规则建立的是正式开发门禁、机器可读合同、公共校验和模板；它本身不是生产切换声明。当前工作包已将六条市场数据、CNINFO、IRM 以及两条成员关系历史能力纳入源码正式 Contract，但仍不修改 Hermes，不启用 Production Definition，不部署 systemd，也不访问生产数据路径；研究、System B 和其他业务仍按各自工作包推进。

正式上线采用单一二元门禁：一个 Pipeline 完整通过本规则、契约校验和公共验收，才能被放入部署选择清单；否则不得被正式 QRP runtime 调度。现有生产继续由 Hermes 保持原有行为，直到全部计划内 Pipeline 都完成同一标准的改造并统一验收、统一切换。

## 2. 三层职责

### 2.1 Pipeline 源代码

每条 Pipeline 的源码 `PipelineContract` 是业务生产语义的唯一事实来源，必须定义：

- 稳定身份、名称、说明、合同版本和正式 executor；
- 参数覆盖、目标交易日/范围策略和交易日历；
- 输入、最低结构、新鲜度检查和失效错误码；
- 输出、写入模式、唯一键、完成标记和质量检查；
- 业务依赖、物理资源锁、幂等、事务和失败恢复；
- overlap、retry、timeout 和性能预算；
- 可机器判定的 `PipelineResult`。

业务逻辑可以是 Python、DuckDB SQL、文件处理或外部 API 客户端，但它必须放在统一外层生命周期之内。源码不得把业务参数、数据库路径、日期算法、依赖、锁或失败判断交给部署层补齐。

### 2.2 Pipeline runtime

通用 `qrp_atlas.orchestration` 是 Job 编排基础，负责：

- scheduler 根据 cron 创建运行记录和判断依赖状态；
- SQLite run record 的 claim、overlap、lease、heartbeat 和 stale recovery；
- 物理资源锁；
- 正式 Contract 的进程内 executor、兼容 argv Definition 的子进程监督、timeout、retry、日志、状态和历史；
- 将 `PipelineResult` 或编排合成的 `JobResult` 写入独立 runtime SQLite。

正式 Contract 不重写 scheduler、锁或 lease。它通过 `qrp_atlas.pipeline.job_adapter` 映射为 `JobDefinition`，使 Orchestration 得到 executor 入口、依赖、锁、timeout、retry、性能预算和新鲜度摘要。

### 2.3 生产部署

部署选择清单只能决定是否启用以及何时触发。每个条目只允许：

```json
{
  "pipeline_id": "market_daily_production",
  "enabled": true,
  "schedule": "15 16 * * 1-5"
}
```

合同选择文件的外层只有 `schema_version` 和 `pipelines`，每个 `pipelines` 项严格只含上述三个字段。全局时区属于 runtime 环境级设置。部署层不得保存或覆盖数据库路径、表名、业务 argv、目标日期、依赖、锁、freshness、completion、timeout、retry、性能预算或失败规则。

当前 `deploy/pipeline/pipeline-definitions.shadow.json` 是历史 shadow Foundation，不是正式 Contract 部署清单，不能作为正式上线依据。

## 3. 正式合同

源码类型位于 `qrp_atlas.pipeline.contracts`，注册接口位于 `qrp_atlas.pipeline.registry`。合同是 Python 的不可变 dataclass，并可通过 `PipelineContract.describe()` 输出无凭据的机器可读 JSON 视图。

### 3.1 `PipelineContract` 字段

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `pipeline_id` | `str` | 全局唯一、稳定 `snake_case` 身份；生产重命名必须走显式迁移。 |
| `name` | `str` | 人类可读名称。 |
| `description` | `str` | 明确业务范围与完成目标。 |
| `contract_version` | `str` | 合同/生产语义版本。 |
| `kind` | `PipelineKind` | `ATOMIC` 或只聚合依赖的 `DAG`。 |
| `executor` | `PipelineExecutor` | 统一上下文到 `BusinessExecution` 的正式执行入口。 |
| `target_date_policy` | `TargetDatePolicy` | 日期解析、交易日历和显式日期校验。 |
| `parameters` | `tuple[ParameterContract, ...]` | 源码拥有的附加参数名、类型、默认值和必填约束。 |
| `inputs` | `tuple[InputContract, ...]` | 输入身份、结构与 freshness。 |
| `outputs` | `tuple[OutputContract, ...]` | 输出、完成和最低质量。 |
| `dependencies` | `tuple[str, ...]` | 业务 Pipeline 依赖，不表示物理锁。 |
| `resource_locks` | `tuple[str, ...]` | 物理资源互斥声明。 |
| `resource_reads` | `tuple[str, ...]` | 共享只读资源声明；正式 Contract 可用表级 DuckDB 资源表达读取范围。 |
| `manual_execution_allowed` | `bool` | 是否允许通过正式 CLI `run` 提交该 Contract 的人工 Run（及其人工依赖链）；部署选择不能覆盖。 |
| `idempotency` | `IdempotencyContract` | 重跑、恢复、staging 和原子替换语义。 |
| `transaction` | `TransactionContract` | 写入事务边界和失败可见性。 |
| `execution` | `ExecutionPolicy` | overlap 规则与最大 retry。 |
| `performance` | `PerformanceBudget` | 日常预算、告警阈值、硬 timeout 和基线证据。 |

合同校验要求所有字符串化业务语义非空、所有 ID 无重复、依赖可发现且 DAG 无环。`pipeline_id` 只能使用稳定 `snake_case`，不能从文件路径、shell、时间戳或环境变量临时推导。

### 3.2 运行上下文与日期

`PipelineInvocation` 在运行前携带 `run_id`、`pipeline_id`、`scheduled_for`、`attempt`、统一 `AppSettings`、显式参数覆盖、审计上下文和一个 `ExecutionControl`。`TargetDatePolicy` 必须提供：

- `policy_id`、说明和真实交易日历身份；
- 非交易日行为：拒绝、上一交易日、下一交易日或明确允许自然日；
- 由 `scheduled_for` 和统一配置解析业务日期/范围的 `resolver`；
- 对 `--trade-date YYYY-MM-DD` 显式覆盖的校验器。

公共生命周期把解析结果写入 `PipelineRunContext.target_window`。业务代码不得直接用 `date.today()` 或私有环境变量推断目标日期。显式日期不通过交易日策略时，结果必须为 `FAILED`，错误码为 `TARGET_DATE_OVERRIDE_INVALID`。

`PipelineInvocation.execution_control` 必须原样传入 `PipelineRunContext.execution_control`；业务 executor 不得替换、复制或丢弃该实例。目标日期/参数解析、输入与 freshness 检查、外部 I/O 前后以及进入或继续 DuckDB 写事务前都必须调用 `execution_control.check()`。网络请求、provider retry/backoff 和其他阻塞等待必须使用不超过剩余 deadline 的 `bounded_timeout()` 或等价受控等待。Python 线程不会被强制终止，正式 executor 不得忽略取消或进行不可取消的无限阻塞。

除 `--trade-date` 外，业务参数必须以 `ParameterContract` 声明名称、`STRING`/`INTEGER`/`BOOLEAN`/`DATE` 类型、说明、必填性和类型正确的默认值。手动调用统一使用 `--set name=value`；未知、重复、缺失或类型错误的参数返回稳定失败，不能由部署命令拼接或由 executor 私自读取未声明环境变量。

### 3.3 输入和 freshness

`InputContract` 必须逐项定义：

- `input_id`、`InputKind`（`TABLE`、`FILE`、`EXTERNAL_API`、`UPSTREAM_PIPELINE`）和精确 `source`；
- `required_fields` 与可执行的 `structure_check`；
- 目标日期语义和输入缺失错误码；
- 一项 `FreshnessContract`，包括检查对象、目标日期语义、最大交易日滞后、非交易日行为、稳定错误码和可执行 `checker`；
- 上游 Pipeline 输入的 `upstream_pipeline_id`，且它必须同时出现在 `dependencies`。

检查必须能回答“为什么当前允许运行”。表、文件和外部 API 不得只写模糊的数据库名称或自然日比较。freshness 检查失败会阻止 executor 运行，并以对应稳定错误码返回失败。

来源完整性必须建立在该来源可证明的事实上，不能通过无业务依赖的表或尚未进入现有生产链的 Pipeline 虚构覆盖集合。当前 `market_daily_update` 的 Tushare `daily(trade_date)` 响应可验证非空、必需字段和目标日期一致性，但没有权威总数或逐证券停复牌语义。因此它不能仅凭 `stock_info + suspend_d` 区分正常停牌缺席与接口部分返回，也不得为此依赖当前尚未调度的 `suspend_d_ingest`。该边界必须在 Contract 中明确；未来只有在接口提供可核验总数/状态，或已有生产链中存在经单独验收的真实上游数据依赖时，才能收紧该 Pipeline 的覆盖检查。

### 3.4 输出、完成与质量

`OutputContract` 必须定义：

- `output_id`、`physical_resource`、配置化 `location` 和表/文件 `object_name`；
- `unique_key`、`WriteMode`、目标日期语义和 `allow_empty`；
- `CompletionContract`：完成标记、错误码和可执行检查；
- 至少一个可执行质量检查。

写入模式只能是 `APPEND`、`UPSERT`、`REPLACE_TARGET_DATE`、`REPLACE_TARGET_RANGE`、`FULL_REBUILD` 或 `READ_ONLY`。成功 executor 必须报告每个输出的 `OutputResult`；所有输出必须完成，行数不能为负，`rows_written` 必须和总指标一致。禁止空结果的输出若写入零行，统一返回 `EMPTY_OUTPUT_NOT_ALLOWED`。进程退出码本身不是数据完成依据。

### 3.5 依赖和物理锁

`dependencies` 是业务数据先后关系，例如 `market_daily_update -> adj_factor_daily -> system_b_state_readiness`。不得以 cron 错峰替代依赖，也不得因两个任务访问同一数据库就建立伪业务依赖。

`resource_locks` 表示物理冲突，与依赖分离。受管理的资源键和排他锁固定为：

| 物理资源键 | 统一排他锁 | 配置位置 |
| --- | --- | --- |
| `quant_db` | `quant_db_writer` | `settings.paths.duckdb_path` |
| `system_b_episode_db` | `system_b_episode_writer` | System B episode 数据库配置 |
| `system_b_pools_db` | `system_b_pools_writer` | System B pools 数据库配置 |

任何写入同一物理 DuckDB 的合同必须使用上述同一锁。公共校验会拒绝声明受管理资源却缺失对应锁的写任务。不得按表、模块或 Pipeline 名称创建不同写锁绕过互斥。

正式 Contract 的写入声明只允许使用对应物理文件的数据库级 canonical writer lock；例如 `quant.db` 的写入统一使用 `quant_db_writer`。`resource_reads` 用于共享读取声明，可以使用 `duckdb://<database>#<object>` 表级资源；表级资源不得出现在正式 Contract 的 `resource_locks` 中，也不能替代数据库级写锁。通用 Orchestration 锁引擎仍为兼容 JSON Definition 支持表级资源锁，但那不是正式 Contract 的生产写入规则，本轮不改造该通用引擎。

### 3.6 幂等、事务与失败

`IdempotencyContract` 必须给出幂等键、同一目标重复执行语义、已有目标处理、失败重跑方式、是否使用 staging 和原子替换边界。`TransactionContract` 必须给出模式、边界和失败半成品可见性。

写任务只能使用 `DATABASE_TRANSACTION` 或 `STAGING_ATOMIC_REPLACE`。不能原子直写时，必须遵循：

```text
计算或下载
→ staging
→ 结构/质量校验
→ 原子提交或替换
→ completion 检查
```

同一目标日期重跑不得重复追加、破坏历史、把残留视为成功或产生冲突有效版本。任何参数、配置、结构、freshness、API、局部资产、空输出、事务、completion、质量、timeout、依赖、锁或未处理异常失败，必须以非零退出并产生 `FAILED` 的业务结果或编排结果。允许的无数据情况只能返回 `NOOP`，且必须有明确的 `noop_reason`。

### 3.7 性能

`PerformanceBudget` 是强制字段：

| 字段 | 含义 |
| --- | --- |
| `normal_budget_seconds` | 正常日常运行预算。 |
| `warning_threshold_seconds` | 低于或等于正常预算的告警阈值。 |
| `hard_timeout_seconds` | runtime 强制 timeout，不能低于正常预算。 |
| `benchmark_scope` | 标准基准数据范围和扫描范围。 |
| `baseline_source` | 可重复基准、真实历史运行或同规模生产证据的位置。 |

基线来源不得是猜测。`PipelineResult` 应记录业务代码实际能够可靠取得的耗时、阶段、读取/写入行数、资产/日期、数据库写耗时和 retry 等指标；可可靠获取时还记录 RSS、临时磁盘、API 请求和 batch。通用门禁只验证指标的类型、有限性、非负性、输出行数一致性和预算字段的机械关系，不替代专项性能基线。超过阈值会在结构化 diagnostics 中留下性能告警；timeout 仍由 runtime 硬性执行，不能通过放宽 timeout 掩盖算法、扫描范围、全量重算、数据库使用或外部接口瓶颈。

大批量操作必须优先集合计算，明确扫描范围，控制重复读取，不得无依据逐行 Python 循环、按资产反复打开数据库或复制整表。性能测试必须覆盖声明的标准数据范围。

### 3.8 `PipelineResult` 与 `JobResult`

`PipelineResult` 是机器判定的执行结果，字段为：

| 字段 | 含义 |
| --- | --- |
| `run_id`、`pipeline_id` | runtime 身份与稳定 Pipeline 身份。 |
| `status` | `SUCCESS`、`FAILED` 或允许的 `NOOP`。 |
| `target_window` | `target_date` 或 `start_date`/`end_date`。 |
| `started_at`、`completed_at`、`duration_seconds` | 完整运行时间。 |
| `attempt` | runtime attempt。 |
| `metrics` | 行数、资产/日期数、阶段、写入、API、batch、资源等指标。 |
| `outputs` | 每项物理输出及完成情况。 |
| `input_checks`、`freshness_checks`、`completion_checks` | 可审计检查结果。 |
| `performance` | 当前耗时相对警告/正常预算的结果。 |
| `diagnostics` | 稳定错误码、等级、无凭据消息和结构化细节。 |
| `noop_reason` | 仅允许 NOOP 时的明确原因。 |

`PipelineResult` 是 Pipeline 业务层的结构化结果，正常的正式 Contract 执行会把它作为 JSON 载荷持久化到独立 runtime SQLite 的 `job_result` 表。`JobResult` 是 Orchestration 对每个 Job Run 的持久化结果记录，主键为 `run_id`，与 `job_run` 的最终状态、日志摘要和 lease 历史关联；二者不能混称。`PipelineResult.status` 使用 `SUCCESS`、`FAILED` 或 `NOOP`；`JobRun.status` 使用 `SUCCESS`、`FAILED`、`TIMED_OUT`、`CANCELLED` 或 `SKIPPED`，编排合成的结果载荷另外记录相同的 `terminal_status`。

每一个终态 `JobRun` 必须恰好有一条 durable `JobResult`，终态转换、结果写入和 lease 释放在同一事务中完成。正常返回真实业务结果时保留 `PipelineResult` 载荷；timeout、cancel、scheduler skip、stale recovery、Runner 异常或其他没有业务结果的路径由 Orchestration 合成结果，至少包含：

```json
{
  "result_type": "orchestration",
  "source": "orchestration",
  "run_id": "RUN_ID",
  "status": "TIMED_OUT",
  "terminal_status": "TIMED_OUT",
  "error_code": "ORCHESTRATION_TIMEOUT",
  "error_summary": "...",
  "business_result": null
}
```

timeout、cancel 和 skip 即使 executor 曾经产生载荷，也不得伪造业务成功结果；运行时失败但没有业务载荷同样使用编排结果。错误码只需满足 `ERROR_CODE_PATTERN = ^[A-Z][A-Z0-9_]*$`，本规则不建设错误码注册中心。

正式 Contract 在 `qrp-atlas-jobs serve` 的线程 worker 内执行，不使用子进程或结果文件 IPC。`orchestration/results/<run_id>.json` 仅属于兼容 argv Definition 的一次性 executor-to-runner IPC；读取和持久化尝试后立即删除，SQLite `job_result` 才是需要查询和保留的结构化结果。

## 4. 统一生命周期

所有正式 Pipeline 的外层顺序固定为：

```text
Scheduler scan：匹配 cron，处理依赖/overlap 预判，并创建 PENDING、BLOCKED 或 SKIPPED run
→ Runner 在原子 claim transaction 中校验 Definition/version、overlap，取得 resource lease 并标记 RUNNING
→ serve 线程 worker 创建/传递同一个 ExecutionControl，调用正式 Contract 的进程内 executor
→ executor 解析目标日期和参数，执行输入结构、freshness、业务、completion 与质量检查
→ executor 在 Contract 声明的事务或 staging 边界内写入业务输出
→ Runner/Store 在同一事务中持久化终态 JobRun 与恰好一条 JobResult，并释放 lease
```

`SKIPPED` 不进入 executor，在 scheduler 创建时直接获得编排合成的 `JobResult`。只有兼容 argv Definition 才走受控子进程分支；正式 Contract 不通过子进程、临时结果文件或业务 argv 完成上述生命周期。

正式命令为：

```bash
qrp-atlas-jobs validate-contracts
qrp-atlas-jobs list-contracts
qrp-atlas-jobs run job_id
qrp-atlas-jobs run job_id --trade-date 2026-07-29
qrp-atlas-jobs run job_id --set batch_size=500
```

`run` 不是绕开 Orchestration 的脚本快捷方式：它创建独立 Job Run 后再使用既有 claim、锁、heartbeat、timeout 和结果落库。排程场景使用 `serve --contract-selections ...`，由 Pipeline adapter 生成 Job Definition；部署不需要也不得手写业务 argv。

## 5. 原子任务和顶层 DAG

`PipelineKind.ATOMIC` 承担一个可独立验收的业务写入或只读检查。`PipelineKind.DAG` 只声明必要原子 Pipeline 依赖与整体完成状态，不能重复原子业务写入。校验要求 DAG 至少有一个依赖，且不能声明写输出。

顶层日常 DAG 成功的含义是 runtime 已确认其每一项必需原子依赖成功。它不是 shell 串联多个命令，也不是一个隐藏业务总脚本。所有依赖、目标日期一致性与下游 completion 仍由各原子合同明确声明和验证。

## 6. 注册、模板与当前边界

新增正式 Pipeline 的源码模块应：

1. 定义 `PipelineContract`、日期策略、输入/输出检查和 executor；
2. 用 `register_pipeline(contract)` 注册；
3. 将模块名显式加入 `pipeline.contract_catalog.CONTRACT_MODULES`；
4. 为该合同补充公共验收测试和性能证据；
5. 通过 `qrp-atlas-jobs validate-contracts` 后，才可在单独的部署选择清单中引用它。

`qrp_atlas.pipeline.examples.contract_template` 是无 I/O、无真实凭据、无部署选择的参考模板。它只演示 Contract、executor、NOOP、测试和 runtime 结果接口，不能被当作生产数据任务，也绝不加入默认 catalog。当前默认 catalog 显式导入 `qrp_atlas.pipeline.market_data_contracts`、`qrp_atlas.pipeline.cninfo_contracts`、`qrp_atlas.pipeline.irm_qa_contracts` 和 `qrp_atlas.pipeline.membership_contracts`，共包含十条通过正式验收的 Contract：六条基础市场数据 Contract、`cninfo_research_visit_ingest`、`irm_qa_incremental`、`industry_membership_ingest` 和 `index_component_ingest`。IRM 使用 P5W 最新 feed、`pid` 幂等追加、`quant_db_writer` 和 `overlap_policy=FORBID`；其计划日期只是请求观察标签，不能被误读为 provider 日期过滤。成员关系 Contract 使用显式范围、`revision_id` 幂等追加、`quant_db_writer` 和 `overlap_policy=FORBID`；Tushare 没有可靠的 total/page 证据，结果保留这一完整性边界。其他既有 Pipeline 仍不会被默认 CLI 发现或进入 QRP 正式调度。源码 catalog 可发现不等于部署启用：本仓库没有为这些 Contract 新增部署选择、Production Definition、systemd 或 Hermes 变更。

## 7. 公共测试规则

每个新增或改造后的 Pipeline 至少要有下列公共测试；测试只能使用 `tmp_path`、临时 SQLite/DuckDB、fixture 和 mock 外部接口：

1. 身份和注册：唯一 `pipeline_id`、正式 catalog 可发现、统一入口可调用、无重复依赖、DAG 无环。
2. 参数和日期：自动业务日期、显式覆盖、非交易日、非法参数非零失败、无部署层业务参数依赖。
3. 输入和 freshness：正常输入、表/文件/字段缺失、过期数据、上游失败、外部 API 不完整。
4. 输出和 completion：正常、允许/禁止空结果、部分输出、重复键、completion 缺失、事务中断和 staging 失败。
5. 幂等和恢复：同日重跑、失败后重跑、中断恢复、无重复写入、历史不丢失、无半成品可见。
6. Runtime 集成：scheduler record、依赖阻塞、claim、overlap、资源锁、timeout、retry、heartbeat、lease 回收、结果缺失/损坏/身份不一致、状态不一致、持久化失败、旧 Definition 兼容和结果 SQLite 落库。
7. 性能：标准规模基准、数据量指标、耗时记录、扫描范围、复杂度不退化和预算超限检测。

可复用的基础工具在 `qrp_atlas.pipeline.testing.ContractTestHarness` 和 `assert_contract_result_matches_context`。模板专项测试在 `tests/pipeline/test_pipeline_contract.py`，并且不连接市场数据库或外部服务。

通用门禁只负责可机械验证的结构约束：身份和字段形状、资源声明、错误码格式、指标类型/有限性/非负性、输出结果对应关系及预算字段关系。事务边界、幂等、原子性、分页完整性、来源完整性、freshness 的业务含义和真实性能基线，必须由各 Pipeline 的专项测试与证据证明；通用门禁不以非空描述或错误码格式替代这些证明。

## 8. PR 强制门禁

任何新增 Pipeline 或生产语义改动的 PR 必须同时提交：

- `PipelineContract` 和正式 executor；
- 参数/日期策略、输入/输出、freshness、completion、质量检查；
- 依赖、物理锁、幂等、事务和失败恢复说明；
- timeout、retry、性能预算、标准规模与测量基线；
- 公共契约测试、必要的 runtime 集成测试和性能证据；
- 本文档或相关 Pipeline 文档更新。

缺少其中任何一项不得合并。只新增脚本、cron、shell、成功日志、退出码、无 completion、无失败退出、无幂等说明或无性能证据，均不是可接受交付。

提交前至少执行：

```bash
.venv/bin/pytest -q tests/pipeline/test_pipeline_contract.py
.venv/bin/pytest -q tests/pipeline
.venv/bin/pytest -q
.venv/bin/python -m compileall -q src tests
git diff --check
```

## 9. 既有 Pipeline 的后续重构

后续重构必须逐条按本合同完成，且不改变已封板业务规则：

1. 从源码提取稳定身份、日期、输入输出和现有真实完成语义；
2. 补足 freshness、质量、幂等、事务、失败传播、物理锁和性能测量；
3. 用临时数据库和 mock 构建完整公共测试，不把业务规则移入部署层；
4. 通过合同验收后加入 source catalog，但不单独启用生产选择；
5. 全部计划内生产 Pipeline 完成、全量合同测试和生产就绪检查通过；
6. 再一次性创建部署选择、切换调度权并替换 Hermes。

在第 5 步前，Hermes 仍是现有生产的唯一权威。任何单条已完成源码重构的 Pipeline 都不得提前进入 QRP 正式排程。
