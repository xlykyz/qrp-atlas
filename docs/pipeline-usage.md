# Pipeline 使用手册

## 目的与边界

Job Orchestration 是 QRP Atlas 的独立日常任务执行系统。Pipeline Contract 通过 adapter 接入通用 Job Definition，生成持久化 Job Run，按依赖关系执行，并在仓库外保留简洁的最终结果审计日志。

本手册的正式入口是：

```bash
qrp-atlas-jobs
```

当前版本不安装服务、不启用操作系统定时器、不访问生产数据库，也不替代任何现有生产调度。所有示例应先指向测试或受控运行时目录。

## 核心对象

```text
Pipeline Contract / Job Definition
        |
        +-- dependencies, target schedule, timezone, locks, retry policy
        |
Job Run (one Definition execution attempt)
        |
        +-- atomic claim, RUNNING heartbeat, final result and task summary
        |
Task execution
        |
        +-- formal Contract: serve-process thread + independent DuckDB connection
        +-- compatibility Definition: argv subprocess (stdout/stderr is non-formal diagnostic output)
```

- **Contract**：源码拥有的业务语义，包括输入、输出、业务日期解析、幂等性、事务、依赖、读写资源、资源锁和人工执行许可。
- **Job Definition**：Orchestration 可执行定义，包含 `job_id`、定义版本、启用状态、cron schedule、时区、依赖、`resource_reads` 和 `resource_locks`。普通 JSON Definition 还包含 argv；正式 Pipeline Contract Definition 绑定进程内 executor，不再通过 argv 子进程执行。
- **Dependency**：有向无环图中的上游任务。计划按拓扑顺序排列；上游未成功时下游保持 `BLOCKED`，不会被错误执行。
- **Job Run**：每次尝试的持久化记录。单个 Definition 是当前 Orchestration 的最小任务单位，因此一个 Job Run 同时承载该任务的认领、状态和结果摘要；业务代码如需细分阶段，可使用 Stage Run API 记录阶段输入/输出行数。
- **Task execution**：Runner 原子领取 Job Run、获取资源 lease、发送心跳并写入最终状态。正式 Contract 在 `serve` 进程的线程 worker 内调用 executor；普通兼容 Definition 才以 argv 启动子进程。

运行状态为 `PENDING`、`BLOCKED`、`RUNNING`、`SUCCESS`、`FAILED`、`TIMED_OUT`、`CANCELLED` 和 `SKIPPED`。终态不可回退；重试永远创建新的 attempt，保留失败证据。

## 运行配置

运行状态库与日志必须在源码仓库外。推荐环境文件只包含路径和环境变量名，不放凭据值：

```dotenv
QRP_DATA_DIR=/srv/qrp-atlas/data
QRP_JOB_RUNTIME_DIR=/srv/qrp-atlas/runtime/job
QRP_LOG_DIR=/srv/qrp-atlas/logs
```

最终审计日志固定在：

```text
$QRP_LOG_DIR/job/job-results-YYYY-MM-DD.jsonl
```

运行状态库位于：

```text
$QRP_JOB_RUNTIME_DIR/job_runtime.sqlite3
```

命令支持受控覆盖，主要用于临时验收环境：

```bash
qrp-atlas-jobs \
  --runtime-dir /tmp/qrp-job-runtime \
  --result-log-dir /tmp/qrp-job-logs/job \
  init
```

Runtime 会拒绝将最终结果日志写到源码仓库、当前工作目录回退位置或一个普通文件。日志目录不存在时会创建；无法创建或写入时，`run`/`run-pending`/`serve` 在执行前失败。

## 注册 Pipeline

正式日常 Pipeline 应以源码 Contract 注册：

1. 新建或修改 `PipelineContract`，声明稳定 `pipeline_id`、版本、输入、输出、依赖、读写资源、幂等性、事务和性能限制。
2. 用 `TargetDatePolicy` 实现业务日期解析。市场日、休市日和显式目标日期均由该策略决定，调度器不猜测交易日逻辑。
3. 在 Contract 中声明 `resource_reads`（共享只读资源）和 `resource_locks`（排他写资源）；同一资源不能同时出现。业务 executor 不得自行再启动写同一 DuckDB 的子进程。
4. 在 Contract 中声明 `manual_execution_allowed`；默认允许人工执行。需要禁止时设为 `False`。
5. 将 Contract 加入 `contract_catalog`，并运行 `validate-contracts`。
6. 为目标环境创建 deployment selection。它只能选择 Pipeline、开关自动执行和 schedule，不能携带数据库路径、动态日期、argv、依赖、锁或失败处理规则。
7. 用临时数据库、mock 外部接口和临时日志目录覆盖成功、失败、重试和恢复路径。

检查已注册 Contract：

```bash
qrp-atlas-jobs list
qrp-atlas-jobs show market_daily_update
qrp-atlas-jobs validate-contracts
```

`list` 与 `show` 默认读取源码注册 Contract 适配出的 Job Definition；`list-contracts` 用于查看业务 Contract 详情。它们不启动任务，也不会输出环境变量值。

## 自动调度配置

deployment selection 是仅含选择信息的 JSON：

```json
{
  "schema_version": 1,
  "pipelines": [
    {
      "pipeline_id": "market_daily_update",
      "enabled": true,
      "schedule": "15 16 * * 1-5"
    }
  ]
}
```

它通过 `--contract-selections` 加载。Contract 的业务日期策略和依赖仍来自源码。Contract selection 统一按 `Asia/Shanghai` 解读 cron；普通 JSON Definition 则必须显式声明 `timezone`。

`schedule` 是目标执行时间的 cron 表达式，例如 `15 16 * * 1-5` 表示上海时间工作日 16:15。Runtime 只在到达该时间后创建运行；不会提前运行。休市日是否实际产生业务结果由 Pipeline 自身的业务日期策略处理。

查看目标日期的拓扑计划而不执行：

```bash
qrp-atlas-jobs plan market_daily_update \
  --contract-selections /etc/qrp-atlas/pipeline-selection.json \
  --target-date 2026-07-30
```

输出是一行一个任务的 JSON，按上游到下游顺序给出 schedule、时区、定义版本和该目标日期对应的计划时刻。

普通 argv Definition 适用于受控测试或非 Contract 任务，格式如下：

```json
{
  "schema_version": 1,
  "definitions": [
    {
      "job_id": "example_daily",
      "name": "Example daily task",
      "enabled": true,
      "schedule": "30 16 * * 1-5",
      "timezone": "Asia/Shanghai",
      "command": ["/opt/qrp-atlas/.venv/bin/python", "-m", "example.task"],
      "working_directory": "/opt/qrp-atlas",
      "dependencies": [],
      "timeout_seconds": 300,
      "max_retries": 1,
      "overlap_policy": "FORBID",
      "resource_reads": [],
      "resource_locks": ["quant_db_writer"],
      "manual_execution_allowed": true,
      "inherit_environment": false,
      "environment": {}
    }
  ]
}
```

`environment` 只能保存非敏感的固定值。凭据应由批准的运行环境提供，不得放入 Definition 或日志。

`resource_reads` 使用共享 lease：多个纯读任务可以并行。`resource_locks` 使用排他写 lease：写写、写读、读写同一资源都会串行。对于同一 DuckDB 的表级声明，使用 `duckdb://<database>#<object>`；同库不同表可以并行，同表读写会串行。已有的 `quant_db_writer`、`system_b_episode_writer` 等名称仍表示整个数据库级排他资源。lease 仲裁在运行状态 SQLite 的单一事务中完成，`serve` 的 worker 数量不会绕过它。

## 手动操作

先校验和查看配置：

```bash
qrp-atlas-jobs validate-definitions --definitions ./pipelines.json
qrp-atlas-jobs list --definitions ./pipelines.json
qrp-atlas-jobs show example_daily --definitions ./pipelines.json
qrp-atlas-jobs plan example_daily --definitions ./pipelines.json --target-date 2026-07-30
```

执行单个已注册 Contract，指定业务日期：

```bash
qrp-atlas-jobs --env-file /etc/qrp-atlas/runtime.env \
  run market_daily_update --target-date 2026-07-30
```

Contract 声明参数时，可用重复的 `--set NAME=VALUE` 提供人工覆盖：

```bash
qrp-atlas-jobs --env-file /etc/qrp-atlas/runtime.env \
  run market_daily_update --target-date 2026-07-30 --set batch_size=500
```

人工 Run 会把 `target-date` 和受控参数覆盖保存到 Run invocation context。服务已启动时，CLI 只提交该 Run，`serve` 领取后使用保存的原值执行；依赖任务不会继承目标任务的 `--set` 参数。对失败 Run 创建的 `retry` 会继承原 invocation context，Contract 仍会在执行前再次解析和校验参数。

执行完整依赖链：

```bash
qrp-atlas-jobs --env-file /etc/qrp-atlas/runtime.env \
  run adj_factor_daily --target-date 2026-07-30 --with-dependencies
```

对普通 Definition 使用显式 manifest：

```bash
qrp-atlas-jobs --env-file /etc/qrp-atlas/runtime.env \
  run example_daily --definitions ./pipelines.json --target-date 2026-07-30
```

`--target-date` 只把人工选择的日期传给任务；业务日期合法性仍由 Contract 校验。对于普通 Definition，该值会以 `QRP_PIPELINE_TARGET_DATE` 和 `QRP_PIPELINE_TRADE_DATE` 注入受控子进程。`--with-dependencies` 会按拓扑顺序执行；上游失败时下游保留 `BLOCKED`，不会绕过依赖。

查询状态和最新结果：

```bash
qrp-atlas-jobs status
qrp-atlas-jobs status --job-id market_daily_update --limit 20
qrp-atlas-jobs status --run-id RUN_ID --include-result
qrp-atlas-jobs latest market_daily_update --include-result
```

人工重试只允许 `FAILED` 和 `TIMED_OUT`，且受到 Definition 的 `max_retries` 限制：

```bash
qrp-atlas-jobs retry RUN_ID --contract-selections /etc/qrp-atlas/pipeline-selection.json
qrp-atlas-jobs retry RUN_ID --definitions ./pipelines.json --execute
```

第一条命令只创建新的 `PENDING` attempt；第二条显式创建并立即执行。Runtime 不会无限自动重试。

当任一 `serve` 实例持有该运行库的有效 service lease 时，`run`、`run-pending` 和 `retry --execute` 都只提交或保留 `PENDING` run，并在 JSON 输出中标记 `submitted_to_service=true`。CLI 不会自行启动业务任务；由 `serve` 在下一轮领取并执行。这避免手动 CLI 与后台服务同时写同一 DuckDB。

## 启动常驻服务

启动前先确认 definition、运行库和日志路径：

```bash
qrp-atlas-jobs validate-contracts --contract-selections /etc/qrp-atlas/pipeline-selection.json
qrp-atlas-jobs --env-file /etc/qrp-atlas/runtime.env \
  serve --contract-selections /etc/qrp-atlas/pipeline-selection.json --max-workers 4
```

普通 Definition 的等价入口：

```bash
qrp-atlas-jobs --env-file /etc/qrp-atlas/runtime.env \
  serve --definitions ./pipelines.json
```

`serve` 是长驻进程。它以短间隔等待、按 cron 扫描、执行所有到期且可领取的任务，并在空闲时不写日志。`Ctrl+C` 和 `SIGTERM` 会请求停止循环；已启动的单个任务会走 Runner 的正常收尾，随后服务释放 lease。

无依赖且读写资源不冲突的任务会在不同线程 worker 中并发执行；每个 worker 使用独立 Runtime SQLite connection，正式 Contract executor 在同一 `serve` 进程内创建自己的 DuckDB connection。资源冲突任务会保持 `PENDING`，待冲突 lease 释放后由同一服务继续领取。正式 Contract 禁止自行启动写同一 QRP DuckDB 的子进程。

受控验收可以只跑一个循环：

```bash
qrp-atlas-jobs --env-file /etc/qrp-atlas/runtime.env \
  serve --contract-selections /etc/qrp-atlas/pipeline-selection.json --once
```

`--once` 不是日常托管方式，只用于测试或诊断。

服务健康检查：

```bash
qrp-atlas-jobs --env-file /etc/qrp-atlas/runtime.env health
```

输出包括服务 owner、服务心跳、lease 截止时间、最近扫描时刻、`PENDING`/`RUNNING` 数量和最近致命/循环错误。相同运行状态库上第二个同名 `serve` 进程无法取得服务 lease；即使发生竞争，Job Run 的原子认领和资源锁仍会阻止重复执行。

## 恢复与稳定性规则

- 到期前不会创建运行。
- 服务首次启动时，会在 `--max-catch-up-minutes` 窗口中为每个 Definition 选择最近一次到期点，补偿“目标时间已过但没有运行记录”的情况。
- 已有 scheduler cursor 时，重启会精确扫描 cursor 与当前时刻之间遗漏的 cron 分钟；已存在或已成功的同一 `job_id`、业务时刻和定义版本不会重复创建有效日常运行。
- `RUNNING` 记录的心跳超时会被标记为 `FAILED`，资源 lease 被回收。它不会自动重试，必须由人工 `retry`。
- 子进程退出异常、超时、心跳失败或 Runner 内部执行异常会落到对应 Job Run 的最终失败状态，单个任务失败不会终止其他独立任务。
- 正式 Contract 使用协作式 deadline/cancellation：外部等待使用 `ExecutionControl.bounded_timeout()`，执行阶段和 DuckDB 写事务前调用 `execution_control.check()`。到期状态为 `TIMED_OUT`；租约/心跳丢失会取消当前执行并禁止进入新的写事务。Python 线程不会被强制杀死，因此 Contract 不得忽略这些检查或把不可取消的无限阻塞调用放进正式 executor。
- 普通扫描/计划局部异常被记录到服务 lease，并进入下一轮；状态库不可用、审计日志不可用或服务所有权丢失属于致命错误，`serve` 会明确退出而不是伪装健康。
- 无任务时服务以 `--poll-interval-seconds` 等待，不忙轮询，也不写“没有任务”的日志。

外部 supervisor 可以在将来按本进程 exit code 进行重启，但本轮不提供或安装 systemd、Windows Service、NSSM、WinSW、容器或其他操作系统托管配置。

## 审计日志

每个完成的任务写入一行 JSONL，包含：

- `recorded_at`、`job_id`、`definition_version`、`job_run_id`（CLI 也显示同值的 `run_id`）；
- `business_date`（Contract structured result 可用时）、计划/实际开始/结束时间；
- 最终状态和毫秒耗时；
- 任务摘要、输出 ID 与行数摘要；
- 失败时的稳定错误类型和经过截断/敏感键脱敏的错误摘要。

日志不记录完整环境变量、Definition 环境值、凭据、输入行、SQL、循环心跳或空闲轮询。正式 Contract 不创建或永久保存 stdout/stderr 文件；成功运行只保留结构化结果与 JSONL 审计记录，失败只保留长度受限且脱敏的错误摘要。普通 argv 兼容入口可以保留隔离 runtime 目录中的 stdout/stderr，但它不是迁移后日常 Pipeline 的正式执行入口。

## 测试方法

所有 Job Orchestration 验证必须使用临时运行库和临时日志目录：

```bash
.venv/bin/pytest -q \
  tests/orchestration/test_foundation.py \
  tests/orchestration/test_regressions.py \
  tests/orchestration/test_structured_results.py \
  tests/orchestration/test_service.py
```

覆盖范围包括 Definition 唯一性与环检测、Contract 校验、拓扑顺序、手动依赖链、目标时间前后行为、首次补偿、重启去重、运行认领、孤儿恢复、失败隔离、显式重试、服务 lease、活动服务时的 CLI 提交、读写资源冲突仲裁、无冲突任务并发、CLI 查询以及仓库外的静默结果审计日志。

## 当前不支持

- 真实生产数据库验证、生产任务运行或 Hermes 迁移；
- 开机启动、进程自动重启、systemd/Windows Service/NSSM/WinSW 安装；
- 多机器 worker、消息队列、Kubernetes 或分布式调度；
- Web 调度控制台、DAG 图编辑器、通用工作流 DSL、日志采集/告警平台；
- 自动无限重试、无声明的业务日期猜测，或部署配置覆盖 Contract 业务语义。
