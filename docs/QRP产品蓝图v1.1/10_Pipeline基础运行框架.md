# Pipeline 基础运行框架

## 1. 本轮职责边界

```text
qrp-atlas-api
  提供应用 API；不启动调度循环。

qrp-atlas-pipeline-scheduler
  读取 Git 版本化定义，按 cron/timezone 创建 PENDING 或 BLOCKED 运行记录；不执行命令。

qrp-atlas-pipeline-runner
  认领一个 PENDING 记录，获取资源 lease，并以 argv 执行命令；不计算到期计划。
```

现阶段 Hermes 仍是唯一生产调度权威。包内默认 definition manifest 为空，`run-pending` 要求显式 `--definitions`；因此新 CLI 不会默认执行任何旧 Pipeline。

## 2. 运行库与配置

运行元数据位于独立 SQLite，不复用 `quant.db`、Hermes `scheduler.db` 或认证 PostgreSQL。路径从 `AppSettings` 派生：

```text
QRP_PIPELINE_RUNTIME_DIR
  默认 QRP_DATA_DIR/runtime/pipeline

<runtime-dir>/pipeline_runtime.sqlite3
<runtime-dir>/logs/<run-id>.stdout.log
<runtime-dir>/logs/<run-id>.stderr.log
```

SQLite 启用 WAL、外键和 busy timeout。定义是 Git 中 JSON manifest，运行库只保存其 `pipeline_id` 和 `definition_version` 快照，不是定义唯一真值。

## 3. 状态与恢复

允许的状态为：`PENDING`、`BLOCKED`、`RUNNING`、`SUCCESS`、`FAILED`、`TIMED_OUT`、`CANCELLED`、`SKIPPED`。

`PENDING` 可变为 `BLOCKED`/`RUNNING`/取消；`RUNNING` 只能完成为成功、失败、超时或取消；终态不原地重写。重试会创建新 attempt，保留原 run 的日志、退出码和错误。

所有主 DuckDB 写任务的未来 definition 必须声明 `quant_db_writer`。资源锁保存在 SQLite，包含 `owner_run_id`、heartbeat 和 lease 到期时间。Runner 仅在一个 transaction 中同时认领 run 和获得全部锁；清理命令会回收过期锁，并将过期 heartbeat 的 `RUNNING` 记录标记为失败。

## 4. CLI

```bash
qrp-atlas-pipeline init
qrp-atlas-pipeline validate-definitions --definitions path/to/pipelines.json
qrp-atlas-pipeline list-definitions --definitions path/to/pipelines.json
qrp-atlas-pipeline scan --definitions path/to/pipelines.json
qrp-atlas-pipeline run-pending --definitions path/to/pipelines.json
qrp-atlas-pipeline status
qrp-atlas-pipeline cleanup --stale-after-seconds 120
qrp-atlas-pipeline retry RUN_ID --definitions path/to/pipelines.json
```

`scan` 只创建运行记录。Runner 使用 argv 而非 shell 字符串，分离 stdout/stderr，受控继承环境，timeout 后结束完整进程组，并记录 wall time、子进程 user/system CPU、leader RSS 采样和 exit code。Stage API 可由未来改造后的业务代码提供 `input_rows`、`output_rows` 与 metadata；本轮没有接入旧 Pipeline。

## 5. systemd 示例与回滚

`deploy/qrp-atlas-pipeline-*.example` 仅为后续人工部署模板，未安装、启用或启动。示例中的 scheduler 和 runner 分别以一分钟 scan 与一分钟一个 pending run 工作；它们需要先将受审 definition manifest 部署到 `/etc/qrp-atlas/pipeline-definitions.json`。

后续若人工安装后需要回滚，先停止并 disable 两个 timer/service，恢复由 Hermes 负责的调度，再保留 SQLite 运行库和日志用于审计。不要通过删除运行库掩盖失败证据，也不要修改 Hermes Job 来完成本轮回滚。
