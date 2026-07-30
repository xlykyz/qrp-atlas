# Pipeline 基础运行框架

## 1. 本轮职责边界

```text
qrp-atlas-api
  提供应用 API；不启动调度循环。

qrp-atlas-jobs serve
  在单一长驻进程内恢复 stale run、按 cron/timezone 创建 PENDING 或 BLOCKED 记录，并由线程 worker 认领和执行 Job。
```

现阶段 Hermes 仍是唯一生产调度权威。包内默认 definition manifest 为空，`run-pending` 要求显式 `--definitions`；因此新 CLI 不会默认执行任何旧 Pipeline。本轮没有接入 Hermes Job、旧业务脚本或生产 DuckDB。

## 2. 运行库与定义

运行元数据位于独立 SQLite，不复用 `quant.db`、Hermes `scheduler.db` 或认证 PostgreSQL。路径从 `AppSettings` 派生：

```text
QRP_JOB_RUNTIME_DIR
  默认 QRP_DATA_DIR/runtime/job

<runtime-dir>/job_runtime.sqlite3
<runtime-dir>/logs/<run-id>.stdout.log
<runtime-dir>/logs/<run-id>.stderr.log
```

SQLite 启用 WAL、外键和 busy timeout。定义是 Git 中 JSON manifest，运行库只保存其 `job_id` 和 `definition_version` 快照，不是定义唯一真值。加载 manifest 时会校验缺失依赖和完整有向图循环；例如 `A -> B -> C -> A` 会拒绝为：`pipeline dependency cycle detected: A -> B -> C -> A`。校验失败时 Scheduler 不会扫描或写入 run。

## 3. 状态、认领与恢复

允许的状态为：`PENDING`、`BLOCKED`、`RUNNING`、`SUCCESS`、`FAILED`、`TIMED_OUT`、`CANCELLED`、`SKIPPED`。

`PENDING` 可变为 `BLOCKED`/`RUNNING`/取消；`RUNNING` 只能完成为成功、失败、超时或取消；终态不原地重写。重试会创建新 attempt，保留原 run 的日志、退出码和错误。`FORBID` Job 的多个 attempt 可以同时处于 `PENDING`，但最终只能有一个进入 `RUNNING`。

Runner 认领在单个 `BEGIN IMMEDIATE` transaction 内完成：确认 run 存在且为 `PENDING`、确认 Job id 与 definition version、检查 `FORBID` 的其他 `RUNNING` attempt、检查所有资源锁、插入全部锁、更新目标 run 为 `RUNNING`。因此 Scheduler 的预检查仅用于提前展示 `BLOCKED`，不是并发正确性的唯一保障；`ALLOW` Job 可以并发，但相同资源锁仍会阻止认领。

数据库级写任务继续声明 `quant_db_writer` 等受管理锁；需要表级并发时使用 `duckdb://<database>#<object>`，同库不同表可并行、同表读写冲突。资源锁保存在 SQLite，包含 `owner_run_id`、heartbeat 和 lease 到期时间，不能只依赖进程内锁。

每次 Scheduler scan 都会在 cron 扫描前执行 stale recovery：超过阈值的 `RUNNING` 记录变为 `FAILED`，保留 `started_at`、日志路径和已有指标，写入 `stale heartbeat recovery`，删除其资源锁；不会自动创建 retry。过期 lease 也同时回收。该步骤幂等，避免人工 cleanup 被遗漏而永久阻塞后续调度。

默认配置为 heartbeat 5 秒、lease 30 秒、stale threshold 180 秒，且命令会强制：

```text
stale_after_seconds > lease_seconds > heartbeat_interval_seconds > 0
```

部署时 Scheduler 和 Runner 必须使用同一组 heartbeat/lease 参数；stale threshold 要保留足够余量，不能设置成会误杀正常长任务的激进值。

兼容 argv Definition 的 Runner 在短周期监督业务进程，而非仅以完整 timeout 等待。正式 Contract 则在 `serve` 进程的线程 worker 中调用 executor，并由独立 DuckDB connection 执行。heartbeat 返回 `False`、SQLite heartbeat 异常或 heartbeat 线程异常退出都会通过线程安全失败事件通知 Runner；argv 兼容任务会终止完整进程组，正式 Contract 记录受限错误摘要，随后释放资源 lease。

## 4. Scheduler cursor 与 catch-up

运行库的 `scheduler_cursor` 表保存：

```text
scheduler_id PRIMARY KEY
last_scanned_at
created_at
updated_at
```

第一版默认 `scheduler_id=default`。首个 scan 没有 cursor 时只处理调用时所在的 UTC 分钟，成功后记录该分钟。后续 scan 处理：

```text
last_scanned_minute < minute <= current_minute
```

每个 UTC 分钟都会转换到 definition timezone 后匹配 cron；记录的 `scheduled_at` 保持时区感知 UTC，因此 DST 的重复本地小时不会生成 naive datetime 或重复主键。创建本区间 run 与 cursor 推进在一个 SQLite transaction 内提交。任一异常不会推进 cursor；已插入的记录可由唯一键安全重放。

默认最大补偿窗口为 360 分钟，可由 `scan --max-catch-up-minutes` 显式调整。超过上限时 scan 返回 `CATCH_UP_LIMITED`，包含原请求起点与实际 `scan_start_at`，再推进到当前分钟；它不会静默回扫数月历史，也不会静默隐藏被限幅的旧计划。

systemd timer 的 `Persistent=true` 只负责错过唤醒后重新调用 service；QRP cursor 才负责识别两次调用之间遗漏的具体 cron 分钟，两者互补而非重复。

## 5. CLI

```bash
qrp-atlas-jobs init
qrp-atlas-jobs validate-definitions --definitions path/to/pipelines.json
qrp-atlas-jobs list-definitions --definitions path/to/pipelines.json
qrp-atlas-jobs scan --definitions path/to/pipelines.json \
  --max-catch-up-minutes 360 --heartbeat-interval-seconds 5 \
  --lease-seconds 30 --stale-after-seconds 180
qrp-atlas-jobs run-pending --definitions path/to/pipelines.json \
  --heartbeat-interval-seconds 5 --lease-seconds 30
qrp-atlas-jobs status
qrp-atlas-jobs cleanup --stale-after-seconds 180
qrp-atlas-jobs retry RUN_ID --definitions path/to/pipelines.json
```

`scan` 只创建运行记录。未带 `--run-id` 的 `run-pending` 在没有 `PENDING` run 时输出 `{"status":"IDLE","reason":"NO_PENDING_RUN"}` 并以 exit code 0 退出。显式 `--run-id` 的不存在记录、非 `PENDING` 状态、definition 缺失或版本不匹配，以及 overlap/resource lock 认领失败都会输出结构化的具体原因并以非零退出。

兼容 Definition 的 Runner 使用 argv 而非 shell 字符串，分离 stdout/stderr，受控继承环境，timeout 后结束完整进程组，并记录 wall time、子进程 user/system CPU、leader RSS 采样和 exit code。正式 Contract 不生成永久 stdout/stderr 文件，只保留结构化结果和 JSONL 审计记录。Stage API 可由业务代码提供 `input_rows`、`output_rows` 与 metadata。

## 6. 进程托管边界

外部 supervisor 或操作系统服务配置不属于本轮交付；本轮只保证 `qrp-atlas-jobs serve` 进程自身的循环稳定性、信号停止和 Job 恢复语义。生产服务注册与自动拉起另行处理。

后续若人工安装后需要回滚，先停止并 disable 两个 timer/service，恢复由 Hermes 负责的调度，再保留 SQLite 运行库和日志用于审计。不要通过删除运行库掩盖失败证据，也不要修改 Hermes Job 来完成本轮回滚。
