# Pipeline Lake 工程候选

日期：2026-11-02。基线：`6d777ec`；任务分支：`feature/v1.1-pipeline-lake`。
本轮交付隔离工程候选，不代表生产切换。旧 API 默认连接旧 DuckDB，旧调度器不变。

## 覆盖范围

| 范围 | 本轮实现 |
| --- | --- |
| 核心事实 | trading_calendar、stock_info、历史证券身份 companion、suspend_d、daily_market_snapshot、daily_basic、adj_factor_changes |
| Raw | 逐页 SDK Arrow IPC 快照，未知字段、字段类型和原始值保留；sha256、观察时间、脱敏参数、完整性、分页证据 |
| 存储 | DuckLake + PostgreSQL catalog 主路径；独立 core/extension catalog 与文件域 |
| 发布 | 独立 staging、最终类型校验、分区锁、CAS、发布记录与数据同事务、按发布身份恢复 |
| 读取 | 固定快照 DataPortal；按数据集固定 publication manifest；禁止直接 glob DuckLake 文件 |
| 扩展 | 显式 allowlist，滚动均值插件、复用原算法的 System B state 插件 |
| 编排 | Dagster 独立 core/extension code locations；CLI 单独可用，无嵌套旧 claim/lease |
| 消费 | `/api/daily` 和 `/api/daily/dates` 显式开关；默认 legacy，无失败后静默回退 |
| 迁移 | 旧库只读导入、dry-run、业务键/日期/字段对照、幂等、legacy 证据标注 |
| 恢复 | PostgreSQL pg_dump/pg_restore + 文件备份；指定历史快照实际查询验收 |

ETF、指数、财务、热度、研报、互动问答仍在旧链路。Episode/三池未迁移，边界见后文。
没有修改冻结的 `web/`，没有生产补数、数据修复、服务重启或路由切换。

## 实现边界

`contracts/lake_core.py` 复用既有 TableSchema 和映射。日线成交量、金额单位沿用旧清洗；
证券名称与 ST/涨跌停规则通过固定股票快照及既有规则准备。缺失行情不复制前日。
复权保存目标日完整因子快照，表字段复用 `adj_factor_changes`；读端仍按不晚于目标日的因子使用。

SDK 返回快照不是原始 HTTP 字节。分页采用 limit/offset 与短页结束约定，检测重复页及页数上限，
再按目标日期、业务键和证券覆盖校验。第三方是否对所有接口实现该约定必须分别验收。
2026-11-02 对用户提供的第三方地址进行了小请求：index_basic 返回 5 行，daily 两个 offset
各返回 2 条不同记录。没有用这些请求修改现有数据库，也不将小请求等同于完整供应商验收。

发布前先在独立 DuckDB 中强制转换契约类型，并在转换后再次检查键、非空和分区。
同分区覆盖必须提供计算开始前取得的 `expected_previous`；同一幂等身份重试返回原发布。
全表快照使用全表锁，schema/主键/分区模式不可在同一数据集悄悄改变。
提交之后、调度记账之前退出，通过事务内 receipt 找到原快照，不重复追加。

OS 文件锁没有 TTL 抢占；所有本机写进程必须使用相同 root。不同分区可并发，catalog 冲突有限重试。
PostgreSQL writer 使用共享 advisory maintenance gate，备份使用排他 gate；网络与清洗不持有该锁。
外部 DuckLake writer、压实和清理必须在备份前另行停止。首轮不自动删除快照或孤儿文件。

## 时间语义和状态恢复

新采集的 current stock snapshot 只能标记其实际观察日，不能用于伪造历史证券身份。
`observed`、`legacy` 与 `pit` 分开：迁移不会凭空补齐原始响应、公开时间、修订历史。
当前核心适配器产物均为 observed；严格历史 PIT 消费默认拒绝不足证据的输入。
演练/研究必须显式启用 `--allow-observed` 或 `QRP_LAKE_ALLOW_OBSERVED=true`，结果仍标记为 observed。
DataPortal 的严格行级查询另要求 available_at、observed_at、来源证据与带时区 knowledge cutoff，
排除截止时点之后公开或取得的行；time travel 本身不构成 PIT 证明。

System B 复用原标准输入 SQL、复权口径和纯计算算法，不是新写的玩具状态机。
checkpoint 保存代码/schema/参数、前态身份、输入版本、业务内容摘要、输出摘要。
正常推进校验下一交易日；历史业务输入变化需要显式重放。采集时间变化不等同业务修订。
首版仍从提供的历史完整重算，未宣称已完成增量算法优化。历史重放按日期分别发布，旧快照可读；
范围执行可能部分完成，重试通过相同输入及 checkpoint 身份恢复。不要把部分范围当作全部已更新。

## 本机安装与隔离配置

原生产依赖不升级。本轮使用 Python 3.13、DuckDB 2.0.0、Dagster 1.13.28、
psutil 7.2.2；DuckLake extension 实测构建标识 `417a9117f5`，PostgreSQL scanner `c0f5a3c0c4`。
部署前记录实际 extension 版本，不能仅因 Python 包锁定就假定 extension 也已锁定。

```powershell
python -m venv .venv-lake
.venv-lake/Scripts/python -m pip install -e '.[lake,test]'
```

`lake-server` extra 包含匹配的 webserver 与 PostgreSQL Dagster 元数据适配器。
PostgreSQL catalog 需要预先创建空数据库。CLI 不创建数据库或系统用户。
core 与 extension 配置文件必须分离，模板参考 `.env.example` 和 `deploy/lake/README.md`。
不要把真实 token、DSN 或生产配置提交到仓库。第三方来源通过既有 `QRP_TUSHARE_API_URL` 配置；
提供的用户 token 没有写入工程文件。

以下路径均由操作者替换为已批准的隔离目录：

```text
python -m qrp_atlas.pipeline.lake.cli --env-file <core.env> init --domain core --install-extensions
python -m qrp_atlas.pipeline.lake.cli --env-file <extension.env> init --domain extension
python -m qrp_atlas.pipeline.lake.cli --env-file <core.env> run --domain core --dataset trading_calendar --date YYYY-MM-DD
python -m qrp_atlas.pipeline.lake.cli --env-file <core.env> run --domain core --dataset daily_market_snapshot --date YYYY-MM-DD --input-manifest <inputs.json>
```

`inputs.json` 由已发布上游 receipt 构建：

```json
{"trading_calendar":{"snapshot_id":12,"publication_id":"<published-id>","domain":"core"}}
```

真实市场调用还需要 stock_info 和 suspend_d 同目标日期的 receipts。CLI 校验 receipt 的数据集、
分区与 snapshot 绑定。`--raw-batch <id>` 可以不调用供应商重放已保存批次。
Dagster CliLauncher 自动把同日依赖 receipts 传给 CLI，独立日期回补失败不冒充成功。
历史 stock_info 不能通过“今天重新请求”补成历史 snapshot，必须使用有来源限制的 legacy 导入或已有录制响应。

插件启用文件由操作者控制，例如：

```json
{"rolling_mean":"qrp_atlas.pipeline.lake.extensions.rolling:RollingMeanPlugin","system_b_state":"qrp_atlas.pipeline.lake.extensions.system_b:SystemBStatePlugin"}
```

```text
python -m qrp_atlas.pipeline.lake.cli --env-file <extension.env> run --domain extension --dataset system_b_state --date YYYY-MM-DD --input-manifest <inputs.json> --plugins <enabled.json> --allow-observed
```

扩展配置不得含 core writer catalog。新增插件实现 `spec`、`schema`、`run(context)`，
将正式 schema 放在 contracts 或插件包的契约模块，通过 allowlist 启用；无需修改核心执行器。
纯 Python 声明不能代替 OS 权限隔离，Linux 仍需实际角色和目录权限验证。

## 旧数据、备份和切换

```text
python -m qrp_atlas.pipeline.lake.cli --env-file <core.env> import-legacy --source <readonly-export.duckdb> --dataset daily_market_snapshot --start YYYY-MM-DD --end YYYY-MM-DD
```

默认 dry-run；仅在隔离目标明确确认后加 `--apply`。导入复用旧表规范化字段，不标记为 Raw。
重复同一映射和数据恢复原 receipt，已有不同版本会拒绝无 CAS 的盲覆盖；修订迁移需另行准备前置版本计划。
原生产库若正被写入，不能直接复制单文件假装一致备份。导出需遵守旧服务自己的写锁/备份流程。

```text
python -m qrp_atlas.pipeline.lake.cli --env-file <core.env> backup --domain core --path <new-backup-dir> --external-writers-quiesced --pg-bin <postgres-bin-dir>
python -m qrp_atlas.pipeline.lake.cli --env-file <empty-restore.env> restore --domain core --path <backup-dir> --pg-bin <postgres-bin-dir>
```

恢复要求空 catalog 与新目录；校验备份 checksum，并通过旧 snapshot 查询验证。恢复环境需显式
`QRP_LAKE_RELOCATED=true`，将 catalog 读取重定向至恢复文件目录，不去读原文件冒充恢复成功。
backup 不包含供应商 Raw 或 Dagster 元数据；它们需要独立保留/备份，不能把湖仓备份等同整系统备份。

读取切换仅针对已验证数据集：`QRP_LAKE_DAILY_SOURCE=lake`、固定 `QRP_LAKE_DAILY_SNAPSHOT`
及只读 catalog 配置。旧接口字段保持兼容。不存在自动 latest 路由；新版本验收后由操作者显式更新固定版本。
撤回开关恢复 legacy，但旧库在切换后可能落后，必须记录截止日和补齐方案。
生产触发权按数据集交接，禁止新旧同时生效；不得停止其余未迁移链路。

## Episode / 三池后续边界

| 能力 | 当前路径 | 后续适配要求 |
| --- | --- | --- |
| State | 新插件可演练，旧生产仍保留 | 固定核心输入、版本化 checkpoint，不改变算法 |
| Episode | `pipeline/system_b_episode` 及独立输出库 | 固定 state/input revisions；区分当时观察与 finalized 事后事实；将写库替换为独立发布 |
| 三池 | `pipeline/system_b_pools` 及独立输出库 | 固定行情/因子/状态/Episode 血缘；保留全市场关系；不能按股票盲目拆并行 |

本轮不迁移 Episode/三池算法或消费者，不宣称它们已获得新的存储与隔离保证。

## 验证证据与限制

`tests/pipeline/lake` 覆盖 Raw、清洗与完整性、类型转换后键校验、CAS、固定快照、
真正进程强退、PostgreSQL 并发、备份恢复、原 System B 对照、修订重放、外部插件、
子进程超时/取消/RSS/磁盘/父进程强退，以及真实 Dagster→CLI→Raw→DuckLake 链。
`tests/config/test_lake_settings.py` 验证配置隔离与秘密脱敏。
`tests/pipeline/lake/test_consumer.py` 验证真实旧 API 调用点选择固定湖仓版本而不访问旧库。

PostgreSQL 专项测试要求仅供测试的 loopback `QRP_TEST_POSTGRES_DSN`，每项创建 UUID 数据库并清理；
备份测试另需 `QRP_TEST_PG_BIN`。没有提供这些条件时显示 skipped，不计作通过。

Linux OS 用户/文件权限、cgroups、常驻 Dagster daemon 重启与槽位回收、目标机真实历史规模性能
仍须按 `deploy/lake/README.md` 在授权环境实测。本地进程预算轮询不是内核硬限额。
所有 Windows 性能和合成数据结果只代表该测试环境。生产数据缺口、PIT 证据缺失不会因本次重构消失。

### A01–A12 验收记录

以下“通过”仅指本机隔离环境。所有测试源码在 `tests/pipeline/lake/`；测试不依赖真实供应商 token。

| 编号 | 本轮结果 | 证据与剩余边界 |
| --- | --- | --- |
| A01 | 本机通过 | `test_parallel_provider_waits_do_not_hold_publication_right`：两个采集等待时仍可发布，返回两个独立真实 Raw 批次 |
| A02 | 本机通过 | `test_core.py` 分页失败、重复页、上限及非法数据；`test_partial_raw_never_reaches_ducklake` |
| A03 | 本机通过 | `test_core_integration.py`、`test_runtime_cli_integration.py`：重复原始批次、同日期重放，发布身份和结果不重复 |
| A04 | 本机通过 | `test_postgres_abrupt_process_exit_recovers[before_commit]`：真实子进程强退仍读取完整旧版本 |
| A05 | 本机通过 | 同一测试 `[after_commit]`：真实 PostgreSQL 提交后强退，恢复同一 receipt，记录仅一条 |
| A06 | 本机通过；Linux 未验收 | 独立核心定义、显式插件启停，`test_runtime_worker.py` 验证失败、超时、取消、内存、磁盘、子进程和父进程强退；OS 权限/cgroups 未实测 |
| A07 | 本机通过；常驻调度未验收 | 同表不同日期并发含一失败分区、同目标 CAS 竞争、Dagster 同日依赖及单日期回补；daemon 重启/槽位回收待 Linux 实测 |
| A08 | 本机通过 | `test_state_cli.py`：真实状态初次/次日续算、业务修订拒绝、冻结 checkpoint 范围重放、重复幂等与旧快照保留；`test_plugins.py` 对照原算法 |
| A09 | 本机通过；历史来源证据受限 | 严格 PIT 拒绝 observed/legacy，行级截止时间排除后知事实；真实 daily API 固定快照及旧接口回归。当前供应商数据不获得严格历史 PIT 身份 |
| A10 | 本机通过 | `test_custom_extension_entrypoint_needs_no_executor_edit`：外部插件自带 schema 经同一 CLI 发布 |
| A11 | 本机通过 | `test_postgres_backup_restores_fixed_history`：pg_dump/pg_restore 到全新 catalog/目录，旧快照查询实际旧价格 |
| A12 | 已测量，性能未验收 | 250 日 × 1,000 资产历史，两轮各 20 日；并发核心 P95 2.656 s，对照 2.269 s。门槛未设定，队列与纯锁等待未独立测量，见基准文档 |

全量命令 `.venv-lake/Scripts/python -m pytest -q --tb=short` 的首次结果为
**1644 passed、2 failed、32 errors、3 skipped**（`.lake-test/full-test.log`）。
32 个 error 都是既有研究 API fixture 创建符号链接时的 Windows `WinError 1314`；没有跳过或放宽这些断言。
两个 failed 是 DuckDB 2 活动结果未释放的兼容问题，已修复：旧 System B repository 完整消费单行 COUNT，
旧 IRM loader 在注册前释放上次结果并在写后清理临时关系。同两模块在 DuckDB 2.0 和原 1.4.4 环境均 **20 passed**。
最终专项回归 **180 passed**（208.02 s，`.lake-test/final-targeted.log`），命令为：

```powershell
$env:QRP_TEST_POSTGRES_DSN='host=127.0.0.1 port=55439 user=qrp_test dbname=qrp_lake_test'
$env:QRP_TEST_PG_BIN=(Resolve-Path '.lake-test/tools/pgsql/bin').Path
.venv-lake/Scripts/python -m pytest -q --tb=short tests/pipeline/lake tests/config tests/api/test_daily_adjustment.py tests/pipeline/system_b/test_production.py tests/pipeline/test_irm_qa.py
```

上述 DSN 仅属于本机临时测试实例，不是生产连接。专项执行期间追加的 A01/A07 两个并发测试单独运行
**2 passed**（`.lake-test/concurrency-final.log`），未重复计算到 180 项中。
24 条 warning 来自 Dagster `backfill_policy` beta API；保留版本锁定，升级时需重新验证。
没有再运行一次完整套件，不能把局部复验描述为全量绿灯。
53 个候选文本文件通过 UTF-8、代码块闭合与凭据模式检查，39 个 Python 模块编译检查通过，
`pyproject.toml` 可解析，`git diff --check` 通过。

### Old Contract Change 与部署交接

- `config.settings` 新增可选 lake 配置；未配置时保留 legacy 默认行为，诊断不输出 catalog 凭据。
- daily 两个 API 入口通过连接适配器切换；URL、响应字段及旧价格调整规则不变。独立前端无需为本轮修改字段契约。
- 原 System B 和 IRM 两处只调整 DuckDB 结果/临时关系的生命周期；算法、表结构、upsert 含义不变，无旧库原地迁移。
- 新依赖安装到独立环境：`.[lake,lake-server]`；不对原生产虚拟环境直接升级 DuckDB。
- Linux 尚未同步任何代码、配置或依赖。候选部署清单为 `deploy/lake/` 中的独立用户、目录、3 个新 PostgreSQL 数据库、4 个服务及端口；先验收隔离、恢复和性能，再决定触发权及读取切换。
- 新服务首次部署需要启动 core/extension code location、daemon 和 webserver；旧 API 只有在明确切换读取配置时才重载。各数据集旧任务触发权须逐一交接，不全局停掉旧 pipeline。
- 旧数据默认继续由旧库服务；迁移先 dry-run，再向独立目标导入和对照。没有本轮必须在生产执行的 migration 或数据修复。
- 当前功能范围不含 Episode/三池等全量业务迁移；历史来源证据、真实供应商分页及 Linux 运行验收仍是上线前置条件。

详细性能结果及复现方法见 [pipeline-lake-benchmark.md](pipeline-lake-benchmark.md)。

## 仓库与续接状态

本轮代码、测试和文档通过 `feature/v1.1-pipeline-lake` 任务分支提交供远端审查，
开发基线为 `6d777ecc096bd891a307e9f6bef552c073b05f23`。审查范围是该基线与任务分支之间的差异；
没有合并或直接更新 `develop/v1.1`、`main`，也没有部署到 Linux。
原先未跟踪的 `2026-09-16-sandbox-joinquant-parity-test-report.md` 保留，没有并入本轮修改。
本机 `.venv-lake/`、`.lake-test/` 已忽略；测试 PostgreSQL 在交付前停止，测试证据和临时数据保留供复查。
下一步为审查候选 diff 并约定 Linux 隔离验收范围，按部署文档准备环境后再验证；不应直接替换生产环境。

官方 API 依据：[DuckLake 连接](https://ducklake.select/docs/stable/duckdb/usage/connecting)、
[Time travel](https://ducklake.select/docs/stable/duckdb/usage/time_travel)、
[备份恢复](https://ducklake.select/docs/stable/duckdb/guides/backups_and_recovery)。
