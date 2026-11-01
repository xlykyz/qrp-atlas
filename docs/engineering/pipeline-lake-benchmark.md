# Pipeline lake A12 基准

## 2026-11-02 本机实测

命令：`.venv-lake/Scripts/python tools/lake_benchmark.py --env-file E:/projects/qrp-atlas/.lake-test/benchmark.env --history-days 250 --days 20 --assets 1000`。
运行前只读 `--plan` 通过，性能门槛明确未设定。环境为 Windows 11、4 个物理核心/8 个逻辑 CPU、
约 31.8 GiB RAM、Python 3.13.12、DuckDB 2.0.0、pandas 3.0.1、本机独立 PostgreSQL 17.11；
每个计算连接 2 线程、4GB DuckDB 内存预算。预置 250,000 行历史，每轮测量 20 个日期，各发布 1,000 行。

| 测量 | 仅核心 | 扩展并发 |
| --- | ---: | ---: |
| 核心端到端 P50 | 1.963 s | 2.498 s |
| 核心端到端 P95 | 2.269 s | 2.656 s |
| 输入读取 P95 | 0.751 s | 0.954 s |
| 计算 P95 | 0.566 s | 0.706 s |
| 发布 P95（含校验、锁与回执） | 0.976 s | 1.118 s |
| 核心进程峰值 RSS | 271.6 MiB | 284.3 MiB |
| 进程树峰值 RSS | 271.6 MiB | 488.7 MiB |
| 采样临时文件峰值 | 14,482 bytes | 28,861 bytes |

并发轮的扩展完成 41 次扫描/发布；核心 P95 比值 1.1705。两轮输出哈希一致，
MA20 解析公式核对及固定快照回读均通过。结果状态为 `measurement_only_no_thresholds`，
不能写成性能目标达标。队列等待和纯锁等待没有单独测量，结果为 `null`；PostgreSQL 资源不在 RSS 统计内。
完整原始结果保存在本机忽略目录 `.lake-test/benchmark-250/benchmark-results.json`，
运行日志为 `.lake-test/benchmark-run.log`，未把生成数据或连接配置纳入版本控制。

## 2026-11-02 Linux 隔离实测（审查修复后）

在 `myserver` 的独立目录 `/home/claire/qrp-lake-tests/review-86fb751-20261102` 运行，
未使用生产数据库、行情或服务配置。主机为 i7-9750H、6 物理核心/12 逻辑 CPU、约 60.4 GiB RAM；
Python 3.13.13、DuckDB 2.0.0、pandas 3.1.0、独立 PostgreSQL 18.4（端口 55439）。
基准进程组限制为 CPUQuota=400%、MemoryMax=8G，测试 PostgreSQL 另限 CPUQuota=100%、MemoryMax=2G；
每连接 2 线程、4GB DuckDB 预算。主机原有服务保持运行，测试不是独占整机测量。

命令参数仍为 `--history-days 250 --days 20 --assets 1000`：250,000 行初始历史，每轮 20 个日期，
每日期 1,000 行。完整回归结束后才开始性能测量。

| 测量 | 仅核心 | 扩展并发 |
| --- | ---: | ---: |
| 核心端到端 P50 | 2.852 s | 5.870 s |
| 核心端到端 P95 | 4.213 s | 6.819 s |
| 输入读取 P95 | 2.559 s | 3.940 s |
| 计算 P95 | 1.059 s | 3.309 s |
| 发布 P95（含校验、锁与回执） | 1.966 s | 3.974 s |
| 核心进程峰值 RSS | 356.0 MiB | 358.5 MiB |
| 进程树峰值 RSS | 371.4 MiB | 644.9 MiB |
| 采样临时文件峰值 | 14,438 bytes | 28,770 bytes |

扩展并发轮完成 49 次扫描/发布。核心 P95 比值为 1.6184，即增加约 61.8%；
不能据此宣称扩展对核心无影响。两轮输出哈希一致、解析公式核对及固定快照回读通过，
性能状态仍为 `measurement_only_no_thresholds`。Windows 与 Linux 的版本、资源预算不同，不能直接据此比较平台优劣。
各阶段 P95 不可相加；RSS 是采样值，进程树求和可能重复计入共享页，不等于 cgroup 实占。
未测调度排队、独立锁等待或 PostgreSQL CPU/RSS，也未与旧 pipeline 对照。
该滚动均值负载不衡量 System B 按日期重放的全历史计算成本，真实规模与性能门槛仍需单独约定。

原始结果保留于测试节点 `benchmark-250/benchmark-results.json`（相对上述隔离目录），
本机副本为 `.lake-test/linux-review/benchmark-results.json`，另有 `linux-benchmark.log` 和 `runtime-freeze.txt`。
生成数据、日志、配置和凭据不提交；下面的复现方法及统计边界同样适用。

## 基准方法

`tools/lake_benchmark.py` 在专用空 PostgreSQL 数据库与新目录中测量两轮相同核心计算：无扩展，以及一个扩展进程同时扫描固定历史快照并发布到独立 catalog。测试只产生 `benchmark_*` 合成数据，不调用数据供应商，不读取或改写旧数据库，不代表生产性能验收。

默认规模为 20 个工作日标签 × 1,000 个合成资产，并预置 20 日 MA20 回看。日期由工作日序列生成，不冒充真实交易所日历。`--history-days 250` 增加历史扫描规模。合成价格为线性序列，因此 MA20 可独立按解析公式核对；两轮还对比输出内容哈希，并回读已发布快照。

## 环境与安全前置条件

使用已安装锁定依赖和 DuckLake/PostgreSQL 扩展的隔离 Python 环境。自行或由获授权的部署步骤准备两个全新 PostgreSQL 数据库，以及只读核心 catalog 的数据库角色和文件读取权限。所有非系统表必须为空；脚本拒绝复用已初始化或有业务数据的数据库。脚本不创建 PostgreSQL 数据库、用户或授权。

另建绝对路径 `benchmark.env`，不要使用 `root.env` 或仓库 `.env`：

```dotenv
QRP_RUNTIME_ENV=test
QRP_LAKE_ROOT=/absolute/new/benchmark-run
QRP_LAKE_CORE_CATALOG=postgres:dbname=BENCH_CORE host=localhost user=BENCH_WRITER password=INJECT_SECRET
QRP_LAKE_CORE_READ_CATALOG=postgres:dbname=BENCH_CORE host=localhost user=BENCH_READER password=INJECT_SECRET
QRP_LAKE_EXTENSION_CATALOG=postgres:dbname=BENCH_EXTENSION host=localhost user=BENCH_EXTENSION_WRITER password=INJECT_SECRET
QRP_LAKE_THREADS=2
QRP_LAKE_MEMORY_LIMIT=4GB
QRP_LAKE_DAILY_SOURCE=legacy
```

凭据仅放在操作者管理的未跟踪配置，不提交真实值。Windows 将目录替换为绝对 Windows 路径。`QRP_LAKE_ROOT` 必须全新或为空，并且不能与旧数据目录重叠；失败后保留的目录和 catalog 不会自动清空，重试需新建目标。运行前仍需满足项目的 Linux 写操作授权约定。

## 执行

先只读检查配置、目标目录与 catalog：

```powershell
.venv-lake/Scripts/python.exe tools/lake_benchmark.py --env-file C:/isolated/benchmark.env --plan
```

完整默认样本与历史规模样本分别使用不同的新目标：

```powershell
.venv-lake/Scripts/python.exe tools/lake_benchmark.py --env-file C:/isolated/benchmark.env
.venv-lake/Scripts/python.exe tools/lake_benchmark.py --env-file C:/isolated/history-benchmark.env --history-days 250 --assets 1000
```

Linux 使用隔离虚拟环境中的 `python` 和对应绝对配置路径。可以先用 `--days 2 --assets 50` 检查脚本连通性；小样本只证明流程可运行，不能作为 P95 或容量结论。要验收性能，在运行前由操作者明确传入 `--max-core-p95-seconds`、`--max-slowdown-ratio`；没有门槛时状态固定为 `measurement_only_no_thresholds`，不会报“性能通过”。

## 结果与边界

输出为新目标根目录中的 `benchmark-results.json`，包含每个日期的固定输入读取、计算、发布和端到端耗时，以及两轮 P50/P95、核心与进程树 RSS、CPU 时间和 staging/spill 空间采样。发布计时包含候选校验、锁等待、catalog 提交和发布回执恢复；锁等待未单独埋点，字段为 `null`，不能用总发布时间冒充纯提交时间。未运行调度器，队列等待也为 `null`。

端到端计时从读取输入快照开始，到取得发布回执结束；后续独立正确性校验耗时单独记录。资源以 50 ms 间隔采样，可能漏掉短峰值；不包含 PostgreSQL 服务器 RSS/CPU，不等于 cgroup 硬隔离验证。固定先运行 baseline 再运行并发，缓存及宿主其他任务可能影响结果。真实目标机和生产数据规模仍需另行验收。

扩展在新进程中只收到核心 reader 连接和 extension writer 连接，但这里运行的是可信合成负载；它不是凭据隔离、恶意插件或崩溃恢复验收替代品。脚本不会删除测试数据、快照或 catalog。清理必须由操作者确认专用目标后执行。
