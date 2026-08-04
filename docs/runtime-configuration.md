# QRP v1.0 运行配置与部署

本文说明 QRP Atlas v1.0 的统一运行配置、跨平台路径、秘密处理、初始化与诊断方式。配置实现的唯一事实来源是 `qrp_atlas.config.settings`。

## 新用户首次运行：setup

安装完成后，默认产品入口是：

```bash
qrp-atlas-config setup
qrp-atlas-api
```

`setup` 是 `AppSettings`、`initialize_runtime()`、包内空数据库初始化和 `doctor()` 的产品化编排，不维护第二套配置解析或默认值。流程为：

```text
选择 profile
→ 确认配置文件
→ 设置运行与数据目录
→ 创建、复用或跳过 DuckDB
→ 配置 API 和认证
→ 可选配置 Tushare 与代理
→ 查看脱敏摘要
→ 明确确认
→ 原子保存
→ 初始化目录和数据库
→ doctor
→ 输出唯一启动命令
```

交互流程支持在确认页返回修改。`Ctrl+C`、EOF 或取消不会写配置、创建目录或创建数据库。stdin/stdout 不是 TTY 时必须显式使用 `--non-interactive`，否则立即失败。

### 三个 profile

- `local`：`127.0.0.1`、local 认证、development、CORS `*`，默认建议创建空 DuckDB。
- `lan`：`0.0.0.0`、local 认证、development；必须确认仅用于可信局域网，并确认或收紧 CORS。
- `production`：production、database 认证；必须从隐藏输入或受保护环境提供 PostgreSQL DSN，必须使用显式 CORS，运行和数据目录必须在仓库外。

Profile 只提供首次建议，所有候选值仍先由 `AppSettings.load(...)` 严格验证。

### 配置文件与重复运行

源码部署默认写入仓库根 `.env`。也可在命令前或 `setup` 子命令后指定其他文件：

```bash
qrp-atlas-config --env-file /etc/qrp-atlas/qrp-atlas.env setup
qrp-atlas-config setup --env-file /etc/qrp-atlas/qrp-atlas.env
```

已有文件会先解析并显示脱敏摘要，用户明确选择更新后才写入。更新保留未知变量、注释和未修改秘密；原文件复制到不覆盖的 `.bak`/`.bak.N`，新内容通过同目录临时文件原子替换。POSIX 上目标文件和备份权限为 `0600`；Windows 不执行无意义的 `chmod`，文件访问边界由当前 Windows 用户和目录 ACL 决定。

目标目录不可创建或不可写时 setup 明确失败。保存后的重新加载、初始化或 doctor 发生阻塞失败时，原配置恢复；setup 不自动删除用户已有目录或数据库。

### DuckDB 三种模式

1. `create`：在 `QRP_DUCKDB_PATH` 原子创建 v1.0 空库，只包含 `daily_market_snapshot`、`market_phase`、`trade_execution`，所有表零行；路径已存在时绝不覆盖。
2. `reuse`：要求普通文件存在，以 DuckDB 只读模式打开并执行 `SHOW TABLES`，不写入数据。
3. `skip`：完成配置、目录初始化和 doctor；最终明确提示真实数据库接口在 DuckDB 准备完成前不可用。

`scripts/init_db.py` 只是包内 `qrp_atlas.database.create_empty_database()` 的兼容薄入口，schema SQL 只保留一份。setup 不运行 pipeline、不下载数据、不做历史回补。

### 秘密处理

PostgreSQL DSN、Tushare Token 和可能含凭据的代理 URL 使用隐藏输入；摘要、`repr`、doctor、日志和错误只显示 `configured` 状态或异常类型。未修改的已有秘密会保留。setup 不提供任何秘密 CLI 参数，`--set` 也不允许用于 setup；非交互模式只能从进程环境或已有受保护配置文件读取秘密。

setup 不主动连接 PostgreSQL、不部署 PostgreSQL schema、不创建用户，也不在线验证 Tushare Token。PostgreSQL 后续管理继续使用现有 `qrp-atlas-auth`。

### 非交互模式

```bash
qrp-atlas-config setup \
  --profile local \
  --env-file /srv/qrp-atlas/qrp-atlas.env \
  --home /srv/qrp-atlas/runtime \
  --data-dir /srv/qrp-atlas/data \
  --database create \
  --non-interactive --yes
```

非交互模式不等待输入，缺少 `--profile`、确认或 production 必需项时立即失败。已有配置还必须显式使用 `--update-existing`。端口占用作为警告输出；自动化调用方可据此选择不同 `--api-port`。命令稳定返回：成功 `0`，配置/执行失败 `2`，安全取消 `130`。

### 自定义 env 文件启动

默认仓库 `.env` 可直接使用 `qrp-atlas-api`。非默认文件必须使用正式参数：

```bash
qrp-atlas-api --env-file /srv/qrp-atlas/qrp-atlas.env
```

向导成功页会输出与实际文件一致的命令，不依赖仅在向导进程中有效的临时环境变量。

Windows PowerShell 示例：

```powershell
qrp-atlas-config setup --env-file 'D:\QRP Atlas\qrp-atlas.env'
qrp-atlas-api --env-file 'D:\QRP Atlas\qrp-atlas.env'
```

Linux 示例：

```bash
qrp-atlas-config setup --env-file "$HOME/.config/qrp-atlas/qrp-atlas.env"
qrp-atlas-api --env-file "$HOME/.config/qrp-atlas/qrp-atlas.env"
```

路径支持绝对路径、`~` 和相对仓库根的路径；Windows/Linux 最终解析仍由 `AppSettings` 的统一跨平台规则决定。

## 1. 审计分类与边界

本次运行配置审计将仓库内容分为三类：

- **A. 部署敏感配置**：代码/数据位置、DuckDB、API 监听、认证模式、PostgreSQL DSN、日志、只读模式、代理、限速和临时远程访问工具端口。这些内容由统一配置模型管理。
- **B. 秘密配置**：`TUSHARE_TOKEN`、可能含密码的 PostgreSQL DSN、可能含认证信息的代理 URL，以及远程访问 token 文件内容。秘密只通过环境或受保护文件注入，不进入展示、日志和异常文本。
- **C. 业务与架构常量**：contracts 字段、表名、schema、算法参数、业务枚举以及稳定数据源 URL。它们不因“消灭硬编码”而被机械外部化；只有部署时确需切换的 Tushare-compatible API URL 被配置化。

审计同时移除了 Python 业务模块中分散的 `os.getenv`/dotenv 读取、开发机绝对路径和以 shell 当前目录为基准的数据路径。旧公开入口保留为兼容适配层。

## 2. 单一配置入口

主要对象：

- `AppSettings`
- `PathSettings`
- `DatabaseSettings`
- `ApiSettings`
- `AuthenticationSettings`
- `ExternalServicesSettings`
- `LoggingSettings`
- `RuntimeSettings`

应用代码应使用：

```python
from qrp_atlas.config.settings import get_settings

settings = get_settings()
database_path = settings.paths.duckdb_path
```

需要隔离环境的测试或显式运行器可使用 `AppSettings.load(environ=..., overrides=..., project_root=...)`。普通业务模块不得再次直接读取同名环境变量。

## 3. 优先级和 dotenv

每个配置项使用同一优先级：

```text
显式 overrides / CLI --set
> 进程环境变量
> QRP_ENV_FILE 指定的 dotenv，或仓库根 .env
> 稳定默认值
```

`QRP_ENV_FILE` 本身可由显式参数、override 或进程环境指定。显式指定的文件不存在时会立即报错；默认的仓库根 `.env` 不存在时不会报错。相对 dotenv 路径从仓库根解析，而不是从当前 shell 目录解析。

`.env.example` 是公开配置清单和注释模板，不包含真实秘密。生产环境建议由 systemd `EnvironmentFile`、CI/CD secret store 或同等安全注入机制提供配置。

## 4. 路径模型

`QRP_HOME` 管理运行时文件，`QRP_DATA_DIR` 管理持久数据：

```mermaid
flowchart TD
    R["repository root"] -->|default| H["QRP_HOME"]
    R -->|default: repository/data| D["QRP_DATA_DIR"]
    H --> L[".runtime/logs"]
    H --> T[".runtime/tmp"]
    H --> RA[".runtime/remote_access"]
    D --> DB["db/quant.db"]
    D --> RAW["raw/"]
    D --> CAN["canonical/"]
    D --> ST["state/"]
    D --> BR["backtest_runs/"]
    D --> BT["backtest_tasks/"]
    D --> RR["robustness_runs/"]
    D --> DS["declarative_strategies/"]
```

高级覆盖项只用于确有独立挂载或兼容需求的目录。未配置新变量时，默认仍是仓库根和仓库内 `data/`，不会自动迁移现有数据。

### 解析规则

- 所有路径使用 `pathlib.Path`。
- 相对路径总是相对于仓库根，不依赖 CWD。
- Windows 盘符绝对路径和 UNC 路径可在 Windows 使用；POSIX 绝对路径可在 Linux 使用。
- 在另一平台配置外来绝对路径会明确失败，避免把 `C:\...` 当作 Linux 相对文件名，或把 `/srv/...` 当作 Windows 路径。
- `C:relative` 这类盘符相对路径、空路径和包含 NUL 的路径会失败。
- `init` 负责集中创建目录；不会创建 DuckDB 文件或覆盖任何已有文件。

## 5. 支持的公开配置

权威清单在 `.env.example` 与代码中的 `SUPPORTED_ENV_VARS`，二者由测试保持一致。配置按以下领域分组：

- 路径：`QRP_HOME`、`QRP_DATA_DIR`、DuckDB、state、回测结果/任务、稳健性、声明式策略、log、tmp。
- 数据库/运行：`QRP_READ_ONLY`、`QRP_RUNTIME_ENV`、`QRP_LOG_LEVEL`。
- API：`QRP_API_HOST`、`QRP_API_PORT`、`QRP_API_CORS_ORIGINS`。
- 认证：`QRP_AUTH_MODE`、本地用户字段、PostgreSQL DSN、session TTL。
- 外部服务：Tushare token/API URL、请求间隔、重试、HTTP/HTTPS/NO_PROXY。
- 临时远程访问工具：token 文件路径、只读数据库路径和端口。

Boolean、整数、浮点数、URL、UUID、enum 和路径均严格解析，非法值不会静默回退。

## 6. 秘密处理

以下值永远不在 `config show` 中显示原文：

- `TUSHARE_TOKEN`
- `QRP_AUTH_DATABASE_URL`
- `QRP_HTTP_PROXY`
- `QRP_HTTPS_PROXY`

它们只显示 `configured` 或 `not configured`。配置 dataclass 的 repr 同样隐藏秘密，PIT backfill 错误摘要会替换已配置秘密并截断疑似认证文本。临时远程访问工具只配置 token 文件路径，文件内容不属于应用配置展示。

安全要求：

1. 不把真实 token、密码、DSN 或带凭据代理 URL 写入仓库。
2. 不把秘密放在 CLI `--set` 中用于长期服务，因为命令行可能被进程列表或审计系统记录。
3. PostgreSQL 模式仅在 `QRP_AUTH_MODE=database` 时要求 DSN；不使用 Tushare 时 token 缺失只产生 doctor 警告。
4. 生产环境应限制 env 文件和 token 文件的文件权限。

## 7. 命令

安装项目后可使用 console script，也可直接运行模块：

```bash
qrp-atlas-config show
qrp-atlas-config doctor
qrp-atlas-config database-audit
qrp-atlas-config init

python -m qrp_atlas.config show
python -m qrp_atlas.config doctor
python -m qrp_atlas.config init
```

全局参数必须位于子命令前：

```bash
qrp-atlas-config --env-file /etc/qrp-atlas/qrp-atlas.env doctor
qrp-atlas-config --set QRP_DATA_DIR=runtime-data show
```

- `show` 输出最终解析值、运行模式、数据库模式、外部服务配置状态和每项来源；秘密已脱敏。`--compact` 输出单行 JSON。
- `doctor` 不创建文件，检查目录可读写/可创建性、只读约束、DuckDB 状态、认证配置、Tushare 凭证、平台路径和生产 CORS/本地认证警告。阻塞失败返回 1，配置解析失败返回 2。支持 `--json`。
- `database-audit` 只读检查主库、IRM、System B episode/pool 数据库和 Job Runtime SQLite 的有效路径、可读性及契约表；同时列出疑似旧库/备份文件，但不会删除。阻塞失败返回 1，支持 `--json`。
- `init` 幂等创建必要目录，保留已有目录和数据库，不创建数据库、不写秘密。只读模式下缺失目录是失败。支持 `--json`。

主库 schema 迁移必须使用显式的仓库脚本；默认是 dry-run，只有带 `--apply` 才会写入：

```bash
python scripts/migrate_canonical_schema.py \
  --env-file /etc/qrp-atlas/qrp-atlas.env
python scripts/migrate_canonical_schema.py \
  --env-file /etc/qrp-atlas/qrp-atlas.env --apply
python scripts/migrate_canonical_schema.py \
  --env-file /etc/qrp-atlas/qrp-atlas.env --apply \
  --backup-path /var/backups/qrp/quant.before-schema.db
```

迁移只补建缺失的 contract 表，默认 dry-run，只有显式 `--apply` 才执行写入；执行时在同一 DuckDB 连接内 `FORCE CHECKPOINT`，通过 `ATTACH` + `COPY FROM DATABASE` 创建并校验表集合、列结构和行数一致的回滚备份，校验失败删除无效备份，迁移事务失败回滚并保留有效备份；不会搬迁、合并或删除现有数据库。

API 通过统一入口启动：

```bash
qrp-atlas-api
```

它读取 `QRP_API_HOST`、`QRP_API_PORT` 和 `QRP_LOG_LEVEL`；FastAPI CORS 从同一配置对象读取。

## 8. 部署示例

### Windows：代码与数据分离

PowerShell 临时会话示例（路径仅为通用示例）：

```powershell
$env:QRP_HOME = 'D:\qrp-runtime'
$env:QRP_DATA_DIR = 'E:\qrp-data'
$env:QRP_API_HOST = '127.0.0.1'
$env:QRP_API_PORT = '8000'
python -m qrp_atlas.config doctor
python -m qrp_atlas.config init
qrp-atlas-api
```

若持久配置，请创建未纳入 Git 的 `.env` 或由 Windows 服务管理器注入环境变量。不要修改源码常量。

### Linux：systemd

仓库中的 `deploy/qrp-atlas-api.service` 使用通用布局：

- 代码：`/opt/qrp-atlas`
- 配置：`/etc/qrp-atlas/qrp-atlas.env`
- 运行用户：`qrp-atlas`
- 数据和运行目录：由 env 文件中的 `QRP_DATA_DIR`/`QRP_HOME` 决定

部署前先在服务用户权限下运行：

```bash
sudo -u qrp-atlas /opt/qrp-atlas/.venv/bin/qrp-atlas-config \
  --env-file /etc/qrp-atlas/qrp-atlas.env doctor
sudo -u qrp-atlas /opt/qrp-atlas/.venv/bin/qrp-atlas-config \
  --env-file /etc/qrp-atlas/qrp-atlas.env init
```

再安装并启动 unit。生产配置至少应显式设置 `QRP_RUNTIME_ENV=production`、数据目录、API 监听/CORS，并根据需要设置认证和秘密。

### PostgreSQL 认证

```env
QRP_RUNTIME_ENV=production
QRP_AUTH_MODE=database
QRP_AUTH_DATABASE_URL=postgresql://USER:PASSWORD@db.example.com:5432/qrp_atlas
QRP_AUTH_SESSION_TTL_SECONDS=86400
```

先应用 `deploy/postgres/001_auth_schema.sql`，再使用 `qrp-atlas-auth` 管理用户。DSN 必须通过安全注入提供；database 模式缺少 DSN 会在启动前失败，不会降级到 local。

## 9. 只读模式

`QRP_READ_ONLY=true` 用于只读数据库/API 场景：

- API 拒绝显式 DuckDB 写连接。
- `init` 不创建缺失目录。
- `doctor` 要求所需目录和 DuckDB 已存在且可读。
- `ensure_dirs`、默认回测任务/结果、稳健性结果、声明式策略存储和 PIT launcher 会显式拒绝写入。
- 写入型 pipeline、维护脚本和其他结果生成器不应在只读部署中启动。

只读开关不是操作系统权限的替代品。生产只读部署仍应使用文件系统只读挂载或只读账户作为第二层保护。

## 10. 兼容与迁移

未设置新变量时：

- 数据仍位于仓库 `data/`。
- DuckDB 仍是 `data/db/quant.db`。
- local 认证和现有稳定本地用户标识保持不变。
- API 默认监听 `127.0.0.1:8000`，CORS 默认 `*`。
- 旧路径变量 `QRP_DB_READ_ONLY`、`QRP_ATLAS_BACKTEST_RUNS_DIR`、`QRP_ATLAS_BACKTEST_TASKS_DIR`、`QRP_ATLAS_ROBUSTNESS_RUNS_DIR`、`QRP_ATLAS_DECLARATIVE_STRATEGIES_DIR` 继续作为兼容别名，但新部署应使用 `QRP_*` 新名称。
- 旧常量如 `DB_PATH`、`DATA_DIR` 和 `ensure_dirs` 继续存在，但值来自统一配置。

迁移到外部数据盘时先停止写入进程，由运维人员复制并校验数据，再设置 `QRP_DATA_DIR` 或高级覆盖项；应用不会自动搬迁、合并或删除已有数据。

## 11. 已知边界

- setup 不安装 Python、虚拟环境、systemd、Windows Service、Docker/Kubernetes、反向代理或防火墙规则。
- setup 不迁移已有数据，不下载行情，不执行 pipeline，不部署 PostgreSQL schema，不管理用户。
- setup 不提供网页设置页、远程配置中心或远程秘密管理。
- Windows 上配置文件保护依赖用户账户和 ACL；setup 只在 POSIX 上执行 `0600`。
- 本改动不提供动态配置中心、Vault/Consul、Docker 或 Kubernetes 编排；主库缺表迁移仅由显式 schema migration 脚本完成。
- 配置在进程内通过 `get_settings()` 缓存；修改环境后应重启进程。测试可调用 `reset_settings_cache()`。
- doctor 验证 PostgreSQL DSN 的格式和存在性，不主动连接远程 PostgreSQL，避免诊断命令产生外部副作用。
- doctor 不调用第三方数据服务；Tushare token 缺失只表示相关 pipeline 不可用。
- 日期化 PIT systemd units 是维护任务示例，不代表通用调度系统。
