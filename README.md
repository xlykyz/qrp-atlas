# qrp-atlas

qrp-atlas（QRP）是一个面向个人 A 股交易研究的量化平台：以标准数据契约为基础，由指标层生成可复用事实，策略层输出标准化决策，回测系统负责数据准备、现实成交模拟、研究评价与结果产品化。

> 当前状态：**QRP v1.0 后端产品基线已经完成最终验收。**
>
> 这表示核心架构、代表性策略链路、组合回测、标准结果、任务运行、比较、重放和多用户隔离已经闭环；不表示产品已经完成正式发布。当前尚未创建 `v1.0.0` tag 或 GitHub Release，前端交互、部署运维和日常使用体验仍需继续完善。
>
> v1.0 架构已经封版。后续能力应在既定模块边界内扩展，不再进行无必要的顶级模块拆分或架构级重构。

完整架构说明见 [`docs/核心架构v1.0/QRP_v1.0_核心架构文档.md`](docs/核心架构v1.0/QRP_v1.0_核心架构文档.md)。v1.0 产品终点、任务与验收记录见 [`docs/QRP产品蓝图v1.0/`](docs/QRP产品蓝图v1.0/)。

## 核心工作流

```text
外部数据源
    ↓
pipeline：采集、清洗、标准化、PIT 版本化与入库
    ↓
contracts：字段、schema、映射、时间语义与校验
    ↓
DuckDB：市场、研究与运行事实
    ↓
backtest runtime / product service
    ├── indicators：指标、特征、因子与状态事实
    ├── strategies：内置或声明式交易决策
    ├── backtest engine：组合资金与现实成交模拟
    └── research：因子、事件、残差与稳健性评价
    ↓
standard results：结果包、快照、比较与重放
    ↓
API
    ↓
web：策略配置、任务运行、复盘与结果分析
```

核心量化抽象由低到高为：

```text
contracts → indicators → strategies
```

- `contracts` 定义“数据是什么”；
- `indicators` 描述“市场、题材或标的已经发生了什么”；
- `strategies` 决定“面对这些事实应当做什么”；
- `backtest` 负责“准备输入、模拟执行并评价结果”；
- `backtest/product` 将策略、任务、运行记录和标准结果组织成可调用产品链路。

## 已实现能力

| 模块 | 当前能力 |
| --- | --- |
| `contracts/` | 标准字段、DuckDB 表结构、主键与可空性、数据源映射、ticker/日期/市场约定、PIT 时间语义、DataFrame 对齐与校验。 |
| `pipeline/` | 日线行情、估值市值、复权、指数、涨跌停、停复牌、研报、机构调研、财务报表、历史行业归属、历史指数成分和业绩预告事件等正式链路；支持 PIT 历史回补、清洗、加载、审计与恢复。 |
| `indicators/` | 参数化技术指标、System B 状态、市场宽度与风险、经典趋势/动量/波动/量价指标、横截面因子与算子、行业/市值中性化、事件指标和滚动残差指标。`Factor ⊂ Indicator`。 |
| `strategies/` | 统一定义、参数校验、版本化注册表、经典时间序列策略、横截面选股策略、事件漂移、残差均值回归，以及受限声明式策略。 |
| `backtest/research/` | forward return、IC/Rank IC、因子分组、目标暴露、事件研究、残差研究、行业残差和 walk-forward / 参数稳健性评价。 |
| `backtest/` | 策略运行适配、共享现金组合、目标权重调仓、T+1、停牌/涨跌停不可成交、整数手、手续费/印花税/滑点、订单/成交/持仓、MAE/MFE、净值、回撤与绩效。 |
| `backtest/product/` | 真实策略目录、任务持久化与状态机、经典/横截面/事件/声明式产品链、运行历史、比较、重放和 owner 隔离。 |
| `backtest/results/` | 原子写入和加载标准结果包，包含配置、摘要、净值、订单、成交、交易、目标、暴露、基准/超额、诊断和可复现快照。 |
| `auth/` / `users/` | 本地单用户与 PostgreSQL 数据库认证、内部用户模型、会话和 FastAPI 当前用户依赖；QRP 业务数据通过内部 `user_id` 隔离。 |
| `api/` | 认证、行情与复盘、策略目录、回测任务、运行结果、比较、重放等接口，以及独立的临时只读访问工具。 |
| `web/` | React + Vite 前端；支持个股复盘、数据库预览、复盘日志、真实策略目录与回测任务工作流、结果分析和多运行对比。 |
| `tests/` | 覆盖 contracts、pipeline、indicators、strategies、backtest、产品链、API、认证、owner 隔离和发布级验收场景。 |

v1.0 已经形成以下真实产品闭环：

```text
策略目录
→ 参数与股票池配置
→ 回测任务
→ PIT 数据准备
→ 指标与策略运行
→ A 股组合执行
→ 标准结果包
→ 历史查询 / 分析 / 比较 / 重放
```

代表性验收链路覆盖经典时间序列、横截面、事件驱动和声明式策略。策略只声明输入、参数和决策规则，不直接访问数据库；运行器负责数据准备，通用引擎负责执行，结果层负责持久化和复现。

## 当前边界与后续重点

当前主要缺口已经从“后端能力是否存在”转向“产品是否足够好用、稳定和易维护”：

- 前端仍较薄弱，需要继续完善信息架构、策略配置、任务反馈、运行历史、结果解释、错误态和页面重构；
- 前端主包仍偏大，需要按页面和业务能力逐步拆分加载；
- 需要补齐正式部署、备份恢复、运行监控、数据质量告警和持续更新运维；
- 需要用真实日常研究工作持续验证策略目录、默认参数、数据覆盖和结果解释；
- 动态产业—题材库目前完成的是 `docs/动态产业题材库v0.1/` 预研与契约设计，尚未进入正式运行代码；
- v1.0 不包含实盘自动交易、QMT 下单、强化学习、高频交易或分布式回测。

因此，“v1.0 后端验收完成”与“可以作为成熟产品发布”是两个不同状态。正式发布前仍应进行前端产品化、部署验证和真实使用验收。

## 项目结构

```text
qrp-atlas/
├── src/qrp_atlas/
│   ├── contracts/              # 标准字段、schema、映射、约定与校验
│   ├── pipeline/               # 外部数据采集、标准化、PIT 回补与入库
│   ├── indicators/             # 指标、特征、因子与客观状态
│   ├── strategies/             # 策略定义、注册、校验与实现
│   │   ├── builtin/            # Python 内置策略
│   │   └── declarative/        # 受限声明式策略
│   ├── backtest/
│   │   ├── research/           # 因子、事件、残差与稳健性研究
│   │   ├── product/            # 目录、任务与真实产品运行链
│   │   └── results/            # 标准结果、查询、比较与重放
│   ├── auth/                   # 身份认证与会话
│   ├── users/                  # 内部用户实体与状态
│   ├── api/                    # FastAPI 应用接口
│   └── config/                 # 路径、环境与运行配置
├── web/                        # React + Vite 前端
├── tests/                      # 自动化与发布级验收测试
├── docs/                       # 架构、蓝图、研究与任务文档
├── deploy/                     # 部署与 PostgreSQL 初始化资源
├── scripts/                    # 一次性工具和维护脚本
├── tools/                      # 独立开发辅助工具
└── data/                       # 本地数据与运行结果，通常不提交 Git
```

`src/qrp_atlas/AGENTS.md` 规定核心包的长期开发边界；子目录存在更具体的 `AGENTS.md` 时，应同时遵守距离目标文件最近的规则。仓库根目录的 `AGENTS.md` 是开发机本地配置，不提交 Git。

## 关键架构规则

### 数据入库必须经过 contracts

正式 pipeline 写入数据库前，原则上执行：

```text
外部字段映射
→ schema 对齐
→ 类型标准化
→ 契约校验
→ insert / upsert DuckDB
```

标准字段、表结构、主键、映射和通用市场规则不得在 pipeline 内重复定义。

### 事实、决策、执行与评价分离

- 客观指标、特征和因子放入 `indicators/`；
- 入场、持有、退出、排序、选股和目标权重规则放入 `strategies/`；
- 成交、仓位、资金、成本、收益和风险计算放入 `backtest/`；
- forward return、IC、事件收益和稳健性评价放入 `backtest/research/`；
- 任务、运行、比较和重放编排放入 `backtest/product/` 与 `backtest/results/`；
- HTTP 请求与响应编排放入 `api/`；
- 前端不直接访问 DuckDB，也不直接执行 Python 策略。

### 保持单向依赖

```text
pipeline → contracts
indicators → contracts
strategies → indicators / contracts
backtest runtime → contracts / indicators / strategies / engine
backtest product → runtime / research / results
api → auth / users / indicators / strategies / backtest / results
web → api
```

底层模块不得反向依赖上层模块，通用回测引擎不得内置任何具体指标或策略知识。

## 本地开发

项目要求 Python 3.13 或更高版本。

### 创建环境

```bash
python -m venv .venv
source .venv/bin/activate          # Linux / macOS
# .venv\Scripts\Activate.ps1       # Windows PowerShell
pip install -e ".[test]"
```

### 运行测试

```bash
python -m pytest
```

新增模块、修改公共契约或准备合并运行逻辑时，应运行完整测试检查跨模块回归；开发过程中可以先运行与变更范围匹配的测试目录。纯文档任务可以不运行测试，但应在交付说明中明确记录。

### 运行日更管线

```bash
python -m qrp_atlas.pipeline.daily_update.run
python -m qrp_atlas.pipeline.daily_update.run --date 2026-06-01
```

### 前端开发

```bash
cd web
npm install
npm run dev
npm run build
```

默认认证模式为本地单用户模式；数据库认证必须通过环境变量显式启用。具体说明见 [`docs/用户与认证/README.md`](docs/用户与认证/README.md)。

## 运行配置与部署

QRP v1.0 的运行配置统一由 `qrp_atlas.config.settings` 解析。未配置时仍使用仓库内 `data/`，因此现有本地开发方式保持兼容；生产部署可把代码、运行目录和持久数据完全分离。

配置优先级固定为：

```text
显式参数 / qrp-atlas-config --set
> 进程环境变量
> QRP_ENV_FILE 或仓库根 .env
> 稳定默认值
```

### 本地默认开发模式

```bash
python -m qrp_atlas.config show
python -m qrp_atlas.config doctor
python -m qrp_atlas.config init
qrp-atlas-api
```

`show` 只显示脱敏后的最终配置；`doctor` 不写文件并以非零状态报告阻塞错误；`init` 幂等创建目录，但不会创建或覆盖数据库。

### Windows 自定义数据目录

```powershell
$env:QRP_HOME = 'D:\qrp-runtime'
$env:QRP_DATA_DIR = 'E:\qrp-data'
$env:QRP_API_HOST = '127.0.0.1'
python -m qrp_atlas.config doctor
python -m qrp_atlas.config init
```

相对路径始终按仓库根解析，不依赖 PowerShell 当前目录。

### Linux systemd 部署

通用 unit 示例位于 `deploy/qrp-atlas-api.service`，默认采用 `/opt/qrp-atlas` 代码目录和 `/etc/qrp-atlas/qrp-atlas.env` 配置文件。数据位置由服务环境中的 `QRP_DATA_DIR` 决定，不要求放在代码仓库内。启动服务前应以服务用户运行 `qrp-atlas-config doctor` 和 `init`。

### PostgreSQL 认证模式

```env
QRP_RUNTIME_ENV=production
QRP_AUTH_MODE=database
QRP_AUTH_DATABASE_URL=postgresql://USER:PASSWORD@db.example.com:5432/qrp_atlas
QRP_AUTH_SESSION_TTL_SECONDS=86400
```

`database` 模式缺少 DSN 会立即失败且不会降级到本地认证。真实 DSN、Tushare token 和带认证信息的代理 URL 必须由安全环境注入，不得提交、打印或写入诊断结果。正式环境还应显式收紧 `QRP_API_CORS_ORIGINS`。

旧的 `QRP_DB_READ_ONLY` 和 `QRP_ATLAS_*_DIR` 路径变量仍作为兼容别名，但新部署应使用 `.env.example` 中的 `QRP_*` 名称。应用不会自动迁移已有数据。

完整变量清单、路径派生、Windows/Linux 示例、只读模式、systemd 和迁移边界见 [运行配置与部署文档](docs/runtime-configuration.md)。

## 数据目录

`data/` 用于本地运行数据，通常不提交 Git：

```text
data/
├── db/                  # DuckDB 数据库
├── raw/                 # 原始数据备份
├── canonical/           # 标准化数据备份
├── state/               # 回补、审计和运行状态
├── backtest_tasks/      # 回测任务记录
└── backtest_runs/       # 标准回测结果包
```

数据库结构应与 `src/qrp_atlas/contracts/` 保持一致。数据库文件、原始行情、运行结果和本地凭证不得作为普通代码提交。

## 文档入口

- [QRP v1.0 核心架构](docs/核心架构v1.0/QRP_v1.0_核心架构文档.md)
- [QRP v1.0 产品蓝图与验收](docs/QRP产品蓝图v1.0/README.md)
- [核心包说明](src/qrp_atlas/README.md)
- [核心包 Agent 规则](src/qrp_atlas/AGENTS.md)
- [前端说明](web/README.md)
- [临时只读远程访问工具](tools/remote_access/README.md)

## 许可证

个人项目。