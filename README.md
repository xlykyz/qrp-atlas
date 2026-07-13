# qrp-atlas

qrp-atlas（QRP）是一个面向个人 A 股交易研究的量化平台：以标准数据契约为基础，由指标层生成市场事实、策略层输出交易决策，再通过回测系统完成数据准备、成交模拟与结果分析。

> 当前状态：**QRP v1.0 核心架构已封版，后端核心链路已经实现，正在扩充数据、指标、策略、组合回测与产品交互能力。**
>
> 架构封版不代表功能冻结。后续能力应在既定模块边界内扩展，不再进行架构级重构。

完整架构说明见 [`docs/核心架构v1.0/QRP_v1.0_核心架构文档.md`](docs/核心架构v1.0/QRP_v1.0_核心架构文档.md)。

## 核心工作流

```text
外部数据源
    ↓
pipeline：采集、清洗、标准化
    ↓
contracts：字段、schema、映射、约定与入库校验
    ↓
DuckDB：标准事实库
    ↓
backtest runtime：读取数据、准备指标、运行策略
    ├── indicators：计算客观事实
    ├── strategies：输出 ENTER / HOLD / EXIT / NO_ACTION
    └── backtest engine：模拟成交、成本与绩效
    ↓
results / API
    ↓
web：配置、复盘与分析
```

核心业务抽象由低到高为：

```text
contracts → indicators → strategies
```

- `contracts` 定义“数据是什么”；
- `indicators` 描述“市场或标的已经发生了什么”；
- `strategies` 决定“面对这些事实应当做什么”；
- `backtest` 负责“把策略运行起来并模拟交易结果”。

## 当前能力

| 模块 | 已实现能力 |
| --- | --- |
| `contracts/` | 标准字段、DuckDB 表结构、主键与可空性、数据源映射、ticker/日期/市场规则、DataFrame 对齐与校验。 |
| `pipeline/` | 日线行情、估值与市值、复权因子、指数、涨跌停、停复牌、研报、机构调研等数据的正式入库链路。 |
| `indicators/` | MA5、收盘价相对 MA5 状态、连续站上/跌破天数、System B 基础状态、市场宽度与市场风险、横截面算子、正式因子生成与行业/市值中性化。 |
| `strategies/` | 统一策略模型、参数校验、版本化注册表、Python 内置策略、受限声明式策略、横截面 Top N 选股与 `cross_sectional_momentum_long_only` / `multifactor_long_only`。 |
| `backtest/` | 通用信号回测、策略运行适配、固定持有与动态退出、手续费/印花税/滑点、MAE/MFE、交易与 skipped 汇总。 |
| `backtest/results/` | 回测结果文件的加载、查询与响应模型。 |
| `api/` | 行情、复盘、回测结果等后端接口，以及临时只读能力会话。 |
| `web/` | React 复盘前端、个股复盘与回测分析页面。 |
| `tests/` | contracts、pipeline、indicators、strategies、backtest、API 等模块的自动化测试。 |

当前后端已经形成以下核心闭环：

```text
contracts
→ indicators
→ strategies
→ backtest runtime
→ backtest engine
```

策略定义与运行已经分离：策略只声明输入、参数和决策规则，不直接访问数据库；回测运行器负责读取数据、准备指标并将策略决策交给通用执行引擎。

## 当前边界与缺口

项目尚未完成完整产品闭环，当前重点不是再次调整架构，而是在现有架构内补齐能力密度：

- 扩充参数化时间序列、事件与残差指标，以及因子分析（IC / 分组收益等）；
- 增加事件驱动与残差相对价值等代表性内置策略；
- 补充财务报表、历史行业归属、历史成分与标准事件时间轴；
- 建立组合级现金、目标权重、调仓、资金曲线、回撤与风险归因；
- 加入 T+1、涨跌停不可成交、停牌、整数手、最低佣金等 A 股现实约束；
- 将策略目录、参数配置、回测任务创建与运行能力接入 API 和前端；
- 增加样本内外、walk-forward、参数稳健性与结果可复现能力。

现有回测结果查询与分析页面可以读取已有结果，但策略配置、任务创建、真实组合净值和完整运行闭环仍在建设中。

## 项目结构

```text
qrp-atlas/
├── src/qrp_atlas/
│   ├── contracts/              # 标准字段、schema、映射、约定与校验
│   ├── pipeline/               # 外部数据采集、清洗、标准化与入库
│   ├── indicators/             # 可复用客观指标与市场状态
│   ├── strategies/             # 策略定义、注册、校验与实现
│   │   ├── builtin/            # Python 内置策略
│   │   └── declarative/        # 受限声明式策略
│   ├── backtest/               # 回测运行器、通用执行引擎与结果服务
│   ├── api/                    # FastAPI 应用接口
│   └── config/                 # 路径、环境与运行配置
├── web/                        # React + Vite 前端
├── tests/                      # 自动化测试
├── docs/                       # 架构、设计、研究与开发文档
├── deploy/                     # 部署与临时访问配置
├── scripts/                    # 一次性工具和实验脚本
└── data/                       # 本地数据目录，通常不提交 Git
```

`src/qrp_atlas/AGENTS.md` 规定核心包的开发边界；修改核心模块前应先阅读该文件。

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

### 事实、决策与执行分离

- 计算结果属于客观市场事实时，放入 `indicators/`；
- 入场、持有、退出、排序和选股规则放入 `strategies/`；
- 成交、仓位、资金、成本、收益和风险计算放入 `backtest/`；
- HTTP 请求与响应编排放入 `api/`；
- 前端不直接访问 DuckDB，也不直接执行 Python 策略。

### 保持单向依赖

允许的主要依赖方向：

```text
pipeline → contracts
indicators → contracts
strategies → indicators / contracts
backtest runtime → contracts / indicators / strategies / engine
api → indicators / strategies / backtest / results
web → api
```

底层模块不得反向依赖上层模块，通用回测引擎不得内置任何具体策略知识。

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

每次新增模块或修改公共契约后，应运行完整测试，检查跨模块回归；开发过程中也可以先运行与变更范围匹配的测试目录。

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

## 数据目录

`data/` 用于本地运行数据，通常不提交 Git：

```text
data/
├── db/              # DuckDB 数据库
├── raw/             # 原始数据备份
├── canonical/       # 标准化数据备份
└── backtest_runs/   # 回测结果文件（如启用）
```

数据库结构应与 `src/qrp_atlas/contracts/` 保持一致。数据库文件、原始行情和生成结果不得作为普通代码提交。

## 文档入口

- [QRP v1.0 核心架构文档](docs/核心架构v1.0/QRP_v1.0_核心架构文档.md)
- [核心包说明](src/qrp_atlas/README.md)
- [核心包 Agent 规则](src/qrp_atlas/AGENTS.md)

## 许可证

个人项目。