# qrp-atlas

qrp-atlas 是个人 A 股交易复盘与市场数据分析系统。

项目目标不是做一个通用行情终端，而是围绕个人交易系统，沉淀稳定的本地数据底座、复盘页面和后续量化回测/分析能力。

当前核心能力：

- 采集并清洗 A 股行情、涨跌停池、调研公告、个股研报、行业研报等数据
- 将多源数据标准化后写入本地 DuckDB
- 通过 FastAPI 对外提供查询接口
- 通过 Web 前端查看数据、做复盘、辅助后续交易系统迭代

---

## 架构边界

当前项目按以下边界组织：

```text
外部数据源
    ↓
pipeline/        数据采集、清洗、标准化、入库
    ↓
contracts/       pipeline 与 DuckDB 之间的强制数据契约层
    ↓
DuckDB           本地标准事实库
    ↓
api/             查询数据库并组织响应
    ↓
web/             前端展示与交互
```

### contracts/ 的定位

`src/qrp_atlas/contracts/` 是本项目的数据契约层，主要约束 **pipeline → database** 的入库边界。

它负责：

- 标准字段名
- 标准表结构
- 字段属性与主键
- 外部数据源字段映射
- 入库前 schema 对齐
- 入库前类型转换和校验
- A 股底层市场规则，例如 ticker 标准化、交易所识别、涨跌停规则

它不负责：

- API 普通查询
- API 值级更新
- 前端展示逻辑
- 交易策略规则
- 回测撮合逻辑
- 临时脚本

一句话：

```text
contracts/ 管“数据结构与入库口径”
pipeline/ 管“数据生产流程”
DuckDB 管“标准事实存储”
api/ 管“数据出库与值级操作”
web/ 管“展示”
strategy/backtest 管“交易逻辑”
scripts/ 管“临时实验”
```

---

## 项目结构

```text
qrp-atlas/
├── src/qrp_atlas/
│   ├── api/                    # FastAPI 路由与数据查询接口
│   ├── config/                 # 路径、环境变量、配置
│   ├── contracts/              # 数据契约层：fields/schema/mappings/conventions/validate
│   └── pipeline/               # 正式数据管线
│       ├── daily_update/       # 日更行情管线
│       ├── cninfo/             # 调研公告数据
│       ├── research_report/    # 个股研报数据
│       ├── research_industry/  # 行业研报数据
│       └── duckdb_store.py     # DuckDB 存储辅助入口
├── web/                        # React 前端项目
├── data/                       # 本地数据目录，通常不提交 git
├── deploy/                     # 部署相关配置
└── scripts/                    # 临时脚本、一次性工具、实验代码
```

说明：

- `pipeline/` 是正式入库链路，应遵循 `contracts/`。
- `api/` 主要从 DuckDB 取数，不要求强制通过 `contracts/` 包装。
- `scripts/` 是临时脚本目录，不作为正式数据契约的一部分。
- `web/` 是前端独立开发目录，但仍属于总仓库管理。

---

## contracts/ 模块说明

| 模块 | 职责 |
|------|------|
| `fields.py` | 标准字段名常量 |
| `schema.py` | DuckDB 标准表结构、字段类型、主键、建表 SQL |
| `mappings.py` | 外部数据源字段到内部标准字段的映射 |
| `conventions.py` | A 股底层市场规则与数据解释规则 |
| `validate.py` | 入库前 DataFrame schema 对齐、类型转换、字段校验 |
| `__init__.py` | contracts 的统一公开导入入口 |

正式 pipeline 写入 DuckDB 前，原则上应走：

```text
apply_mapping
→ align_to_schema
→ quick_validate
→ insert/upsert DuckDB
```

---

## 数据管线

当前正式管线包括：

- `daily_update`：日更行情快照
- `cninfo`：调研公告/机构调研数据
- `research_report`：个股研报数据
- `research_industry`：行业研报数据
- `zt_pool` / `dt_pool`：涨停池 / 跌停池数据存储入口

入库原则：

1. 外部字段映射集中在 `contracts.mappings`
2. 表结构集中在 `contracts.schema`
3. 字段名集中在 `contracts.fields`
4. A 股底层规则集中在 `contracts.conventions`
5. 写库前通过 `contracts.validate` 对齐并校验

---

## API 与前端

API 层主要职责是读取 DuckDB 并为前端组织响应。

API 普通查询不需要强制走 `contracts.validate`，因为数据库已经是经过 pipeline/contracts 约束后的标准事实源。

API 只有在以下情况才轻量引用 contracts：

- 需要统一 ticker / index_code 等查询参数格式
- 需要复用 A 股底层市场规则
- 需要避免高频字段名漂移
- 新增整行结构化记录时需要参考 schema

前端位于 `web/`，用于数据展示、复盘页面和后续分析功能开发。

---

## 技术栈

| 层 | 技术 |
|----|------|
| 数据存储 | DuckDB |
| 数据处理 | Python 3.13+, pandas |
| 数据源 | akshare、tushare 及其他正式 pipeline 数据源 |
| 后端 API | FastAPI |
| 前端 | React + Vite |
| 本地配置 | `.env` |

---

## 常用命令

### Python 环境

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

如果系统没有 `python` 命令，可使用项目虚拟环境中的解释器：

```bash
.venv/bin/python -m compileall src
```

### 编译检查

```bash
python -m compileall src
```

### 手动运行日更管线

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

---

## 数据目录

`data/` 通常用于本地运行数据，不建议提交 git。

常见内容：

```text
data/
├── db/          # DuckDB 数据库文件
├── raw/         # 原始数据备份
└── canonical/   # 清洗后的规范化数据备份
```

---

## 开发原则

### 1. pipeline 入库必须尊重 contracts

正式 pipeline 不应绕过 contracts 直接写入 DuckDB。

新增正式数据源时，应先补齐：

```text
fields.py
schema.py
mappings.py
conventions.py（如涉及 A 股底层规则）
validate.py（如需要新的通用校验能力）
```

### 2. 不要把策略逻辑放进 contracts

以下内容不属于 contracts：

- 买入规则
- 卖出规则
- 仓位规则
- 节点 M / W
- 主线退潮判断
- 回测撮合逻辑
- 复盘页面展示偏好

这些应放在未来的 strategy / backtest / domain 层。

### 3. API 不做过度 contracts 化

API 纯查询以 DuckDB 现有 schema 为事实来源。

不要为了形式上的 SSOT，把 API 普通查询全部改成 schema 驱动，也不要把值级更新改造成 DataFrame validate 流程。

### 4. scripts/ 保持临时性

`scripts/` 允许存在临时字段映射、临时 SQL、一次性修复逻辑。

除非明确要求，不要为了 contracts 重构 `scripts/`。

---

## 当前阶段

当前阶段的重点是：

```text
建立稳定的数据入库契约
沉淀可复用的本地市场数据库
支撑复盘页面和未来回测/分析能力
```

后续可在此基础上继续建设：

- 通用回测底座
- 交易复盘自动化
- 节点/市场状态分析
- 专属交易系统统计与验证

---

## 许可证

个人项目。