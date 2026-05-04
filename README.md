# qrp-atlas

**市场解构工具包 · 量化分析与复盘**

一个用于量化分析与复盘的个人工具，用于将市场拆解成清晰可观察的结构，而非自动交易系统。

[![Python 3.13](https://img.shields.io/badge/Python-3.13-blue.svg)](https://www.python.org/downloads/)
[![DuckDB](https://img.shields.io/badge/DuckDB-0.10+-orange.svg)](https://duckdb.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.0+-red.svg)](https://streamlit.io/)

---

## 项目概述

**qrp-atlas** 是一个围绕 **数据结构 + 可视化复盘** 构建的研究工具。

### 核心能力

| 能力范围 | 实现方式 |
|---------|---------|
| 市场数据处理 | 东方财富 API 自动采集 |
| 量化分析 | 日线数据规范化与 enrichment |
| 可视化复盘 | Streamlit 交互式仪表盘 |

### 不涉及领域

- 自动交易执行
- 策略回测框架
- 交易信号生成

> **理念**：机器处理结构，人类负责叙事。

---

## 技术架构

```
数据流程：daily_snapshot → ingestion → DuckDB

核心原则：
- 可复用组件位于 src/
- 一次性脚本限于 scripts/
- 本地数据（data/）不纳入版本控制
```

### 技术栈

| 组件 | 技术选型 | 用途 |
|-----|---------|-----|
| 编程语言 | Python 3.13 | 核心实现 |
| 数据库 | DuckDB | OLAP 分析查询 |
| 数据处理 | Pandas | DataFrame 操作 |
| 可视化 | Streamlit | 交互式 Web 界面 |
| 数据源 | 东方财富 API | 市场数据采集 |

### 项目结构

```
qrp-atlas/
├── src/qrp_atlas/          # 核心库模块
│   ├── config/             # 配置管理（SSOT）
│   ├── contracts/          # 数据库 Schema 定义（SSOT）
│   ├── pipeline/           # 数据处理管道
│   │   └── daily_update/   # 每日更新工作流
│   └── sources/            # 外部数据源适配器
├── scripts/                # 一次性工具脚本
├── web/                    # Streamlit 应用
│   └── pages/              # 多页面导航
├── docs/                   # 架构文档
├── data/                   # 本地 DuckDB 存储（gitignored）
└── pyproject.toml          # 项目元数据
```

---

## 数据管道

### 每日更新工作流

1. **采集（Fetch）**：从东方财富 API 获取每日快照
2. **清洗（Clean）**：标准化字段名和格式
3. ** Enrich（Enrich）**：与现有规范数据交叉引用
4. **加载（Load）**：持久化到 DuckDB 进行分析查询

### 数据规范

- 所有表结构定义在 `qrp_atlas.contracts`
- 所有文件路径通过 `qrp_atlas.config` 管理
- 不经 contracts 更新则不修改数据库 schema

---

## 开发进度

**早期开发阶段** — 核心基础设施进行中

当前里程碑：
- [x] 东方财富数据采集
- [x] DuckDB 存储层
- [x] 每日更新管道
- [x] 基础 Streamlit 界面
- [ ] 高级可视化功能
- [ ] 扩展分析工具

---

## 快速开始

### 环境要求

- Python 3.13+
- DuckDB

### 安装

```bash
pip install -e .
```

### 配置

复制 `.env.example` 为 `.env`，根据需要配置数据源凭证。

### 运行

```bash
streamlit run web/app.py
```

---

## 许可证

个人项目，保留所有权利。

---

*最后更新：2026-05-04*
