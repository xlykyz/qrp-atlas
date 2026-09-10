# QRP Atlas 治理体检台账

> 文档性质：治理基线台账（活文档，随治理推进更新）
> 体检日期：2026-09-10
> 代码基线：`develop/v1.1` @ `d991880`
> 覆盖维度：代码架构 / 文档体系 / 生产一致性 / 开发流程
> 调研方式：**只读静态体检**，未修改任何代码，未连接生产服务器
> 说明：本台账不采用 `docs/项目诊断报告.html` 作为输入或依据

---

## 0. 如何使用这份台账

这份台账是**治理的基线快照与进度看板**，不是修复方案。它的作用是回答三个问题：

1. **现状到底有多差？** → 见 §1 量化基线
2. **哪些要先动？** → 见 §2 严重度总览与 §7 立即行动清单
3. **修到哪一步了？** → 见各条目的 `状态` 字段

**条目格式约定**

| 字段 | 含义 |
| --- | --- |
| `ID` | `A/B/C/D-NN`，A=代码架构，B=文档体系，C=生产一致性，D=开发流程 |
| `严重度` | `P0` 阻断级（让治理无法进行或存在生产/数据事故风险）；`P1` 高（直接侵蚀架构边界、造成系统性返工）；`P2` 中（影响可维护性与协作效率）；`P3` 低（整洁性） |
| `状态` | `待处理` / `进行中` / `已关闭` / `已接受（不修）` |
| `证据` | 文件路径 + 行号，或量化事实，全部可复核 |
| `影响面` | 该问题会伤害什么 |
| `修复成本` | 低 / 中 / 高（综合改动范围、联动方、验证难度） |
| `建议动作` | 一句话方向，详细方案另出《治理路线图》 |

**重要边界**：凡涉及生产服务器的结论，本台账仅基于仓库内文件做静态核查，**未实测**。相关条目在 §6 集中标注为证据缺口。

---

## 1. 量化基线

治理需要仪表盘。以下是本次体检测得的基线数字，后续每次复检应与本节对比。

### 1.1 规模

| 范围 | 文件数 | 行数 |
| --- | ---: | ---: |
| `src/qrp_atlas/`（.py，含内容文件） | 347 | **91,745** |
| `tests/`（.py） | 153 | 45,271 |
| `scripts/` | 40 | 4,703 |
| `deploy/` | 25 | 1,065 |

按顶层模块（行数降序）：`pipeline` 39,000（**占 src 42.5%**）、`backtest` 16,336、`indicators` 11,567、`strategies` 6,565、`contracts` 5,672、`orchestration` 3,280、`api` 2,920、`config` 2,914、`stock_collections` 1,666、`jobs_cli.py` 917、`auth` 566、`users` 215、`database` 126。

### 1.2 代码形态

| 指标 | 数值 |
| --- | ---: |
| `src/` 中 >= 500 行的 Python 文件 | **58 个**（占 16.7%，合计约 55,600 行 ≈ src 总行数的 60%） |
| 其中 >= 900 行 | **18 个** |
| 最大文件 | [schema.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/contracts/schema.py) 1,880 行 |
| 估算函数体 > 150 行的函数 | **35 个** |
| 最长函数 | [calculate_theme_ranking](file:///e:/projects/qrp-atlas/src/qrp_atlas/indicators/system_b/theme_ranking.py#L324) 约 674 行 |
| 单文件含 > 200 行函数最多的文件 | [backtest/product/service.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/backtest/product/service.py)（4 个） |
| "仅一行 `# TODO`"的占位文件 | **11 个**（`src/` 6 个 + `scripts/` 5 个） |
| `FIXME` / `HACK` / `deprecated` 标记 | 0 |

### 1.3 测试

| 指标 | 数值 |
| --- | ---: |
| 含 `def test_` 的测试文件 | 145 |
| `def test_` 函数总数 | **1,331** |
| 测试/源码文件比 | 0.44 |
| 覆盖率配置与阈值 | **不存在** |
| 离线可独立运行 | 是（临时/内存 DuckDB + mock，仅 2 处 POSIX 平台跳过） |

### 1.4 数据与编排

| 指标 | 数值 |
| --- | ---: |
| `contracts` 中 `ALL_TABLES` 定义的表 | **58 张** |
| `deploy/duckdb/` 迁移实际建表 | 27 张 |
| **有定义但无迁移文件的表** | **31 张** |
| 编排定义文件数量 | **4 份**（条目数 29 / 2 / 6 / 0，互不一致） |
| 源码注册的 `PipelineContract` | **38 条** |
| 已注册 Contract 但不在编排注册表中的 | **16 条** |
| 在编排注册表中但无对应 Contract 的 | **7 条** |

### 1.5 维度评级（主观判断，依据为上述事实）

| 维度 | 评级 | 主要依据 |
| --- | --- | --- |
| 代码架构 | **红** | 58 个超大文件、35 个超长函数、成套复制粘贴、领域边界缺失、通用层 8 处特化 |
| 文档体系 | **红** | Task 编号同名不同义、SSOT 四处并存、索引与磁盘严重不符、无校验机制 |
| 生产一致性 | **红** | 4 份编排定义互相矛盾、31 张表无迁移、生产权威未入版本库 |
| 开发流程 | **红** | 零 CI、零 pre-commit、架构门禁仅覆盖 12 条规则中的 2 条 |

> 四个维度全部为红，但这不代表"项目完蛋"。评级衡量的是**治理缺口**，不是业务价值。业务建模质量（状态机、轮次、分池、排名、组合约束）与架构文档的意图质量明显高于这些缺口所暗示的水平。

---

## 2. 问题总览

共登记 **37 条**问题：P0 **7 条**、P1 **13 条**、P2 **13 条**、P3 **4 条**。

维度分布：代码架构 **10 条**（A-01~A-10）、文档体系 **8 条**（B-01~B-08）、生产一致性 **10 条**（C-01~C-10）、开发流程 **9 条**（D-01~D-09）。

| ID | 严重度 | 维度 | 问题 | 成本 | 状态 |
| --- | --- | --- | --- | --- | --- |
| A-01 | **P0** | 代码架构 | 通用层无机械化架构门禁，12 条禁止依赖只覆盖 2 条 | 低 | 待处理 |
| A-02 | **P0** | 代码架构 | System B 领域被技术分层切碎，`system_b` 在 5 个顶层目录重复 | 高 | 待处理 |
| A-03 | **P0** | 代码架构 | 通用层被 System B 特化，8 处硬编码污染 | 中 | 待处理 |
| C-01 | **P0** | 生产一致性 | 4 份编排定义互相矛盾，无单一权威定义 | 中 | 待处理 |
| C-02 | **P0** | 生产一致性 | 生产权威 manifest 未纳入 git，无版本与审计基线 | 中 | 待处理 |
| D-01 | **P0** | 开发流程 | 完全无 CI/CD，无 pre-commit，无任何 git hook | 低 | 待处理 |
| D-02 | **P0** | 开发流程 | 蓝图规定的 6 道发布闸门与 PR 强制门禁全部无自动化执行 | 中 | 待处理 |
| A-04 | P1 | 代码架构 | 35 个函数估算超 150 行，最长约 674 行 | 中 | 待处理 |
| A-05 | P1 | 代码架构 | 58 个文件 >= 500 行，18 个 >= 900 行 | 中高 | 待处理 |
| A-06 | P1 | 代码架构 | 两个契约模块之间成套复制粘贴 | 中 | 待处理 |
| A-07 | P1 | 代码架构 | 跨模块重复实现（`_is_missing` 6 处等至少 16 组） | 低中 | 待处理 |
| B-01 | P1 | 文档体系 | `Task07` 在 v1.0/v1.1 同名不同义 | 中 | 待处理 |
| B-02 | P1 | 文档体系 | SSOT 四处并存主张，无优先级与维度说明 | 低中 | 待处理 |
| B-03 | P1 | 文档体系 | 蓝图 README 索引与磁盘严重不符，且无校验机制 | 低 | 待处理 |
| C-03 | P1 | 生产一致性 | 16 条已注册 Contract 不在编排注册表；7 条注册表条目无 Contract | 中 | 待处理 |
| C-04 | P1 | 生产一致性 | 31 张表无迁移文件，靠代码运行时建表 | 中高 | 待处理 |
| C-09 | P1 | 生产一致性 | repo 与生产已漂移（历史文档陈述，待实测） | 需协同 | 待处理 |
| D-03 | P1 | 开发流程 | 无 lint / 类型检查 / 格式化配置 | 低 | 待处理 |
| D-04 | P1 | 开发流程 | 无覆盖率配置与阈值 | 低 | 待处理 |
| D-05 | P1 | 开发流程 | 单一提交者，无第二人 review 机制 | 高 | 待处理 |
| A-08 | P2 | 代码架构 | 11 个 pipeline 子包共享完全一致的 `clean/fetch/load/run` 四件套 | 中 | 待处理 |
| A-09 | P2 | 代码架构 | 11 个"仅一行 `# TODO`"的占位文件 | 低 | 待处理 |
| A-10 | P2 | 代码架构 | 代码量高度集中（16.7% 文件承载 60% 代码；`pipeline` 占 42.5%） | 高 | 待处理 |
| B-04 | P2 | 文档体系 | 命名体系四种并存、大小写混用 | 低 | 待处理 |
| B-05 | P2 | 文档体系 | 文档与代码状态冲突（`api.md` 的 M1/M2/M3 身份语义） | 低 | 待处理 |
| B-06 | P2 | 文档体系 | `docs/` 根目录散落文档未被任何索引收录 | 低 | 待处理 |
| B-07 | P2 | 文档体系 | 文档目录内混入可执行脚本 | 低 | 待处理 |
| C-05 | P2 | 生产一致性 | `deploy/duckdb/` 迁移编号重号（`003` 出现两次），缺 `001` | 低 | 待处理 |
| C-06 | P2 | 生产一致性 | `database/schema.py` 与 `contracts/schema.py` 对 3 张表双份定义 | 低 | 待处理 |
| C-07 | P2 | 生产一致性 | deploy 的 pipeline `*.example` 引用已废弃入口 | 低 | 待处理 |
| D-06 | P2 | 开发流程 | 存在直接提交长期分支 `develop/v1.1` 的痕迹 | 低 | 待处理 |
| D-07 | P2 | 开发流程 | 变更控制无结构化台账 | 低 | 待处理 |
| D-08 | P2 | 开发流程 | 文档索引无校验机制 | 低 | 待处理 |
| B-08 | P3 | 文档体系 | 同一主题被拆到两个命名体系不同的目录 | 低 | 待处理 |
| C-08 | P3 | 生产一致性 | `QRP_PIPELINE_RUNTIME_DIR` 不在 `SUPPORTED_ENV_VARS` 中 | 低 | 待处理 |
| C-10 | P3 | 生产一致性 | 系统表创建双轨（迁移与 `ensure_schema()` 并存） | 低 | 待处理 |
| D-09 | P3 | 开发流程 | `database`、`jobs_cli` 无对应测试目录 | 低 | 待处理 |

---

## 3. 维度 A：代码架构

> **该维度结构性问题**：仓库的一级切分维度**只有"技术分层"，没有"业务领域"**。这导致一个业务子系统（System B）被切成 5 段撒进 5 个顶层目录；同时因为没有机械化门禁，通用层被逐步特化。

### A-01 ｜ 通用层无机械化架构门禁 ｜ **P0** ｜ 待处理

- **问题**：`src/qrp_atlas/AGENTS.md` 用约 12 条显式规则定义了禁止依赖方向，但只有 **2 条**被 pytest 机械检查；其余全部依赖开发者自觉。
- **证据**：
  - 规则：[AGENTS.md L90-L103](file:///e:/projects/qrp-atlas/src/qrp_atlas/AGENTS.md#L90-L103)「禁止的典型依赖」
  - 已覆盖：[test_architecture.py L24-L42](file:///e:/projects/qrp-atlas/tests/orchestration/test_architecture.py#L24-L42)（orchestration 反向依赖）、[test_indicators_dependency.py L8-L17](file:///e:/projects/qrp-atlas/tests/contracts/test_indicators_dependency.py#L8-L17)（contracts 不 import indicators）
  - 未覆盖：`indicators → stock_collections`、`strategies → backtest`、`backtest engine → 具体策略`、`api → frontend` 等约 10 条
  - 无专用工具：全仓库无 `import-linter` / `pydeps` / `grimp` / `tach`
- **影响面**：所有后续开发。这是 A-02/A-03 得以发生的**直接机制原因**——约束写在文档里，不写在检查里。
- **修复成本**：低
- **建议动作**：把 AGENTS.md 的禁止依赖清单转成一个可执行检查，先以 warning 形式输出清单，收敛后再升级为失败。

### A-02 ｜ System B 领域被技术分层切碎 ｜ **P0** ｜ 待处理

- **问题**：System B 是一个业务领域，但**没有自己的顶层命名空间**，按技术层被切碎后撒进 5 个顶层目录。
- **证据**（同一个域名 `system_b` 重复出现于）：
  - [contracts/system_b.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/contracts/system_b.py)
  - `indicators/system_b/`（8 文件 3,278 行）
  - `pipeline/system_b/` + `system_b_asset_rank/` + `system_b_episode/` + `system_b_pools/` + `system_b_theme_rank/`（18 文件约 4,366 行）
  - [strategies/builtin/](file:///e:/projects/qrp-atlas/src/qrp_atlas/strategies/builtin)（`system_b_basic/authorization/decision/portfolio.py`，1,400 行）
  - [api/routes/system_b.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/api/routes/system_b.py) + `api/schemas/system_b.py`
  - **对照**：`stock_collections/` 是正确的领域式顶层模块形态，说明团队知道该怎么做，但只做了一次
  - **额外证据**：`strategies/builtin/` 内通用策略（`classic.py`、`cross_section.py`、`residual.py`）与 System B 专属策略混居，属领域内聚失败
- **影响面**：System B 的改动需要跨 5 个目录；无法独立启停；无法被第二个策略复用；边界无法被机械校验。
- **修复成本**：高
- **建议动作**：把 System B 收拢为单一命名空间（如 `qrp_atlas/system_b/`），通用层改为从扩展点加载；作为独立任务分支推进，不夹带功能交付。

### A-03 ｜ 通用层被 System B 特化（8 处） ｜ **P0** ｜ 待处理

- **问题**：QRP 通用层被写入 `system_b` 专用字面量、专用字段、专用分支。
- **证据**（8 个层面）：

  | 层 | 位置 | 污染形式 |
  | --- | --- | --- |
  | strategies | [registry.py L88-L96](file:///e:/projects/qrp-atlas/src/qrp_atlas/strategies/registry.py#L88-L96) | 默认注册表 import 并注册 3 个 System B 策略 |
  | strategies | [validation.py L323-L352](file:///e:/projects/qrp-atlas/src/qrp_atlas/strategies/validation.py#L323-L352)、[L494-L500](file:///e:/projects/qrp-atlas/src/qrp_atlas/strategies/validation.py#L494-L500) | 专用字段元组 + 按策略码开 `elif` 分支 |
  | contracts | [schema.py L883-L1197](file:///e:/projects/qrp-atlas/src/qrp_atlas/contracts/schema.py#L883-L1197)、[L1702-L1761](file:///e:/projects/qrp-atlas/src/qrp_atlas/contracts/schema.py#L1702-L1761) | 通用 schema 内定义 16 张 System B 表并混入 `ALL_TABLES` |
  | indicators | [definitions.py L11-L19](file:///e:/projects/qrp-atlas/src/qrp_atlas/indicators/definitions.py#L11-L19)、[registry.py L23-L45](file:///e:/projects/qrp-atlas/src/qrp_atlas/indicators/registry.py#L23-L45) | 通用分层枚举开 `SYSTEM_B` 档位 |
  | backtest | [runner.py L130-L136](file:///e:/projects/qrp-atlas/src/qrp_atlas/backtest/harness/runner.py#L130-L136) | 硬编码 `system_b_portfolio` → `max_positions=6 / max_weight=0.25` |
  | backtest | [product/catalog.py L16-L50](file:///e:/projects/qrp-atlas/src/qrp_atlas/backtest/product/catalog.py#L16-L50) | 通用产品目录硬编码 `system_b_basic` |
  | api | [server.py L87-L88](file:///e:/projects/qrp-atlas/src/qrp_atlas/api/server.py#L87-L88) | 通用装配处硬编码 `include_router` |
  | config | [operations.py L91-L99](file:///e:/projects/qrp-atlas/src/qrp_atlas/config/operations.py#L91-L99) | 通用审计硬编码 System B 库/表/锁 |

- **补充证据**：`backtest/portfolio/*`、`orchestration/*`、`strategies/models.py`、`protocol.py`、`pipeline/registry.py` 保持干净——说明污染是**局部的、可隔离的**。
- **对照冲突**：[Task07-C 设计书](file:///e:/projects/qrp-atlas/docs/QRP产品蓝图v1.1/Task07/Task07-C_SystemB_Portfolio_Constraint_Resolution_Final_Target_设计书.md) §5 明文「不把 System B 1/8 / 30% / max6 写进 generic `weights.py`」；`weights.py` 确实施行了，但同一约束在 `runner.py` 复发。
- **影响面**：通用层的可复用性与可理解性；任何第二个策略接入时都要面对这些特化分支。
- **修复成本**：中
- **建议动作**：逐项把硬编码分支外提为扩展点（策略自带 validator / product metadata / 执行参数声明、Provider 注册），语义保持等价。

### A-04 ｜ 35 个函数估算超过 150 行 ｜ P1 ｜ 待处理

- **问题**：存在大量超长函数，最长约 674 行；`backtest/product/service.py` 单文件含 4 个 > 200 行函数。
- **证据**（前 6 名，函数体为相邻 `def` 间距估算）：

  | 估算行数 | 函数 | 位置 |
  | ---: | --- | --- |
  | ~674 | `calculate_theme_ranking` | [theme_ranking.py L324](file:///e:/projects/qrp-atlas/src/qrp_atlas/indicators/system_b/theme_ranking.py#L324) |
  | ~581 | `ThemePipelineService._produce_single_day` | [theme/service.py L624](file:///e:/projects/qrp-atlas/src/qrp_atlas/pipeline/theme/service.py#L624) |
  | ~461 | `run_theme_rank_daily` | [system_b_theme_rank/service.py L171](file:///e:/projects/qrp-atlas/src/qrp_atlas/pipeline/system_b_theme_rank/service.py#L171) |
  | ~455 | `main` | [jobs_cli.py L462](file:///e:/projects/qrp-atlas/src/qrp_atlas/jobs_cli.py#L462) |
  | ~366 | `run_residual_robustness_study` | [robustness.py L1000](file:///e:/projects/qrp-atlas/src/qrp_atlas/backtest/research/robustness.py#L1000) |
  | ~326 | `_validate_contract` | [contract_validation.py L168](file:///e:/projects/qrp-atlas/src/qrp_atlas/pipeline/contract_validation.py#L168) |

- **影响面**：单函数无法被完整审查、无法被单元测试覆盖到分支、修改风险高。
- **修复成本**：中
- **建议动作**：不批量重写；在发生实质业务修改时按"随业务变更拆分"原则处理（与 AGENTS.md 既有条款一致）。

### A-05 ｜ 58 个文件 >= 500 行 ｜ P1 ｜ 待处理

- **问题**：文件规模的头部极其集中，18 个文件 >= 900 行。
- **证据**（前 5 名）：

  | 行数 | 层 | 文件 |
  | ---: | --- | --- |
  | 1,880 | contracts | [schema.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/contracts/schema.py) |
  | 1,867 | pipeline | [market_data_contracts.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/pipeline/market_data_contracts.py) |
  | 1,612 | indicators | [factors.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/indicators/cross_section/factors.py) |
  | 1,520 | pipeline | [pit_fundamentals_contracts.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/pipeline/pit_fundamentals_contracts.py) |
  | 1,463 | orchestration | [store.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/orchestration/store.py) |

  分布：`pipeline` 27 个、`backtest` 10、`indicators` 6、`contracts` 4、`config` 3、`strategies` 3、`orchestration` 2。
- **影响面**：合并冲突高发（单人多分支时尤甚）、审查困难、AI 修改时上下文超限风险。
- **修复成本**：中高
- **建议动作**：优先处理 A-03/A-06 涉及的文件（同时获得解耦与拆分收益），不为拆分而拆分。

### A-06 ｜ 两个契约模块之间成套复制粘贴 ｜ P1 ｜ 待处理

- **问题**：[research_report_contracts.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/pipeline/research_report_contracts.py)（1,207 行）与 [research_industry_contracts.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/pipeline/research_industry_contracts.py)（1,255 行）定义了一整套**同名同构**函数，行号高度接近。
- **证据**（同名函数对及其行号）：`_validate_list_records`、`_prepare_rows`、`_stage_csv_outputs`、`_stage_pdf_outputs`、`_promote_files`、`_load_transaction`、`_db_completion`、`_raw_csv_completion`、`_canonical_csv_completion`、`_pdf_quality`、`_db_quality`、`_csv_quality`；`_execute_*` 分别在 L590 与 L607，`_db_quality` 分别在 L911 与 L958。
- **影响面**：一处修复需在两处同步；两者会持续漂移；共约 2,400 行中的大部分是重复。
- **修复成本**：中
- **建议动作**：把共享的 staging / promote / quality 逻辑下沉为共享 support 模块，两个契约只保留各自的业务差异。

### A-07 ｜ 跨模块重复实现至少 16 组 ｜ P1 ｜ 待处理

- **问题**：多个私有辅助函数在多个契约模块中被重复定义。
- **证据**（定义处数量）：

  | 符号 | 处数 | 示例位置 |
  | ---: | ---: | --- |
  | `_is_missing` | 6 | `membership_contracts.py:133`、`pit_fundamentals_contracts.py:613`、`system_b_decision.py:675` 等 |
  | `_calendar_freshness` | 5 | `etf_support.py:198`、`membership_contracts.py:318`、`market_data_contracts.py:162` 等 |
  | `_performance` | 5 | `cninfo_contracts.py:529`、`irm_qa_contracts.py:437` 等 |
  | `_calendar_structure` | 4 | `etf_support.py:188`、`membership_contracts.py:308` 等 |
  | `_check_context` / `_check_invocation` | 各 4 | 4 个契约模块 |
  | `_normalize_horizons` | 4 | `research/ic.py:234`、`research/groups.py:270` 等 |
  | `_connect` / `_table_completion` / `_normalize_date_str` | 各 3 | 见 A-06 同层 |

  反面证据（避免误判）：`rank_component`、`_validate_staging`、`to_duckdb` 均**仅 1 处**定义，未被复制。
- **影响面**：行为不一致风险（复制后各自演化）；修复需多处同步。
- **修复成本**：低中
- **建议动作**：下沉为共享工具模块，优先处理 `_is_missing`、`_calendar_freshness` 这两个高频项。

### A-08 ｜ 11 个 pipeline 子包共享同构四件套 ｜ P2 ｜ 待处理

- **问题**：11 个 pipeline 子包各自维护结构完全一致的 `clean.py` / `fetch.py` / `run.py`（+ `load_duckdb.py` 或 `load.py`），合计 **48 个样板文件**。
- **证据**：`clean.py` ×11、`fetch.py` ×11、`run.py` ×11、`load_duckdb.py` ×7、`load.py` ×4、`config.py` ×4。
- **影响面**：新增一个数据源的固定成本被抬高；模板化改动需要改 11 处。
- **修复成本**：中
- **建议动作**：评估提取通用采集骨架；注意 AGENTS.md 已明确"纯文件搬迁不是阻塞项"，应随实质变更逐步收敛，不做专项搬迁。

### A-09 ｜ 11 个"仅一行 TODO"的占位文件 ｜ P2 ｜ 待处理

- **问题**：存在只含 `# TODO` 的空占位模块，长期未实现也未删除。
- **证据**：`src/qrp_atlas/__init__.py`、`pipeline/__init__.py`、`pipeline/canonical_store.py`、`pipeline/raw_store.py`、`pipeline/canonicalize_daily_bar.py`、`pipeline/canonicalize_snapshot.py`（6 个）+ `scripts/` 下 5 个（`canonicalize_daily_snapshot.py`、`canonicalize_history_daily.py`、`fetch_daily_snapshot.py`、`fetch_history_daily.py`、`load_duckdb.py`）。
- **影响面**：误导读者以为存在该能力；`pipeline/` 包名被空文件占用，与 `scripts/` 下的同名脚本语义重复。
- **修复成本**：低
- **建议动作**：确认是否仍需要，删除或补实说明。

### A-10 ｜ 代码量高度集中 ｜ P2 ｜ 待处理

- **问题**：16.7% 的文件承载约 60% 的代码；`pipeline` 单模块占 `src/` 的 42.5%。
- **证据**：58 个 >= 500 行文件合计约 55,600 行 / 总计 91,745 行。
- **影响面**：改动集中度高 → 冲突与回归风险集中；模块粒度失衡。
- **修复成本**：高
- **建议动作**：不强求均衡，但新增能力应优先落在小模块，避免继续往超大文件追加。

---

## 4. 维度 B：文档体系

> **该维度结构性问题**：文档的价值密度很高（架构规则、业务契约、审计记录都写得很实），但**元数据层（编号、命名、索引、SSOT 归属）完全无人治理**。这不是内容问题，是元数据治理缺席。

### B-01 ｜ `Task07` 同名不同义 ｜ P1 ｜ 待处理

- **问题**：v1.0 与 v1.1 各有一套 `Task07`，含义完全不同，共用 `Task07-A/B/C` 字面编号。
- **证据**：
  - v1.0：`17_任务07_A_真实回测产品主链.md` ~ `23_任务07_D_声明式策略产品.md`（真实回测产品主链 / 横截面动量 / 事件驱动 / 结果封板 / 声明式策略）
  - v1.1：[Task07/](file:///e:/projects/qrp-atlas/docs/QRP产品蓝图v1.1/Task07)（System B Portfolio Target / Holding Entry Exit / Constraint Resolution）
- **影响面**：任何"Task07"引用都是歧义；跨版本沟通与检索成本高。
- **修复成本**：中
- **建议动作**：给两套编号加版本命名空间前缀，或对 v1.1 启用全新编号段。

### B-02 ｜ SSOT 四处并存主张 ｜ P1 ｜ 待处理

- **问题**：至少四种"唯一事实来源"的主张并存，且从未说明它们分属不同维度、也无优先级。
- **证据**：

  | # | SSOT 主张 | 出处 |
  | --- | --- | --- |
  | 1 | 业务规则 SSOT = `MyTradingSystem`（跨仓） | [02_架构与跨仓边界.md](file:///e:/projects/qrp-atlas/docs/QRP产品蓝图v1.1/02_架构与跨仓边界.md) |
  | 2 | 数据定义 SSOT = `contracts/` 下 3 个核心文件 | [ssot_data_model.md](file:///e:/projects/qrp-atlas/docs/ssot_data_model.md) |
  | 3 | Dual SSOT = `quant.db`（运行时）+ `canonical/**`（恢复） | [mvp_project_structure_v1.3.md](file:///e:/projects/qrp-atlas/docs/architecture/mvp_project_structure_v1.3.md) |
  | 4 | Pipeline 业务语义 SSOT = 源码 `PipelineContract` | [12_Pipeline正式开发规则.md](file:///e:/projects/qrp-atlas/docs/QRP产品蓝图v1.1/12_Pipeline正式开发规则.md) |

  另：主张 2 的文档实际只描述 12 张表且含旧表（`market_phase`、`trade_execution`），与 v1.1 的 58 张表已脱节。
- **影响面**：新人无法判断冲突时以哪份为准；契约类决策缺乏权威依据。
- **修复成本**：低中
- **建议动作**：出一份元规则，说明四者各管哪个维度、冲突时的裁决顺序。

### B-03 ｜ 蓝图 README 索引与磁盘严重不符 ｜ P1 ｜ 待处理

- **问题**：v1.1 蓝图 README 的文档索引只列 10 项，磁盘实际有 13 个编号文件 + 10 个子目录；**无任何校验机制**。
- **证据**：[README.md L64-L75](file:///e:/projects/qrp-atlas/docs/QRP产品蓝图v1.1/README.md#L64-L75) 未收录 `06`、`07`、`08`、`11`、`12`、`13`，以及 `Task07/`、`Task08/`、`Task09/`、`task00/`、`task05/`、`task06/`、`episode-segment-refinement/`、`pipeline-refactor-tasks/`。
  - 反向（索引有但磁盘无）：**无**。
- **影响面**：文档可发现性差；新文档写了也没人知道。
- **修复成本**：低
- **建议动作**：重写索引，并把索引与磁盘一致性做成可执行检查（与 A-01 同一套门禁）。

### B-04 ｜ 命名体系四种并存 ｜ P2 ｜ 待处理

- **问题**：同层级混用 `taskNN`（小写）/ `TaskNN`（大写）/ 语义式命名 / 另一套 `00~06` 编号；文件名分隔符混用下划线与空格。
- **证据**：`task00`、`task03`~`task06`（小写）与 `Task07`、`Task08`、`Task09`（大写）并存；`episode-segment-refinement/`、`system-b-state-productionization/`、`pipeline-refactor-tasks/` 为语义式；`Task07-A_..._设计书.md`（下划线）与 `Task04-A Implementation Plan.md`（空格）混用。
- **影响面**：排序、检索、脚本化处理困难。
- **修复成本**：低
- **建议动作**：定一份命名规范，仅对新增文档强制执行（存量不做搬迁）。

### B-05 ｜ 文档与代码状态冲突 ｜ P2 ｜ 待处理

- **问题**：根目录 `api.md` 把 `M1_core / M2_front / M3_identifiable` 描述为市场身份布尔位并保留旧 `market_phase` 表；v1.1 Task06 明确"M1/M2/M3 是三个评分维度、不再是身份"。
- **证据**：[api.md](file:///e:/projects/qrp-atlas/docs/api.md)（GET /api/phase）vs [Task06 System B 横截面相对评分与排名设计书 v0.1.md](file:///e:/projects/qrp-atlas/docs/QRP产品蓝图v1.1/task06/Task06%20System%20B%20横截面相对评分与排名设计书%20v0.1.md) §2。
- **影响面**：误读设计语义，可能实现出与 v1.1 契约不符的行为。
- **修复成本**：低
- **建议动作**：对失效文档加显式废弃标注或归档。

### B-06 ｜ `docs/` 根目录散落文档 ｜ P2 ｜ 待处理

- **问题**：根目录存在 `api.md`、`ssot_data_model.md`、`daily-job-rollout-decisions.md`、`pipeline-usage.md`、`runtime-configuration.md`、`指标.md`、`deep-research-report.md`、`tunnel.md`、`DFCF_guba_qa_research.md`、`Tushare_Pro数据调研报告_QRP_v1.0.md` 等，均未被任何索引收录。
- **影响面**：与蓝图体系脱节；部分内容已与 v1.1 冲突（见 B-05）。
- **修复成本**：低
- **建议动作**：分类处置——仍有效者纳入索引，失效者归档到 `archive/`。

### B-07 ｜ 文档目录内混入可执行脚本 ｜ P2 ｜ 待处理

- **问题**：`docs/QRP产品蓝图v1.1/task06/` 下存在 `audit_task06_b_rc1.py`、`audit_task06_b_rc2.py` 两个审计脚本。
- **影响面**：文档目录被执行代码污染；脚本无测试覆盖、无入口约定。
- **修复成本**：低
- **建议动作**：移入 `scripts/` 或 `tests/`，文档中保留引用。

### B-08 ｜ 同一主题被拆到两个目录 ｜ P3 ｜ 待处理

- **问题**：Task03 主题同时存在于 [task03/README.md](file:///e:/projects/qrp-atlas/docs/QRP产品蓝图v1.1/task03/README.md) 与 `system-b-state-productionization/README.md`，且前者明确宣告取代旧递推语义、后者声明旧版本不静默覆盖。
- **影响面**：读者需交叉比对才能确定有效结论。
- **修复成本**：低
- **建议动作**：合并或建立显式的互相引用。

---

## 5. 维度 C：生产一致性

> **该维度结构性问题**：**没有任何一份文件是"生产在跑什么"的权威答案。** 四份编排定义互相矛盾，权威 manifest 在服务器上且不在版本控制中，31 张表没有迁移记录。生产状态对外部是不可复现、不可审计的。
>
> 本轮全部结论基于仓库静态文件，未连接生产服务器（见 §6）。

### C-01 ｜ 4 份编排定义互相矛盾 ｜ **P0** ｜ 待处理

- **问题**：仓库内存在 4 份编排定义，条目数与语义互不一致，没有单一权威。
- **证据**：

  | 文件 | 条目数 | 主键 | enabled 语义 |
  | --- | ---: | --- | --- |
  | [pipeline-registry.json](file:///e:/projects/qrp-atlas/deploy/pipeline/pipeline-registry.json) | 29 | `pipeline_id` | 无（用 `status`） |
  | [production-job-definitions.json](file:///e:/projects/qrp-atlas/deploy/pipeline/production-job-definitions.json) | 2 | `job_id` + `pipeline_id` | 全 `false` |
  | [pipeline-definitions.shadow.json](file:///e:/projects/qrp-atlas/deploy/pipeline/pipeline-definitions.shadow.json) | 6 | 仅 `pipeline_id`（缺 `job_id`） | 全 `false` |
  | [default_definitions.json](file:///e:/projects/qrp-atlas/src/qrp_atlas/orchestration/default_definitions.json) | 0 | — | — |
  | 源码注册 `PipelineContract` | 38 | `pipeline_id` | 无 |

  具体矛盾：shadow 中 6 个 pipeline 全部 `enabled=false`，但它们在 registry 中标为 `LEGACY_SCHEDULED`（视为已调度）；`production-job-definitions.json` 仅覆盖 1 条 contract，其余 37 条在仓库内无任何生产 Job 实例。
- **影响面**：无法从仓库回答"生产到底在跑哪些 job"；任何生产变更都缺少可核对基线。
- **修复成本**：中
- **建议动作**：确定唯一权威定义与其余文件的角色（规划台账 / 部署选择 / 兼容样例），并明确字段语义。

### C-02 ｜ 生产权威 manifest 未纳入 git ｜ **P0** ｜ 待处理

- **问题**：生产实际使用的 manifest 在服务器上，未纳入版本控制，无版本历史。
- **证据**：Task09 只读调研报告（成文 2026-09-09）称 live manifest 未被 git 跟踪，且与 `.candidate.json` 逐字节一致；`deploy/qrp-atlas-jobs.service` 不在 repo。
- **影响面**：无法固定审计基线；无法回滚；无法做变更 diff。
- **修复成本**：中（需服务器侧协同）
- **建议动作**：把生产 manifest 纳入版本库（或其副本），建立"以 git 为准"的部署流程。

### C-03 ｜ 已注册 Contract 与编排注册表双向不匹配 ｜ P1 ｜ 待处理

- **问题**：源码注册的 Contract 与编排注册表条目互有缺失。
- **证据**：
  - **16 条已注册 Contract 不在 registry 中**：`etf_daily_update`、`etf_adj_factor_update`、`index_basic_update`、`limit_step_ingest`、`ths_daily_ingest`、`stk_high_shock_ingest`、`dc_hot_ingest`、`ths_hot_ingest`、`theme_m4_production`、`theme_m5_production`、`market_m6_production`、`system_b_asset_rank_daily`、`system_b_theme_rank_daily`、`system_b_decision_facts_daily`、`system_b_strategy_daily`、`system_b_daily_closeout`
  - **7 条 registry 条目无对应 Contract**：`pipeline_daily_summary_agent`、`system_health_weekly_agent`、`system_b_state_initialize`、`system_b_pool_completeness_daily`、`pipeline_daily_summary_deterministic`、`system_health_weekly_deterministic`、`qrp_production_daily_run`
  - 另：`pipeline-registry.json` 相对 [daily-job-rollout-decisions.md](file:///e:/projects/qrp-atlas/docs/daily-job-rollout-decisions.md) 已过时（该决策单第 1/2 组共 7 项新增，registry 只含 1 项）
- **影响面**：能力实际是否可调度无法判断；生产接入遗漏难以发现（例如 `dc_hot_ingest`/`ths_hot_ingest` 未调度会使排名链的依赖边不可满足）。
- **修复成本**：中
- **建议动作**：以源码 Contract 为权威，反向校验并补齐 registry。

### C-04 ｜ 31 张表无迁移文件 ｜ P1 ｜ 待处理

- **问题**：`contracts` 定义了 58 张表，`deploy/duckdb/` 迁移只建 27 张；**31 张表靠代码运行时 `init_database()` 建表**，无迁移记录。
- **证据**：
  - `ALL_TABLES`：[schema.py L1702-L1761](file:///e:/projects/qrp-atlas/src/qrp_atlas/contracts/schema.py#L1702-L1761)（58 张）
  - 迁移建表 27 张（`002`~`010`），**0 张迁移表不在 contracts 中**（正向一致）
  - 缺迁移的 31 张含核心表：`daily_market_snapshot`、`stock_info`、`trading_calendar`、`adj_factor_changes`、`index_daily`、`etf_daily`、`zt_pool`、`daily_basic`、`suspend_d`、`financial_indicator`、`earnings_forecast_event`、`system_b_state_observation`、`system_b_production_run` 等
  - 具体案例：`system_b_state_observation` / `system_b_production_run` / `system_b_latest_state` 由 [ensure_system_b_schema()](file:///e:/projects/qrp-atlas/src/qrp_atlas/pipeline/system_b/repository.py#L129-L183) 创建，全仓库 `CREATE VIEW` 仅此 1 处
- **影响面**：新建环境缺表依赖代码执行顺序；无法审计"某环境的表是什么时候、按哪个版本建的"；迁移与代码双轨（`system_b_episode`、`system_b_pool_*` 同时存在于迁移和 `ensure_schema()`）。
- **修复成本**：中高
- **建议动作**：明确"迁移为准 or 代码建表为准"的单一策略，并为存量表补齐迁移基线。

### C-05 ｜ 迁移编号重号 ｜ P2 ｜ 待处理

- **问题**：`deploy/duckdb/` 中 `003` 出现两次；且该目录缺 `001`（`001_auth_schema.sql` 在 `deploy/postgres/`）。
- **证据**：[003_stock_collections_and_m4.sql](file:///e:/projects/qrp-atlas/deploy/duckdb/003_stock_collections_and_m4.sql) 与 [003_system_b_pools.sql](file:///e:/projects/qrp-atlas/deploy/duckdb/003_system_b_pools.sql)。
- **影响面**：若存在按编号排序的迁移执行器，执行顺序不确定；重号在增量收敛时无法唯一定位。
- **修复成本**：低（但已发布迁移改名需谨慎，需确认生产已应用状态）
- **建议动作**：确认执行器是否依赖编号；如依赖，为后续编号建立唯一性检查。

### C-06 ｜ 双份表定义 ｜ P2 ｜ 待处理

- **问题**：[database/schema.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/database/schema.py#L18-L69) 内嵌 `daily_market_snapshot`、`market_phase`、`trade_execution` 三张表的完整建表 SQL，与 [contracts/schema.py L354-L411](file:///e:/projects/qrp-atlas/src/qrp_atlas/contracts/schema.py#L354-L411) 的权威定义构成第二份文本。
- **证据**：`database/schema.py` 自述为 v1.0 遗留 helper，被 [config/setup.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/config/setup.py#L22) 调用；`runtime-configuration.md` 明确"schema SQL 只保留一份"。
- **影响面**：两份定义可能漂移；违反 contracts 作为 SSOT 的原则。
- **修复成本**：低
- **建议动作**：让 `database/schema.py` 改为引用 `contracts` 的 DDL 生成能力，而非自带一份 SQL 文本。

### C-07 ｜ 已废弃入口残留 ｜ P2 ｜ 待处理

- **问题**：`deploy/qrp-atlas-pipeline-scheduler.service.example` 与 `-runner.service.example` 调用 `qrp-atlas-pipeline` 入口，但该 console script 在 `pyproject.toml` 中**不存在**；正式入口已改为 `qrp-atlas-jobs`。
- **证据**：[scheduler L12](file:///e:/projects/qrp-atlas/deploy/qrp-atlas-pipeline-scheduler.service.example#L12)、[runner L12](file:///e:/projects/qrp-atlas/deploy/qrp-atlas-pipeline-runner.service.example#L12) vs [pyproject.toml L29-L37](file:///e:/projects/qrp-atlas/pyproject.toml#L29-L37)；文档 [pipeline-usage.md L10](file:///e:/projects/qrp-atlas/docs/pipeline-usage.md#L10) 规定正式入口为 `qrp-atlas-jobs`。
- **影响面**：误按示例部署会得到不可用单元。
- **修复成本**：低
- **建议动作**：更新或删除这两个 `.example`。

### C-08 ｜ 环境变量定义分叉 ｜ P3 ｜ 待处理

- **问题**：`deploy/qrp-atlas-pipeline.env.example` 定义了 `QRP_PIPELINE_RUNTIME_DIR`，该变量**不在** `SUPPORTED_ENV_VARS` 支持清单中。
- **证据**：[qrp-atlas-pipeline.env.example L4](file:///e:/projects/qrp-atlas/deploy/qrp-atlas-pipeline.env.example#L4) vs [settings.py L789-L831](file:///e:/projects/qrp-atlas/src/qrp_atlas/config/settings.py#L789-L831)（支持清单只有 `QRP_JOB_RUNTIME_DIR` / `QRP_JOB_RUNTIME_DB_PATH`）。
  - 正面事实：`.env.example` 与 `SUPPORTED_ENV_VARS` 本身**完全一致**，均为 39 项同名，这一对是健康的。
- **影响面**：按该示例配置会设置一个不被识别的变量，掩盖真实配置缺失。
- **修复成本**：低
- **建议动作**：校正示例文件，或删除该示例（与 C-07 同属废弃示例清理）。

### C-09 ｜ repo 与生产已漂移 ｜ P1 ｜ 待处理

- **问题**：仓库状态与生产状态不一致，且不一致程度无法从仓库判断。
- **证据**（均为历史文档陈述，成文 2026-09-09，**可能已过时**）：Task09 报告称 live manifest 29 jobs 全 enabled、repo 仅 2 jobs 全 disabled；repo-only（已注册未调度）10 项；System B 链只到 pool；报告称 35 contracts，当前 repo 已 38 条。
- **影响面**：本机任何"已验证"结论都不能代表生产。
- **修复成本**：需服务器协同
- **建议动作**：授权后做一次生产实测盘点，把结果回写到台账。

### C-10 ｜ 系统表创建双轨 ｜ P3 ｜ 待处理

- **问题**：`system_b_episode/_observation/_segment`、`system_b_pool_membership_daily/_pool_run` 同时存在于迁移文件与运行时 `ensure_schema()`。
- **证据**：[002_system_b_episode.sql](file:///e:/projects/qrp-atlas/deploy/duckdb/002_system_b_episode.sql) / [003_system_b_pools.sql](file:///e:/projects/qrp-atlas/deploy/duckdb/003_system_b_pools.sql) 与 [system_b_episode/service.py L73-L76](file:///e:/projects/qrp-atlas/src/qrp_atlas/pipeline/system_b_episode/service.py#L73-L76)、[system_b_pools/service.py L100-L102](file:///e:/projects/qrp-atlas/src/qrp_atlas/pipeline/system_b_pools/service.py#L100-L102)。
- **影响面**：双轨本身无害（幂等建表），但掩盖了 C-04 的策略不清问题。
- **修复成本**：低
- **建议动作**：随 C-04 一并处理。

---

## 6. 维度 D：开发流程

> **该维度结构性问题**：这个仓库**没有任何自动刹车**。文档里规定了 6 道发布闸门、PR 强制门禁、12 条依赖禁令、提交前必跑测试——全部依赖人工记忆与自觉。这是 A-02/A-03/C-01 能够长期存在的机制根源。

### D-01 ｜ 完全无 CI/CD ｜ **P0** ｜ 待处理

- **问题**：不存在任何自动化检查装置。
- **证据**：
  - 无 `.github/workflows/`、`.gitlab-ci.yml`、`Jenkinsfile`、`.travis.yml`、`.pre-commit-config.yaml`（全仓库扫描，仅 [pyproject.toml](file:///e:/projects/qrp-atlas/pyproject.toml) 与 `.codex/config.toml` 两个配置文件，后者是 Agent 工具配置）
  - `.git/hooks/` 全部为 Git 自带 `*.sample`，**无任何已启用钩子**
  - 无 `Makefile` / `noxfile.py` / `tox.ini`
- **影响面**：测试、lint、架构检查全部可被跳过；实际执行完全依赖个人纪律。
- **修复成本**：低
- **建议动作**：先建最小门禁（pytest + 架构检查），可用本地 pre-commit 起步，不必一步到位上云 CI。

### D-02 ｜ 文档规定的闸门全部无自动化执行 ｜ **P0** ｜ 待处理

- **问题**：蓝图规定了完整的多道门禁，但没有一道被机械化。
- **证据**：
  - [05_验收与变更控制.md L101-L123](file:///e:/projects/qrp-atlas/docs/QRP产品蓝图v1.1/05_验收与变更控制.md#L101-L123)：6 道发布闸门，「任一当前闸门失败，不得进入下一阶段」
  - [12_Pipeline正式开发规则.md L282-L293](file:///e:/projects/qrp-atlas/docs/QRP产品蓝图v1.1/12_Pipeline正式开发规则.md#L282-L293)：PR 强制门禁，「缺少其中任何一项不得合并」
  - [同文档 L295-L303](file:///e:/projects/qrp-atlas/docs/QRP产品蓝图v1.1/12_Pipeline正式开发规则.md#L295-L303)：提交前必跑 `pytest` / `compileall` / `git diff --check`
  - [AGENTS.md L439-L445](file:///e:/projects/qrp-atlas/src/qrp_atlas/AGENTS.md#L439-L445)：交付前原则上运行 `python -m pytest`
- **影响面**：规则越详细，实际执行率越低（因为全靠人记）。规则文本与实际状态之间的落差会持续扩大。
- **修复成本**：中
- **建议动作**：把可机械化的闸门逐条转为检查项，其余标注为"人工闸门"并明确执行证据要求。

### D-03 ｜ 无 lint / 类型检查 / 格式化配置 ｜ P1 ｜ 待处理

- **问题**：项目未配置任何静态质量工具。
- **证据**：[pyproject.toml L63-L65](file:///e:/projects/qrp-atlas/pyproject.toml#L63-L65) 中 `[tool.pytest.ini_options]` 仅有 `testpaths` 与 `pythonpath`；**无** `[tool.ruff]`、`[tool.mypy]`、`[tool.black]`、`[tool.isort]`，也无 flake8/pylint 配置。
- **影响面**：代码风格与类型正确性完全靠人工；对 AI 辅助开发尤其不利（缺少可自动发现低级错误的反馈回路）。
- **修复成本**：低
- **建议动作**：引入最小集合（formatter + import 排序 + 基础 lint），先只检查新增/改动文件。

### D-04 ｜ 无覆盖率配置与阈值 ｜ P1 ｜ 待处理

- **问题**：有 1,331 个测试函数，但无覆盖率采集与阈值。
- **证据**：依赖中无 `pytest-cov`（[pyproject.toml L39-L44](file:///e:/projects/qrp-atlas/pyproject.toml#L39-L44) 仅 pytest/fastapi/httpx）；全仓库无 `--cov` / `fail_under`；`.gitignore` 忽略 `.coverage`/`htmlcov` 仅是忽略规则。
- **影响面**：无法知道测试实际覆盖了什么；新增代码可能零覆盖而不被察觉。已知结构性缺口：`database`、`jobs_cli` 无对应测试目录（间接覆盖）。
- **修复成本**：低
- **建议动作**：先只采集报告（不设阈值），建立基线后再对新增模块设阈值。

### D-05 ｜ 单一提交者，无第二人 review ｜ P1 ｜ 待处理

- **问题**：可观测的提交历史中**只有一个人**的身份。
- **证据**：`.git/logs/` 全部条目作者/提交者为 `Ryan Xia <xlykyz@gmail.com>`，无第二个邮箱。存在 PR 引用（`fix(system-b): address PR #64 review feedback`）与对抗审计文档（Task07-A 结论、Task06-B rc1/rc2），说明曾有过 review 形式，但审查者非独立第三方。
- **影响面**：无独立视角的质量把关；AB 岗与交叉复核机制缺失。这是**组织问题而非技术问题**，任何门禁都无法替代。
- **修复成本**：高
- **建议动作**：明确治理角色与复核要求；在无第二人的现实下，用"机制化检查 + 第三方审计"部分替代人工 review。

### D-06 ｜ 存在直接提交长期分支的痕迹 ｜ P2 ｜ 待处理

- **问题**：`develop/v1.1` 被 commit 直接推进（非 merge/pull），与"长期分支不得直接开发"的规则相悖。
- **证据**：`.git/logs/refs/heads/develop/v1.1` 中 2 次直接推进，均为 docs-only（`docs: reformat 指标.md...`、`docs(v1.1): rename misleading task04 system-b directory`）。规则出处：[AGENTS.md L21](file:///e:/projects/qrp-atlas/AGENTS.md)。
- **影响面**：小（docs-only），但规则被绕过说明缺少强制。
- **修复成本**：低
- **建议动作**：由 D-01 的分支保护/钩子覆盖。

### D-07 ｜ 变更控制无结构化台账 ｜ P2 ｜ 待处理

- **问题**：`SCOPE_CHANGE` 等变更分类有定义、也有实际使用（2026-09-04 中期评估调整），但记录内嵌在文档中，无独立台账。
- **证据**：[05_验收与变更控制.md L125-L149](file:///e:/projects/qrp-atlas/docs/QRP产品蓝图v1.1/05_验收与变更控制.md#L125-L149) 定义 9 类变更；实际记录以文档内嵌说明与提交信息形式存在。
- **影响面**：变更历史无法被检索与审计；相关文档的版本关系难追溯。
- **修复成本**：低
- **建议动作**：建立单一变更登记文件（本台账的 §7 可作为起点）。

### D-08 ｜ 文档索引无校验机制 ｜ P2 ｜ 待处理

- **问题**：与 B-03 同源：索引可以任意失真而无人发现。
- **影响面**：文档可信度下降。
- **修复成本**：低
- **建议动作**：把索引一致性做成检查项，与 A-01 合并实现。

### D-09 ｜ 部分模块无对应测试目录 ｜ P3 ｜ 待处理

- **问题**：`database/`（126 行）与根目录 `jobs_cli.py`（917 行）没有对应的测试目录/同名测试文件。
- **证据**：`tests/` 下无 `database/`、无 `test_jobs_cli.py`；两者通过 `tests/config/`、`tests/orchestration/`、`tests/pipeline/` 间接覆盖。
- **影响面**：917 行的 CLI 入口无专属测试归属。
- **修复成本**：低
- **建议动作**：为 `jobs_cli` 明确测试归属。

---

## 7. 立即行动清单（前 5 项）

按"收益 / 成本"排序，这 5 项的合计成本都很低，但能同时缓解多个 P0：

| 序 | 动作 | 关闭/缓解 | 成本 | 理由 |
| ---: | --- | --- | --- | --- |
| 1 | 建立**架构字面量与依赖方向检查**（先 warning） | A-01、A-03、D-08 | 低 | 一次投入同时覆盖架构门禁与文档索引校验，是所有后续动作的前提 |
| 2 | 建立**最小测试门禁**（pre-commit 跑 pytest） | D-01、D-02、D-04（部分） | 低 | 不需要云 CI，本地钩子即可形成刹车 |
| 3 | 确定**唯一的生产编排权威定义**，其余文件降级为样例/台账 | C-01、C-03 | 中 | 消除"生产在跑什么"的歧义，是生产治理的起点 |
| 4 | 出具**SSOT 与编号元规则** | B-01、B-02、D-07 | 低 | 解决"冲突时以谁为准"，为所有后续文档决策提供依据 |
| 5 | 启动 **System B 边界收敛**（扩展点外提 + 领域收拢） | A-02、A-03 | 中高 | 唯一需要实质改代码的一项，但它是架构债务的核心 |

**建议的执行顺序**：1 → 2 → 4 → 3 → 5。理由：先有度量（1、2），再有规则（4），再动生产（3），最后重构（5）。在没有度量的情况下动 5 会失去收敛依据。

---

## 8. 证据缺口（本机无法验证）

以下内容**必须连接内网 Linux 生产服务器**才能确认，本台账不做推断：

| # | 未知项 | 影响条目 |
| --- | --- | --- |
| 1 | 生产 systemd 服务的实际安装与 enabled/active 状态，尤其 `qrp-atlas-jobs.service` | C-01、C-02、C-09 |
| 2 | live manifest 的真实条目数、启用状态、schedule，以及是否真为权威 | C-01、C-02、C-09 |
| 3 | 生产 env 文件的实际变量集合与值、生产 CORS 与认证模式 | C-09 |
| 4 | 运行库 `job_runtime.sqlite3` 的 run 记录、失败码、scheduler 心跳 | C-09 |
| 5 | 生产数据库实际已应用的迁移与表结构（是否与迁移/contracts 一致） | C-04、C-05 |
| 6 | `dc_hot_ingest` / `ths_hot_ingest` 是否已调度；System B 决策链是否已在生产接入 | C-03、C-09 |
| 7 | 编号排序是否被迁移执行器依赖 | C-05 |
| 8 | root 级 crontab 内容 | C-01 |

**期间限定**：本台账引用的两份历史文档结论具有明确时点限制——[Task09 生产编排 Read-Only 调研报告](file:///e:/projects/qrp-atlas/docs/QRP产品蓝图v1.1/Task09/生产编排Read-Only调研报告.md)（2026-09-09，且其自身声明不回写后续变更）与 [daily-job-rollout-decisions.md](file:///e:/projects/qrp-atlas/docs/daily-job-rollout-decisions.md)（约 2026-08-05）。其中"35 contracts"、"System B 无 PipelineContract"等结论**已被后续提交超越**，不可当作当前状态使用。

---

## 9. 台账维护约定

1. **本台账只登记事实与量化基线**，不承载修复方案；方案另出《治理路线图》。
2. **每次复检更新 §1 量化基线**，用数字变化衡量治理成效（例如"通用层 `system_b` 字面量命中数：8 → 0"）。
3. **条目状态变更须注明日期与依据**（例如"已关闭：由 PR #NN 完成"）。
4. **新增问题按同一格式追加**，不合并到既有条目以免丢失可追踪性。
5. **不虚报进度**：无法验证的项保持在 §8，不得标为"已关闭"。

### 建议纳入复检的量化指标

| 指标 | 当前值 | 目标 |
| --- | ---: | ---: |
| 通用层文件中 `system_b`/`SYSTEM_B` 字面量命中数 | 8 处层面 / 54 个文件 | 0（System B 命名空间外） |
| 禁止依赖清单的机械覆盖率 | 2 / 12 | 12 / 12 |
| CI / pre-commit 存在性 | 无 | 有 |
| `ALL_TABLES` 无迁移文件的表数 | 31 | 0 |
| 编排定义文件数 / 权威数 | 4 / 0 | 多份但单一权威明确 |
| >= 500 行文件数 | 58 | 不增长 |
| 估算 > 150 行的函数数 | 35 | 不增长 |

---

*本台账基于 `develop/v1.1` @ `d991880` 的静态只读体检生成，未修改任何代码，未连接生产服务器。所有条目证据可复核。*
