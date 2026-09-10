# System B 与 QRP 平台耦合现状调研与解耦路线建议

> 文档性质：只读静态调研报告，**未修改任何代码**
> 调研日期：2026-09-10
> 代码基线：`develop/v1.1` @ `d991880`
> 目标形态基准（已与项目所有者确认）：**插件式接入**（`delivery_mode = PLUGIN` 语义）
> 输出重点（已与项目所有者确认）：**现状诊断 + 解耦路线建议**

---

## 0. 摘要（先看这一段）

1. **真正的病根不是"分层违规"，而是"通用层被单一使用者特化"。** 依赖方向整体是健康的：System B 依赖通用设施（`System B → contracts / indicators / pipeline / strategies / backtest`），调研中**未发现** `indicators → pipeline`、`通用层 → 具体策略` 之外的任何反向依赖或循环依赖。
2. **但通用层的"开洞"是系统性的。** System B 专属源码约 **12,439 行**，而 QRP 通用/核心层为了容纳它，在 **8 个层面**被写入了 `system_b` 专用字面量：策略注册表、通用校验、通用契约 schema、通用指标枚举、通用回测执行器、通用产品目录、通用运维审计、通用打包与编排注册表。
3. **最典型的冲突点**：通用回测执行器 [runner.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/backtest/harness/runner.py#L130-L136) 里硬编码了 `if subject.code == "system_b_portfolio": max_positions = 6; max_weight = 0.25`。而对应设计文档 [Task07-C 设计书](file:///e:/projects/qrp-atlas/docs/QRP产品蓝图v1.1/Task07/Task07-C_SystemB_Portfolio_Constraint_Resolution_Final_Target_设计书.md) 第 5 节明文写着"不把 System B 1/8 / 30% / max6 写进 generic `weights.py`"。**约束是逐点口头声明，没有机械化门禁，于是同一个问题在另一个文件里复发了。**
4. **文档体系与代码体系是双重混乱。** Task 编号在 v1.0/v1.1 之间同名不同义（都有 `Task07`）；SSOT 至少有四处并存主张；v1.1 蓝图 README 的文档索引与磁盘实际目录严重不符；唯一的《项目诊断报告》完全没有覆盖该主题。
5. **解耦的正确顺序是"外提扩展点"，不是"重排依赖"。** 建议分 5 个阶段推进，其中阶段 1（把硬编码分支改成注册机制）风险最低、收益最高，可以立刻启动；**不建议**做物理拆分（独立仓/独立进程），也不建议借机建设 Strategy Framework v2。

---

## 1. 调研方法与判断基准

### 1.1 调研范围

| 范围 | 内容 |
| --- | --- |
| 源码 | `src/qrp_atlas/` 全量 System B 相关文件（关键词 `system_b` / `SystemB` / `SYSTEM_B`，命中 54 个文件） |
| 数据层 | `deploy/duckdb/*.sql`、`deploy/pipeline/pipeline-registry.json` |
| 文档 | `docs/QRP产品蓝图v1.0/`、`docs/QRP产品蓝图v1.1/`、`docs/核心架构v1.0/`、`docs/architecture/`、`docs/` 根目录散落文档 |
| 方法 | 逐文件读取、依赖边核对、只读统计。**未执行任何写操作，未创建任何临时脚本，未运行测试** |

### 1.2 判断基准：什么叫"插件式接入"

项目所有者确认的目标形态是**插件式接入**，即 System B 应作为可插拔交付物，通过稳定扩展点挂载到通用框架，而不是靠通用代码里的硬编码分支接入。据此确立 4 条硬标准：

| # | 标准 | 含义 |
| --- | --- | --- |
| S1 | **通用层零字面量** | 通用层（contracts 核心、strategies 注册表、indicators 注册表、backtest engine/runner、api 应用装配、orchestration、config）中不应出现 `system_b` 专用常量、专用字段、专用分支、专用表名 |
| S2 | **经扩展点加载** | System B 通过注册协议 / 插件入口挂载，通用层只认识"扩展点接口"，不认识"System B" |
| S3 | **抽象由多使用者验证** | 通用抽象至少被 2 个真实使用者验证过，否则不能沉淀为"通用能力" |
| S4 | **可独立启停** | 移除 System B 插件后，QRP 通用平台仍能完整构建、启动、运行、通过测试 |

> 说明：这套标准与现有架构文档并不冲突。v1.1 [02_架构与跨仓边界.md](file:///e:/projects/qrp-atlas/docs/QRP产品蓝图v1.1/02_架构与跨仓边界.md) 已经定义了 `semantic_owner = SYSTEM_B / QRP_CORE / OTHER_USER` 与 `delivery_mode = BUILTIN / PLUGIN / EXTERNAL` 两个正交维度，本报告只是把 `delivery_mode` 从 `BUILTIN` 推向 `PLUGIN`，并按 S1–S4 度量当前差距。

---

## 2. 现状事实

### 2.1 System B 的文档身份

v1.1 蓝图对 System B 的**正式定义**是（原文）：

> 「System B 当前产品身份：`semantic_owner = SYSTEM_B`、`delivery_mode = BUILTIN`」
> 「这表示其业务规则语义来源于 MyTradingSystem/System B，但作为 **QRP bundled built-in strategy** 随项目开发、测试与发布。该身份不要求建立第二套 pipeline、database、registry 或 runtime。」
> —— [04_SystemB工程映射.md](file:///e:/projects/qrp-atlas/docs/QRP产品蓝图v1.1/04_SystemB工程映射.md)

需要明确记录的一点：**现有文档从未把 System B 定义为"外挂"**。真正接近"外挂"的表述只有两处：

- `delivery_mode` 枚举中预留的 `PLUGIN / EXTERNAL` 设计空间，但 [Task07-A 设计书](file:///e:/projects/qrp-atlas/docs/QRP产品蓝图v1.1/Task07/Task07-A_SystemB_Portfolio_Target_Contract_Integration_设计书.md) 明文把它列为**禁止实现项**（"禁止：发现抽象机会→Strategy Framework v2→plugin / external / account / OMS 扩建"）；
- 更早的独立文档集 [qrp-atlas-backtest-engine-product-prototype-v0.1.md](file:///e:/projects/qrp-atlas/docs/回测引擎设计/qrp-atlas-backtest-engine-product-prototype-v0.1.md) 中"作为**外部策略模块接入，不污染通用引擎**"的表述（该文档不属于 v1.1 蓝图体系）。

**结论：目标形态（插件式接入）在文档中是被预留但被禁止实现的；实际执行走的是 `BUILTIN` 路线。** 这是理解全部后续问题的前提。

### 2.2 规模事实（量化）

**源码规模**

| 区域 | 文件数 | 行数 |
| --- | ---: | ---: |
| `indicators/system_b/` | 8 | 3,278 |
| `pipeline/system_b/` | 6 | 1,872 |
| `pipeline/system_b_asset_rank/` | 2 | 734 |
| `pipeline/system_b_theme_rank/` | 2 | 680 |
| `pipeline/system_b_pools/` | 4 | 575 |
| `pipeline/system_b_episode/` | 4 | 505 |
| `pipeline/` 顶层 System B 契约模块（5 个） | 5 | 3,010 |
| `strategies/builtin/system_b_*.py` | 4 | 1,400 |
| `contracts/system_b.py` | 1 | 385 |
| **System B 专属源码合计** | **36** | **≈12,439** |

（另有共用题材指标 `indicators/theme/` 4 文件 550 行，`backtest/harness/` 7 文件 1,517 行）

**配套规模**

| 维度 | 数量 |
| --- | --- |
| System B 相关测试 | 10 个文件 / 2,246 行 |
| System B 前缀数据库表 | 13 张（+ 状态表/视图 3 个） |
| `deploy/duckdb/` 中 System B 迁移 | 5 个（`002` / `003` / `008` / `009` / `010`） |
| 已注册 System B `PipelineContract` | 11 条 |
| `pipeline-registry.json` 中 System B 节点 | 8 条 |
| System B 专用 CLI 入口 | 3 个 |
| System B 相关设计文档 | 25+（`docs/QRP产品蓝图v1.1/` 共 61 个 md） |

**复杂度对比**：System B 专属源码（约 1.24 万行）已经接近 QRP 通用核心代码量级。它不是"一个策略"，而是"一个平台级子系统"。

### 2.3 分布事实

System B 的代码跨越了 QRP 的**全部**架构层：

```text
contracts     → contracts/system_b.py（+ schema.py 内的表定义）
orchestration → （干净，未直接命中）
pipeline      → pipeline/system_b/、system_b_asset_rank/、system_b_episode/、
                system_b_pools/、system_b_theme_rank/、system_b_*_contracts.py ×4、
                system_b_task09.py
stock_collections → （干净）
indicators    → indicators/system_b/ 全部
strategies    → strategies/builtin/system_b_{basic,authorization,decision,portfolio}.py
backtest      → backtest/harness/（runner / models / strategy_driver / experiment）
                backtest/product/（catalog / service）
api           → api/routes/system_b.py、system_b_pools.py、api/schemas/system_b.py、
                api/system_b_serialization.py
config        → config/operations.py
deploy        → 5 个 SQL 迁移、8 个编排节点、systemd service/timer
```

**这本身不是问题**——一个完整策略链本来就该贯穿全层。问题在于**通用层是否为它开了洞**，见下一章。

---

## 3. 耦合诊断

### 3.1 关键判断：依赖方向是健康的，通用层是被特化的

调研结论（有证据支撑）：

- **依赖方向正确**：System B 各模块统一依赖通用设施，未发现 `indicators → pipeline`、`通用 → 具体策略`之外的反向依赖，也未发现循环依赖。
- **保持干净的通用模块**（正面证据，应作为解耦的"锚点"）：
  - [strategies/models.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/strategies/models.py)、[strategies/protocol.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/strategies/protocol.py)
  - [pipeline/registry.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/pipeline/registry.py)、[pipeline/production_jobs.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/pipeline/production_jobs.py)
  - `backtest/portfolio/*`（通用组合回测引擎，零 System B 字面量）
  - `orchestration/*`（业务无关 Job Runtime，零 System B 字面量）
- **但通用层被系统性地开洞**：8 个层面出现 System B 专用字面量与专用分支。

**因此，解耦的本质是"把特化点外提为扩展点"，而不是"重排模块依赖"。** 这决定了后面路线建议的形态：不需要大搬家，需要的是把"硬编码的 System B 分支"逐个换成"通用扩展点 + System B 插件注册"。

### 3.2 通用层污染清单（按严重度排序）

#### P0：通用策略注册与校验层

| 位置 | 污染形式 |
| --- | --- |
| [strategies/registry.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/strategies/registry.py#L88-L96) | 通用**默认注册表**直接 import 并注册 3 个 System B 策略（`SystemBBasicStrategy` / `SystemBAuthorizationStrategy` / `SystemBPortfolioStrategy`） |
| [strategies/builtin/__init__.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/strategies/builtin/__init__.py#L28-L30) | 通用 `builtin` 包导入 System B 策略并列入 `__all__` |
| [strategies/__init__.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/strategies/__init__.py#L33-L52) | 通用策略包对外导出 System B 专用校验函数与策略类 |
| [strategies/validation.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/strategies/validation.py#L323-L352) | 通用校验模块硬编码 System B 专用字段元组 `_SYSTEM_B_FACT_FIELDS` 与专用函数 `validate_system_b_portfolio_input` |
| [strategies/validation.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/strategies/validation.py#L494-L500) | 通用归一化分发按**硬编码策略码**开分支：`elif strategy.definition.code == "system_b_portfolio"` |

**违反 S1、S2。** 这是最核心的一处：通用注册表在导入时就把 System B 拉进了通用命名空间，S4（可独立启停）在当前结构下无法成立。

#### P0：通用契约 schema

| 位置 | 污染形式 |
| --- | --- |
| [contracts/schema.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/contracts/schema.py#L164-L217) | 通用 schema 模块导入 System B 表名常量 |
| [contracts/schema.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/contracts/schema.py#L883-L1197) | 通用 schema 文件内直接定义 8 张 System B 表的 `TableSchema` |
| [contracts/schema.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/contracts/schema.py#L1618-L1690) | 通用 schema 内定义 Task09 的 4 张 System B 表 |
| [contracts/schema.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/contracts/schema.py#L1702-L1761) | 通用 `ALL_TABLES` 把 System B 表与全库通用表并列注册 |
| [contracts/__init__.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/contracts/__init__.py#L319-L360) | 通用契约包聚合导出 System B 全部符号、表对象与导出白名单 |

**违反 S1。** `contracts/` 在架构上是"持久化数据结构的唯一事实来源（SSOT）"，现在它同时是 System B 的表定义容器。

#### P1：通用指标注册与分层枚举

| 位置 | 污染形式 |
| --- | --- |
| [indicators/registry.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/indicators/registry.py#L23-L26) | 通用指标注册表 import 具体子系统 `qrp_atlas.indicators.system_b.detector` |
| [indicators/definitions.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/indicators/definitions.py#L11-L19) | 通用分层层级枚举 `IndicatorLayer` 中硬编码成员 `SYSTEM_B = "system_b"` |
| [indicators/parameterized.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/indicators/parameterized.py#L263-L272) | 通用参数化指标注册表内置 `_legacy_system_b_adapter` |
| [indicators/parameterized.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/indicators/parameterized.py#L446-L459) | 通用注册表登记 `system_b_states` 请求及别名映射 |
| [indicators/__init__.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/indicators/__init__.py#L62-L81) | 通用指标包导出 System B 状态机/排名等符号 |

**违反 S1。** 通用枚举为具体子系统开了一档（`IndicatorLayer.SYSTEM_B`），意味着"通用分层"已经被具体业务污染。

#### P1：通用回测执行与产品目录

| 位置 | 污染形式 |
| --- | --- |
| [backtest/harness/runner.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/backtest/harness/runner.py#L130-L136) | 通用执行器硬编码 `if subject.code == "system_b_portfolio": max_positions = 6; max_weight = 0.25` |
| [backtest/harness/runner.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/backtest/harness/runner.py#L306-L314) | 通用 runner 二次特判 `system_b_active_pools` |
| [backtest/harness/models.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/backtest/harness/models.py#L367-L377) | 通用请求模型内硬编码 `system_b_active_pools` universe 名称与拒绝分支（而 `ALLOWED_UNIVERSE_PRESETS` 并不含它——专为 System B 写了一段特判） |
| [backtest/harness/strategy_driver.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/backtest/harness/strategy_driver.py#L51-L59) | 通用策略驱动器内定义 System B 专用函数 `run_system_b_day_by_day_replay(...)` |
| [backtest/harness/strategy_driver.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/backtest/harness/strategy_driver.py#L190-L222) | 按 `code == "system_b_portfolio"` 路由到逐日回放 |
| [backtest/product/catalog.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/backtest/product/catalog.py#L16-L50) | 通用产品目录硬编码 `system_b_basic` 的家族 / 范围 / 支持集 |
| [backtest/product/service.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/backtest/product/service.py#L336-L341) | 通用产品服务硬编码 `if strategy_code == "system_b_basic": windows.append(10)` |

**违反 S1、S2、S3。** 逐项都是"通用能力"没有第二使用者的证据——为了 System B 一个使用者，在通用执行器里写死了执行参数、universe 名称和回放模式。

#### P1：通用 API 装配

| 位置 | 污染形式 |
| --- | --- |
| [api/server.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/api/server.py#L26-L33) | 通用路由模块列表中包含 `system_b`、`system_b_pools` |
| [api/server.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/api/server.py#L87-L88) | 通用应用装配处显式 `app.include_router(...)` |

**违反 S1、S2。** API 装配是天然的扩展点（`include_router` 本身就是注册机制），当前却是硬编码调用。

#### P2：通用运维审计与配置

| 位置 | 污染形式 |
| --- | --- |
| [config/operations.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/config/operations.py#L16-L19) | 通用审计模块导入 System B episode/pool 表 |
| [config/operations.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/config/operations.py#L91-L99) | 定义 `_EPISODE_DATABASE_TABLES`、`_POOL_DATABASE_TABLES` |
| [config/operations.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/config/operations.py#L300-L310) | 把 `system_b_episode` / `system_b_pools` 作为固定审计项 |
| [pipeline/market_facts.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/pipeline/market_facts.py#L16-L23) | 通用市场事实模块直接借用 `SystemBMarketFactStatus` 枚举值定义通用常量 |

**违反 S1。** 最后一条尤其值得注意：通用市场事实的常量语义，其枚举来源是 System B 命名空间。

#### P2：通用 Pipeline 契约目录与打包

| 位置 | 污染形式 |
| --- | --- |
| [pipeline/contract_catalog.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/pipeline/contract_catalog.py#L10-L33) | 通用"生产准入契约模块目录"硬编码 4 个 System B 契约模块 |
| [pyproject.toml](file:///e:/projects/qrp-atlas/pyproject.toml#L33-L35) | 通用包定义 3 个 System B 专用 console 入口 |
| [pipeline-registry.json](file:///e:/projects/qrp-atlas/deploy/pipeline/pipeline-registry.json#L86-L167) | 生产根注册表内混排 8 个 System B 节点（含 1 条 PLANNED） |

**违反 S1、S2。**

### 3.3 典型纠缠案例：一个"文档承诺被代码推翻"的样本

这是本次调研中最有代表性的发现，值得单独记录：

**文档承诺**（[Task07-C 设计书](file:///e:/projects/qrp-atlas/docs/QRP产品蓝图v1.1/Task07/Task07-C_SystemB_Portfolio_Constraint_Resolution_Final_Target_设计书.md) 第 5 节）：

> 「不新增 Common `PortfolioIntent`；不新增 Common `ADD` action；不修改 generic equal-weight semantics 来硬适配 System B；**不把 System B 1/8 / 30% / max6 写进 generic `weights.py`**；如需正式挂载 Registry，最多增加最小、显式的 System B input-normalizer 路由，**不建设 validator plugin registry**。」

**代码实际**：

- `generic weights.py` 确实干净（承诺被遵守）；
- 但同一个约束在 [backtest/harness/runner.py#L130-L136](file:///e:/projects/qrp-atlas/src/qrp_atlas/backtest/harness/runner.py#L130-L136) 以 `max_positions = 6 / max_weight = 0.25` 的形式**在通用执行器里复发了**。

**这说明**：现有护栏是"逐点、逐文件的口头约束"，没有被转成机械化门禁。约束的**意图**（不要把 System B 参数写进通用层）没有被抽象成**规则**（通用层不得出现策略码分支），因此换个文件就绕过了。

同类模式还有 [strategies/validation.py#L494-L500](file:///e:/projects/qrp-atlas/src/qrp_atlas/strategies/validation.py#L494-L500)：Task07-C 允许"最小、显式的 System B input-normalizer 路由"，代码忠实实现了它——**但"允许一条显式路由"在实践中等价于"通用校验层永久包含一条 System B 分支"**。文档措辞的宽容度，最终变成了架构上的永久负债。

---

## 4. 成因分析：为什么会纠缠到这个程度

**成因一：通用层没有第二个使用者，"通用"从未被验证（违反 S3）。**

v1.1 蓝图 README 明确宣告：

> 「QRP v1.1 的**唯一正式生产策略主线是 System B**。」

当平台上只有 System B 一个真实使用者时，"补齐通用能力"实质上永远等同于"为 System B 定制"。`_SYSTEM_B_FACT_FIELDS`、`IndicatorLayer.SYSTEM_B`、`system_b_active_pools`、逐日回放模式——这些都被写成"通用能力"，但没有任何第二个策略验证过它们的通用性。

**成因二：缺少插件机制，"补通用层"是唯一的可用落点。**

当 System B 需要一个新能力时，可选项只有两个：放进 System B 命名空间（但缺少扩展点，框架不认识），或者放进通用层（框架认识，但要硬编码）。当前架构没有提供第三条路，于是每次都选后者。

**成因三：防御性约束防住了"过度设计"，没防住"过度特化"。**

Task07 系列的护栏设计得非常精准，但它防御的威胁是"System B 反向拉扯 QRP 变成 Framework v2"：

> 「正确顺序：System B 业务需求→暴露 Common 缺口→补最薄、可复用的一层→返回 System B 主线。禁止：发现抽象机会→Strategy Framework v2→plugin / external / account / OMS 扩建→再回来实现 System B」

这条约束成功阻止了过度工程（这是它的功劳），但它**没有**约束"补的这最薄一层必须是与具体策略无关的"。于是"最薄的一层"事实上成了"最薄的 System B 特化层"。

**成因四：规模已经越过临界点，局部补丁不再有效。**

1.24 万行专属源码、13 张表、11 条契约、8 个编排节点、5 个迁移。这个体量下，"再补一个小特判"的边际成本看起来很低，但累积效应是通用层的可理解性被持续侵蚀。

**特别注意：根因不是"设计能力不足"，而是"缺少贯穿始终的机械门禁"。** System B 的业务建模（状态机、轮次、分池、排名、授权、组合约束）质量很高，文档约束的**意图**也一直是正确的；问题出在意图没有被固化为可执行的检查。

---

## 5. 文档体系混乱诊断

代码之外，文档层的混乱同样需要记录，因为它直接影响后续任何解耦动作的可执行性。

### 5.1 Task 编号同名不同义（最高优先级问题）

| 编号 | v1.0 含义 | v1.1 含义 |
| --- | --- | --- |
| `Task07` | 真实回测产品主链 / 横截面动量 / 事件驱动 / 结果封板 / 声明式策略（`17~23_任务07_*.md`） | System B Portfolio Target / Holding Entry Exit / Constraint Resolution（`Task07/Task07-A/B/C`） |

两者共用 `Task07-A/B/C` 字面编号，指向完全不同的业务。任何只写"Task07"的引用都是歧义。

### 5.2 命名与目录体系混用

- **大小写不一致**：`task00` / `task03` / `task04` / `task05` / `task06`（小写）与 `Task07` / `Task08` / `Task09`（大写）并存于同一层级。
- **四种命名法并存**：`taskNN` 数字式、`TaskNN` 数字式、`episode-segment-refinement` 语义式、`pipeline-refactor-tasks` 另一套 `00~06` 编号式。
- **分隔符不一致**：`Task07-A_SystemB_...\_设计书.md`（下划线）与 `Task04-A Implementation Plan.md`（空格）与 `M4 Effective Member Rule v1.0.0.md`（空格）混用。
- **同一主题被拆到两个目录**：Task03 的旧说明在 [task03/README.md](file:///e:/projects/qrp-atlas/docs/QRP产品蓝图v1.1/task03/README.md)，生产说明在 `system-b-state-productionization/README.md`。
- **文档目录里混入代码**：`task06/` 下存在 `audit_task06_b_rc1.py`、`audit_task06_b_rc2.py` 两个审计脚本。

### 5.3 SSOT 至少四处并存主张

| # | SSOT 主张 | 出处 |
| --- | --- | --- |
| 1 | 业务规则 SSOT = `MyTradingSystem`（跨仓） | [02_架构与跨仓边界.md](file:///e:/projects/qrp-atlas/docs/QRP产品蓝图v1.1/02_架构与跨仓边界.md) |
| 2 | 数据定义 SSOT = `contracts/` 下 3 个核心文件 | [ssot_data_model.md](file:///e:/projects/qrp-atlas/docs/ssot_data_model.md) |
| 3 | Dual SSOT = `quant.db`（运行时）+ `canonical/**`（恢复） | [mvp_project_structure_v1.3.md](file:///e:/projects/qrp-atlas/docs/architecture/mvp_project_structure_v1.3.md) |
| 4 | Pipeline 业务语义 SSOT = 源码 `PipelineContract` | [12_Pipeline正式开发规则.md](file:///e:/projects/qrp-atlas/docs/QRP产品蓝图v1.1/12_Pipeline正式开发规则.md) |

这四者在**不同维度**上其实可以共存（规则 / 数据结构 / 运行时存储 / 生产语义），但文档从未说明它们的关系与优先级，实践中被当成互相竞争的权威。

### 5.4 文档与代码/生产实际不一致

依据 [Task09 生产编排 Read-Only 调研报告](file:///e:/projects/qrp-atlas/docs/QRP产品蓝图v1.1/Task09/生产编排Read-Only调研报告.md)（注意：该报告成文于 Task09 实施之前，当前 repo 侧已补齐 Task09 契约链，生产侧状态需以服务器实测为准）：

- repo 侧 `production-job-definitions.json` 仅 2 个 disabled 的 research job，**无任何 System B job**；而 live 有 29 个 job。
- 生产权威 manifest 未纳入 git，无版本历史，无法固定审计基线。
- 当时 System B 的 authorization / score / strategy / target 在 repo 无 PipelineContract 注册（该缺口已由 `ca94aa2 feat(task09): add formal System B daily chain` 与 [system_b_task09_contracts.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/pipeline/system_b_task09_contracts.py) 补齐）。
- 报告原文的结论是「Task09 Contract Graph Audit can start: **NO**」。
- [10_Pipeline基础运行框架.md](file:///e:/projects/qrp-atlas/docs/QRP产品蓝图v1.1/10_Pipeline基础运行框架.md) 说"Hermes 仍是唯一生产调度权威"，实际已是 systemd `qrp-atlas-jobs.service`。

### 5.5 索引失真与主题缺位

- v1.1 README 的文档索引只列了 `01~05`、`09`、`10` 及少数 README，**未收录** `06`、`08`、`11`、`12`、`13`、`Task07/08/09`、`task05`、`task06`、`episode-segment-refinement`、`pipeline-refactor-tasks`、`task00`。
- `docs/` 根目录散落 `api.md`、`ssot_data_model.md`、`daily-job-rollout-decisions.md`、`pipeline-usage.md`、`runtime-configuration.md`、`指标.md`、`项目诊断报告.html` 等，均未被蓝图索引收录，且部分内容（如 `api.md` 的 `M1_core / M2_front / M3_identifiable` 身份布尔位、`market_phase` 表）与 v1.1 Task06 的"M1/M2/M3 是三个评分维度、不再是身份"**直接冲突**。
- 唯一的《[项目诊断报告.html](file:///e:/projects/qrp-atlas/docs/项目诊断报告.html)》全文检索「System B / 系统 B / 耦合 / 外挂 / 插件 / 跨仓 / 纠缠」**均无匹配**——即该报告完全没有覆盖本主题，其"不涉及架构推翻"的结论不能用于本议题。

### 5.6 已存在的自我认知

值得记录的是，文档曾以"防御性条款"的形式**间接承认**过这种拉扯，例如：

> 「它只服务 System B，**不注册成 Common validator plugin**，不新增 `StrategyInputScope`。」—— [Task07-B 设计书](file:///e:/projects/qrp-atlas/docs/QRP产品蓝图v1.1/Task07/Task07-B_SystemB_Holding_Entry_Exit_Decision_设计书.md) §6.1
> 「**System B 是独立业务系统，不因源码位于 `pipeline/` 就纳入本轮 PipelineContract 批量重构**」—— [pipeline-refactor-tasks/00_执行总览.md](file:///e:/projects/qrp-atlas/docs/QRP产品蓝图v1.1/pipeline-refactor-tasks/00_执行总览.md) §4

以及 2026-09-04 的「中期评估调整」（`SCOPE_CHANGE`）：

> 「调整属于**语义与产品身份治理**，不要求对已有代码做大规模物理重构……内置与外部插入能力原则上复用同一套 contracts、registry、pipeline、database、runtime 与 result 基础设施。」—— [02_架构与跨仓边界.md](file:///e:/projects/qrp-atlas/docs/QRP产品蓝图v1.1/02_架构与跨仓边界.md) 文件头

但**没有任何一份审计或验收报告把"System B 与平台耦合"直接定性为缺陷**（Task07-A 对抗审计结论为 `BLOCKER 0 / MAJOR 0`）。即：问题是"被感知到并逐点防御"，但从未"被定性、被量化、被治理"。

---

## 6. 解耦路线建议

### 6.1 目标形态

```text
QRP 通用平台（不含任何 system_b 字面量）
  ├── 扩展点协议（Extended Point Protocol）
  │     ├── StrategyProvider
  │     ├── IndicatorProvider
  │     ├── TableSchemaProvider
  │     ├── PipelineContractProvider
  │     ├── ApiRouterProvider
  │     ├── ProductCatalogProvider
  │     └── OpsAuditProvider
  │
  └── 启动时聚合所有已注册 Provider

System B 插件包（qrp_atlas_system_b 或 qrp_atlas.plugins.system_b）
  ├── 自带 contracts / 表定义
  ├── 自带 indicators / strategies / pipeline contracts
  ├── 自带 API 路由 / 产品目录项 / 审计项
  └── 通过 Provider 接口挂载
```

**验收判据（对应 S1–S4）**：

1. `grep -r "system_b\|SYSTEM_B" src/qrp_atlas/{contracts,strategies,indicators,backtest,api,orchestration,config}` 在**通用文件中零命中**（System B 命名空间内的文件除外）。
2. 移除 System B 插件注册后，`python -m pytest` 通用测试全绿、API 可启动、`qrp-atlas-jobs` 可运行。
3. 新增第二个策略（哪怕是 fixture 级别的合成策略）只通过 Provider 接入，不需要改通用层。

### 6.2 阶段 0：建立门禁与基线（先做，且必须做）

| 项 | 内容 |
| --- | --- |
| 动作 | 新增一个只读检查（测试或 lint 脚本），扫描通用层文件中的 `system_b` / `SYSTEM_B` 字面量，输出**清单**而非直接失败 |
| 目的 | 把"意图"变成"可机械执行的规则"，堵住 3.3 节那类"换个文件就绕过"的漏洞 |
| 涉及文件 | 新增检查模块（如 `tests/architecture/test_no_system_b_in_core.py`）；不改任何业务代码 |
| 风险 | 极低（纯新增，先以 warning 形式存在） |
| 验收 | 检查输出一份与第 3.2 节一致的基线清单，且清单可被后续阶段用于度量收敛进度 |

同时建议在此阶段**冻结新增污染**：任何新代码不得新增通用层 System B 字面量，例外需显式记录。

### 6.3 阶段 1：把硬编码分支改成扩展点（收益最高，风险最低）

这是整个路线中**性价比最高**的一步：涉及的都是"通用代码里写死了一个策略码分支"，替换方式机械、语义等价、可逐项验证。

| 序 | 动作 | 涉及文件 | 风险 |
| --- | --- | --- | --- |
| 1.1 | 通用注册表只注册通用策略；System B 三个策略改由插件注册入口挂载 | [strategies/registry.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/strategies/registry.py#L88-L96)、[builtin/__init__.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/strategies/builtin/__init__.py#L28-L30)、[strategies/__init__.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/strategies/__init__.py#L33-L52) | 中（触及策略发现路径，需回归策略目录/测试） |
| 1.2 | 通用校验分发改为按 `StrategyDefinition` **自带的 validator 引用**分发，而非按策略码开 `elif` | [strategies/validation.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/strategies/validation.py#L494-L500) | 中（需给 StrategyDefinition 增加可选 validator 字段，属通用模型扩展） |
| 1.3 | `_SYSTEM_B_FACT_FIELDS` 与 `validate_system_b_portfolio_input` 移入 System B 命名空间 | [strategies/validation.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/strategies/validation.py#L323-L352) | 低 |
| 1.4 | 通用指标注册表移除 System B import；`IndicatorLayer.SYSTEM_B` 从通用枚举移出，改为扩展层值 | [indicators/registry.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/indicators/registry.py#L23-L45)、[definitions.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/indicators/definitions.py#L11-L19) | 中（枚举变更是契约级，需检查持久化与 API 序列化引用） |
| 1.5 | `_legacy_system_b_adapter` 与 `system_b_states` 注册项移出通用参数化注册表 | [indicators/parameterized.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/indicators/parameterized.py#L263-L272) | 低 |
| 1.6 | API 装配改为遍历已注册的 router provider | [api/server.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/api/server.py#L26-L33) | 低 |
| 1.7 | 回测执行参数（`max_positions` / `max_weight` / 逐日回放模式 / universe 名称）改为**策略定义自带声明**，通用 runner 只消费声明 | [harness/runner.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/backtest/harness/runner.py#L130-L136)、[harness/models.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/backtest/harness/models.py#L367-L377)、[harness/strategy_driver.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/backtest/harness/strategy_driver.py#L51-L59) | 中高（涉及回测结果复现一致性，需回归对比） |
| 1.8 | 产品目录项（家族/范围/支持集/窗口）改为策略自带 product metadata | [product/catalog.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/backtest/product/catalog.py#L16-L50)、[product/service.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/backtest/product/service.py#L336-L341) | 低 |
| 1.9 | 运维审计项改为注册式 | [config/operations.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/config/operations.py#L91-L99) | 低 |
| 1.10 | 通用市场事实常量摆脱 `SystemBMarketFactStatus` 依赖，改为独立通用枚举 | [pipeline/market_facts.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/pipeline/market_facts.py#L16-L23) | 低 |

**阶段 1 完成后的效果**：通用层对 System B 的"主动认知"消失，只剩"插件注册"这一条被动关系。此时阶段 0 的门禁应从 warning 升级为**失败**。

### 6.4 阶段 2：契约与数据命名空间隔离

| 项 | 动作 | 涉及文件 | 风险 |
| --- | --- | --- | --- |
| 2.1 | 把通用 `schema.py` 中的 8 张 System B 表 + Task09 的 4 张表定义，整体迁入 System B 契约模块 | [contracts/schema.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/contracts/schema.py#L883-L1197) | 中（表定义是 SSOT，迁移需保证零语义变化） |
| 2.2 | `ALL_TABLES` 从"静态元组"改为"通用核心表 + 已注册 Provider 表"的聚合结果 | [contracts/schema.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/contracts/schema.py#L1702-L1761) | 中（影响所有遍历 `ALL_TABLES` 的建表/校验/审计路径） |
| 2.3 | `contracts/__init__.py` 的 System B 导出白名单收敛为"仅保留通用契约 API" | [contracts/__init__.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/contracts/__init__.py#L319-L360) | 低（但会破坏现有 import 路径，需同步消费者） |

**注意**：`System B` 独立数据库（`system_b_episode.duckdb`、`system_b_pools.duckdb`）是**正面资产**，说明隔离思路在数据层已经被实践过。本阶段只是把同样的隔离原则推广到 schema 定义层。

### 6.5 阶段 3：Pipeline 与生产编排插件化

| 项 | 动作 | 涉及文件 | 风险 |
| --- | --- | --- | --- |
| 3.1 | `CONTRACT_MODULES` 从硬编码列表改为自动发现 | [pipeline/contract_catalog.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/pipeline/contract_catalog.py#L10-L33) | 低 |
| 3.2 | `pipeline-registry.json` 中 System B 节点与通用节点分离（按 `owner` 字段拆分为两个注册表或显式分区） | [pipeline-registry.json](file:///e:/projects/qrp-atlas/deploy/pipeline/pipeline-registry.json#L86-L167) | 中（生产编排文件，需与服务器 manifest 对齐后再动） |
| 3.3 | System B CLI 入口收归插件包 | [pyproject.toml](file:///e:/projects/qrp-atlas/pyproject.toml#L33-L35) | 低 |
| 3.4 | 清理 repo 与生产 manifest 的漂移，把生产权威定义纳入 git | 见 §5.4 | 高（属独立议题，建议单独任务） |

### 6.6 阶段 4：文档治理

| 项 | 动作 |
| --- | --- |
| 4.1 | 解决 `Task07` 编号冲突：给 v1.0 与 v1.1 的 Task 号加命名空间前缀（如 `v1.0/Task07`、`v1.1/Task07`），或为 v1.1 启用全新编号段 |
| 4.2 | 统一目录命名规范（大小写、分隔符、是否带编号），并明确"语义式目录"与"编号式目录"的适用场景 |
| 4.3 | 明确四类 SSOT 的**维度与优先级关系**（规则 / 数据结构 / 运行时存储 / 生产语义各管什么），写入单一索引文档 |
| 4.4 | 重写 v1.1 README 的文档索引，使其与磁盘实际一致；新增文档时同步更新索引 |
| 4.5 | 处置 `docs/` 根目录的散落文档：与 v1.1 冲突的（如 `api.md` 的 M1/M2/M3 身份语义）标注废弃或归档 |
| 4.6 | 把 System B 相关文档从混排的 `taskNN / TaskNN` 体系收拢到独立的一级命名空间（如 `docs/system-b/`），不再寄生在版本蓝图目录下 |

### 6.7 阶段依赖与顺序

```text
阶段 0（门禁与基线）
   ↓
阶段 1（硬编码 → 扩展点）        ← 可立刻启动，收益最高
   ↓
阶段 2（契约与表命名空间）
   ↓
阶段 3（Pipeline 与编排）        ← 3.4 需与服务器侧协同，可独立并行
   ↓
阶段 4（文档治理）               ← 与前四阶段无强依赖，可随时并行
```

---

## 7. 风险与明确不做的事

### 7.1 明确不建议做的事

| 不建议 | 原因 |
| --- | --- |
| **物理拆分（独立仓库 / 独立进程 / 独立服务）** | 成本极高、收益有限。System B 的语义独立性已在 `semantic_owner = SYSTEM_B`、独立 `.duckdb`、独立 CLI 上得到保留；物理拆分不会新增架构价值，反而会切断已有的开发与验证效率 |
| **建设 Strategy Framework v2 / validator plugin registry** | 现有文档已明文禁止，且属典型"为未知未来过度设计"。本报告主张的是**外提已有的特化点**，不是发明新框架 |
| **一次性大规模重构** | 1.24 万行规模下，大爆炸式重构无法验证，且会与 v1.1 主线交付冲突。必须按阶段推进，每阶段可独立验收、可回滚 |
| **在没有第二个使用者的情况下继续"补通用能力"** | 违反 S3。任何新增通用抽象都应先回答"除 System B 外还有谁会用" |
| **为了物理目录整洁而搬迁文件** | 纯文件搬迁不产生解耦收益，只会制造 diff 噪声与合并冲突 |

### 7.2 主要风险

| 风险 | 说明 | 缓解 |
| --- | --- | --- |
| 回测复现性破坏 | 阶段 1.7 改动通用 runner 的执行参数来源，可能改变历史回测结果 | 改动前后对同一策略区间做结果对比，确保数值一致 |
| 契约级变更外溢 | 阶段 1.4 的 `IndicatorLayer` 枚举变更、阶段 2 的表定义迁移会影响持久化与 API 序列化 | 变更前全量检索引用点；`ALL_TABLES` 聚合行为需专门回归 |
| 生产编排联动 | 阶段 3.2 涉及生产注册表，且当前 repo 与 live 已有漂移（§5.4） | 先完成 3.4 的基线对齐，再动编排文件；未经授权不触碰服务器 |
| 与主线交付冲突 | v1.1 仍在推进中 | 每个阶段作为独立任务分支，不混入功能交付 |
| 门禁误伤 | 阶段 0 的扫描可能命中合法用例（如 System B 命名空间内文件、必要的跨仓协议测试） | 白名单以"路径命名空间"为维度，而非逐条豁免 |

### 7.3 本报告的边界

- 本报告**只做静态只读调研**，未修改任何代码、未创建临时脚本、未运行测试。
- §5.4 涉及生产状态的部分，依据的是仓库内既有调研文档，**未连接 Linux 服务器实测**；相关结论需以服务器实测为准。
- 本报告给出的阶段划分与验收判据是**建议**，不构成对 v1.1 主线的变更指令；任何执行都需经项目所有者批准并作为独立任务分支推进。

---

## 8. 附录：证据索引

### 8.1 保持干净的通用模块（解耦锚点）

- [strategies/models.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/strategies/models.py)
- [strategies/protocol.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/strategies/protocol.py)
- [pipeline/registry.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/pipeline/registry.py)
- [pipeline/production_jobs.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/pipeline/production_jobs.py)
- `backtest/portfolio/*`（通用组合回测引擎）
- `orchestration/*`（业务无关 Job Runtime）

### 8.2 System B 专属资产（应保留在插件命名空间）

- 契约：[contracts/system_b.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/contracts/system_b.py)
- 指标：`indicators/system_b/`（8 文件 3,278 行）
- 策略：`strategies/builtin/system_b_{basic,authorization,decision,portfolio}.py`
- Pipeline：`pipeline/system_b/`、`system_b_asset_rank/`、`system_b_episode/`、`system_b_pools/`、`system_b_theme_rank/`、`system_b_task09.py`
- 契约模块：`pipeline/system_b_contracts.py` 等 4 个
- API：[api/routes/system_b.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/api/routes/system_b.py)、[api/routes/system_b_pools.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/api/routes/system_b_pools.py)、[api/schemas/system_b.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/api/schemas/system_b.py)、[api/system_b_serialization.py](file:///e:/projects/qrp-atlas/src/qrp_atlas/api/system_b_serialization.py)
- 独立数据库：`system_b_episode.duckdb`、`system_b_pools.duckdb`（**正面资产**）
- 迁移：`deploy/duckdb/002_system_b_episode.sql`、`003_system_b_pools.sql`、`008_system_b_asset_rank.sql`、`009_system_b_theme_rank.sql`、`010_system_b_task09.sql`

### 8.3 架构规则类文档

- [src/qrp_atlas/AGENTS.md](file:///e:/projects/qrp-atlas/src/qrp_atlas/AGENTS.md)（分层与依赖方向权威规则）
- [QRP_v1.0_核心架构文档.md](file:///e:/projects/qrp-atlas/docs/核心架构v1.0/QRP_v1.0_核心架构文档.md)
- [02_架构与跨仓边界.md](file:///e:/projects/qrp-atlas/docs/QRP产品蓝图v1.1/02_架构与跨仓边界.md)
- [04_SystemB工程映射.md](file:///e:/projects/qrp-atlas/docs/QRP产品蓝图v1.1/04_SystemB工程映射.md)
- [12_Pipeline正式开发规则.md](file:///e:/projects/qrp-atlas/docs/QRP产品蓝图v1.1/12_Pipeline正式开发规则.md)

### 8.4 约束与审计类文档

- [Task07-A_SystemB_Portfolio_Target_Contract_Integration_设计书.md](file:///e:/projects/qrp-atlas/docs/QRP产品蓝图v1.1/Task07/Task07-A_SystemB_Portfolio_Target_Contract_Integration_设计书.md)（§2.1 / §2.2 防扩张约束）
- [Task07-A_对抗审计结论.md](file:///e:/projects/qrp-atlas/docs/QRP产品蓝图v1.1/Task07/Task07-A_对抗审计结论.md)
- [Task07-B_SystemB_Holding_Entry_Exit_Decision_设计书.md](file:///e:/projects/qrp-atlas/docs/QRP产品蓝图v1.1/Task07/Task07-B_SystemB_Holding_Entry_Exit_Decision_设计书.md)（§6.1）
- [Task07-C_SystemB_Portfolio_Constraint_Resolution_Final_Target_设计书.md](file:///e:/projects/qrp-atlas/docs/QRP产品蓝图v1.1/Task07/Task07-C_SystemB_Portfolio_Constraint_Resolution_Final_Target_设计书.md)（§5，3.3 节案例来源）
- [pipeline-refactor-tasks/00_执行总览.md](file:///e:/projects/qrp-atlas/docs/QRP产品蓝图v1.1/pipeline-refactor-tasks/00_执行总览.md)（§4 System B 排除声明）
- [Task09/生产编排Read-Only调研报告.md](file:///e:/projects/qrp-atlas/docs/QRP产品蓝图v1.1/Task09/生产编排Read-Only调研报告.md)
- [Task09/每日运行产品设计.md](file:///e:/projects/qrp-atlas/docs/QRP产品蓝图v1.1/Task09/每日运行产品设计.md)
- [项目诊断报告.html](file:///e:/projects/qrp-atlas/docs/项目诊断报告.html)（全文未覆盖本主题）

---

*本报告基于 `develop/v1.1` @ `d991880` 的静态只读调研生成，所有结论均附可复核的文件路径与行号证据。*
