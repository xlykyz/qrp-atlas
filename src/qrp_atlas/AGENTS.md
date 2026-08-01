# `src/qrp_atlas` Agent Rules

本文件适用于 `src/qrp_atlas/` 及其全部子目录。

开发机或工作区根目录可能存在未提交到仓库的本地 `AGENTS.md`，用于定义该环境下的通用 Agent 配置。若根目录 `AGENTS.md` 存在，则与本文件同时生效；规则冲突时，以路径更具体、距离目标文件更近的 `AGENTS.md` 为准。

QRP v1.0 的权威架构说明见 [`docs/核心架构v1.0/QRP_v1.0_核心架构文档.md`](../../docs/核心架构v1.0/QRP_v1.0_核心架构文档.md)。v1.0 后端产品基线已经完成最终验收；后续功能可以扩充，但不得破坏已经封版的模块职责、时间语义、owner 隔离和依赖方向。

## 核心原则

QRP 的核心量化抽象由低到高为：

```text
contracts → indicators → strategies
```

各层定位：

```text
contracts          定义标准数据与时间语义
orchestration       通用 Job 编排、调度、资源 lease、状态持久化与结果审计
pipeline            生产、版本化和读取标准数据；通过 job_adapter 接入 orchestration
indicators         计算客观指标、特征、因子与状态
strategies         输出交易决策和目标
backtest runtime   准备输入并运行策略
backtest engine    模拟成交、资金、持仓与绩效
backtest research  使用未来结果评价因子和策略
backtest product   编排目录、任务、运行、比较与重放
results            保存和读取标准结果与复现快照
users              定义稳定内部用户
 auth              解析身份、密码和会话
api                编排并暴露应用能力
```

必须始终保持：

- 数据定义与数据生产分离；
- 客观事实与交易决策分离；
- 策略定义与策略运行分离；
- 策略决策与成交模拟分离；
- 历史决策与未来结果评价分离；
- 量化核心与身份控制面分离；
- 核心业务逻辑与 API/前端分离；
- 下层模块不得反向依赖上层模块；
- 所有历史能力遵守 point-in-time；
- 所有用户业务数据遵守 owner 隔离；
- 相同版本、参数、数据快照和执行配置应可复现。

## 允许的依赖方向

```text
pipeline/job_adapter → orchestration
pipeline → contracts
indicators → contracts
strategies → indicators / contracts
backtest runtime → contracts / indicators / strategies / backtest engine
backtest engine → backtest models
backtest research → prepared data / indicators / strategies / backtest
backtest product → strategies / runtime / research / engine / results
results → backtest result models
users → 自身领域对象与持久化协议
auth → users
api → auth / users / indicators / strategies / backtest / results
frontend → api
```

`orchestration/` 是业务无关的顶级 Job Runtime。它不得导入 `pipeline`、`indicators`、`strategies`、`backtest` 或 `api`，也不得理解 DuckDB 表、市场数据源、交易日期或业务质量规则。Pipeline Contract 的业务语义保留在 `pipeline/`，由 `pipeline/job_adapter.py` 将 `PipelineContract.pipeline_id` 映射为 `JobDefinition.job_id`。正式入口是 `qrp-atlas-jobs`；通用运行库使用 `job_runtime.sqlite3` 和 `job_result`，不得重新引入 `pipeline/runtime`。

正式 `PipelineContract` 通过 `in_process_executor` 在 `qrp-atlas-jobs serve` 进程的线程 worker 内执行；只有普通兼容 Definition 可以通过 argv 子进程执行。正式 executor 不得自行启动写同一 QRP DuckDB 的子进程。

正式 `PipelineContract` 的 `resource_reads` 和 `manual_execution_allowed` 是一等字段：前者声明共享只读资源，`duckdb://<database>#<object>` 表级资源只能用于读取；后者控制 `run`/人工依赖提交，部署 selection 不得覆盖。写入同一 DuckDB 文件必须使用其 canonical 数据库级 writer lock，例如 `quant_db_writer`。通用 Orchestration 锁引擎仍可为兼容 JSON Definition 提供表级资源锁，但该兼容能力不改变正式 Contract 的生产写锁规则。

职责必须保持单一：Orchestration 负责 dependency、claim、resource lease、heartbeat、deadline/timeout、retry、恢复和最终 Job 状态；`pipeline.execution.execute_pipeline_contract` 负责目标日期解析、参数校验、输入结构、freshness、业务 executor、completion、质量检查和 `PipelineResult`。不得在任一侧复制另一侧的生命周期。

`PipelineInvocation.execution_control` 必须以同一实例传入 `PipelineRunContext.execution_control`。正式 executor 和检查器必须通过 `check()` 响应取消/到期，并使用 `bounded_timeout()` 限制外部等待；不得在业务层重新创建、替换或绕过该控制器。`PipelineResult` 是业务结果，`JobResult` 是 Orchestration 的 durable 结果记录；每个 `SUCCESS`、`FAILED`、`TIMED_OUT`、`CANCELLED` 或 `SKIPPED` 终态 JobRun 恰好有一条 `JobResult`。没有业务载荷的 timeout、cancel、skip、stale recovery 或 Runner 异常路径使用 `result_type=orchestration`、`source=orchestration`、终态错误元数据和 `business_result=null`，不得伪造业务成功。

正式 Pipeline 的取消和 deadline 是协作式协议：

- 必须将同一个 `ExecutionControl` 从 `PipelineInvocation` 传入 `PipelineRunContext`，不得在业务 executor 中替换或丢弃；
- 外部网络、provider retry/backoff 和其他阻塞等待必须使用 `execution_control.bounded_timeout()` 或等价的受控等待，不能超过当前执行 deadline；
- 输入/freshness/completion/质量检查、外部 I/O 前后以及进入或继续 DuckDB 写事务前必须调用 `execution_control.check()`；
- Python worker 线程不会被强制终止，因此禁止不可取消的无限阻塞、忽略取消状态或在取消后开始新的写事务。

禁止的典型依赖：

```text
contracts → pipeline / indicators / strategies / backtest / api
indicators → strategies / backtest / api
strategies → pipeline / backtest / api / frontend
backtest engine → 具体指标、题材或具体策略
backtest research → 修改历史策略决策
users → auth 或量化核心
auth → indicators / strategies / backtest engine
api → frontend
frontend → DuckDB、文件结果目录或 Python 策略对象
```

依赖方向描述的是模块职责，不要求为了形式统一而制造无意义的 import。

## `contracts/`｜基础契约层

`contracts/` 是持久化数据结构和标准数据语言的唯一事实来源（SSOT），负责：

- 标准字段名；
- DuckDB 表结构；
- 类型、主键和可空性；
- 外部数据源字段映射；
- ticker、日期、交易所、板块和涨跌停等通用约定；
- 发布时间、市场可用时间、业务有效时间、观察时间和修订等 PIT 语义；
- DataFrame 的 schema 对齐、类型标准化与校验；
- 对外公开的基础常量和契约对象。

约束：

- 不查询数据库；
- 不计算指标；
- 不生成交易决策；
- 不模拟成交；
- 不依赖任何业务上层模块。

新增或修改持久化字段、表名、主键、可空性、字段语义、时间语义或数据源映射时，先更新 contracts，再更新生产者、消费者和测试。

不得为兼容错误实现而复制、绕过或静默放宽 contracts。

## `pipeline/`｜数据生产与访问层

`pipeline/` 是外部数据进入系统的强契约边界，负责数据获取、清洗、映射、标准化、版本化、校验、入库和受控数据访问。

正式入库流程原则上为：

```text
外部字段映射
→ schema 对齐
→ 类型与时间标准化
→ 契约校验
→ insert / append-only / upsert DuckDB
```

约束：

- 标准字段名、表名、列清单和主键必须复用 `qrp_atlas.contracts`；
- 外部原始列名必须通过正式映射显式转换；
- 通用市场规则必须复用 `contracts/conventions.py`；
- DataFrame 入库前使用正式契约能力完成对齐和校验；
- 缺列、额外列、非法类型、空结果和兼容策略必须显式处理，不得静默吞掉；
- PIT 数据必须保留来源、可用时间、有效时间和修订，不得以当前记录覆盖历史；
- pipeline 不计算策略指标，不生成交易动作，不承载回测逻辑。

如果 contracts 尚未定义所需结构，应先补充 contracts 及其公开导出，再实现 pipeline。

## `indicators/`｜指标、特征与因子层

`indicators/` 基于调用方准备好的标准数据，计算可复用的客观指标、特征、因子和状态事实。

```text
Factor ⊂ Indicator
```

因子不是独立顶级架构层，不得新增与 `indicators` 平行的 `qrp_atlas.factors` 模块。

指标回答：

> 市场、题材或标的已经发生了什么？

约束：

- 核心计算函数优先接收 DataFrame 或明确结构化输入；
- 不主动查询数据库，不负责数据加载或 PIT 版本选择；
- 可以组合其他底层指标，但不得重复实现已有算法；
- 输出应稳定、可测试，并明确公式、窗口、方向、可用时间和 NaN 语义；
- 不输出 `ENTER`、`HOLD`、`EXIT`、`NO_ACTION` 等交易动作；
- 不包含策略专属权重、Top-N、目标仓位或绝对否决偏好；
- 不处理持仓、成交、成本、收益或未来结果评价；
- 不写数据库；
- 不依赖 `strategies`、`backtest` 或 `api`。

更具体的指标、因子、中性化和未来收益边界见 [`indicators/AGENTS.md`](indicators/AGENTS.md)。

## `strategies/`｜策略定义与决策层

`strategies/` 定义具有明确输入、参数、算法、输出和稳定版本的交易规则单元。

策略回答：

> 面对已经准备好的基础数据和指标，应当做什么？

策略可以：

- 声明 `required_fields` 和 `required_indicators`；
- 定义参数及其类型、默认值和范围；
- 输出标准化的 `ENTER / HOLD / EXIT / NO_ACTION` 决策；
- 输出横截面排名、目标权重和完整调仓快照；
- 通过版本化注册表发布 Python 内置策略；
- 通过受限声明式结构支持用户定义策略。

约束：

- 策略只消费已经准备好的输入，不自行访问数据库；
- 不主动调用 pipeline 加载数据；
- 不重新计算或复制 indicators 中已有指标；
- 不模拟成交，不计算手续费、滑点、收益、净值或回撤；
- 不依赖 `backtest`、`api` 或前端；
- 策略 code 与 version 必须稳定，行为变化应按兼容性决定是否升级版本；
- 已发布声明式版本不可原地改写；
- Python 内置策略和声明式策略必须遵守统一定义、校验和决策模型；
- 声明式策略只能使用受控数据结构和安全求值器，禁止 `eval`、`exec` 或任意代码执行。

策略实现应保持确定性；相同输入、参数和版本应产生可复现决策。

## `backtest/`｜运行、执行、研究与产品层

`backtest/` 包含 runtime、engine、research、product 和 results 等职责。它们可以在同一顶级模块内协作，但不得混淆边界。

### Backtest Runtime

负责：

- 读取策略定义；
- 解析所需基础字段和指标；
- 选择当时可用的 PIT 数据；
- 准备并校验策略输入；
- 调用 indicators；
- 运行 strategies；
- 将标准决策或目标交给执行引擎。

runtime 可以依赖 contracts、indicators、strategies 和通用 engine，但不得把具体策略知识写进 engine。

### Backtest Engine

负责：

- 入场和退出 bar；
- 订单、成交价格与失败原因；
- 共享现金、持仓与目标权重；
- T+1、停牌、涨跌停、整数手和容量约束；
- 手续费、印花税和滑点；
- 交易记录与持仓快照；
- MAE/MFE；
- 收益、净值、回撤和绩效指标。

通用 engine 禁止包含五日线、涨停、节点、题材、System A、System B 或任何具体策略概念。

### Backtest Research

负责：

- forward return；
- IC / Rank IC、因子分组和暴露评价；
- 事件研究与残差研究；
- walk-forward、参数选择、成本压力和稳健性评价。

未来结果只能用于评价，不能影响同一历史时点的 eligible、rank、decision、target 或 execution。研究数据准备必须遵守 PIT 和样本隔离。

### Backtest Product

负责：

- 策略目录和版本查询；
- 任务创建、校验、持久化和状态转换；
- 调用真实 runtime / research / engine；
- 运行历史、比较和重放；
- 将标准结果包与任务状态原子关联；
- 基于当前用户执行 owner 隔离。

产品层不得：

- 建立第二套策略算法或指标注册表；
- 用 mock、fixture 或占位数据冒充真实运行；
- 在任务成功前暴露不完整结果；
- 接受调用方伪造的 owner 访问其他用户数据；
- 重放时绕过锁定请求、数据指纹或策略版本。

### Results

负责标准结果的加载、保存、查询、比较和复现信息，不承担策略算法或成交算法。

正式结果应保留：

- 策略 code、version 与解析后参数；
- 股票池和日期范围；
- 执行与成本配置；
- 订单、成交、交易、持仓、目标和净值；
- 基准、超额、暴露与诊断；
- 数据指纹、锁定产品请求和必要快照；
- owner 归属。

原子写入、覆盖、备份和文件锁语义不得因平台差异或便利性而削弱。

## `users/`｜内部用户层

`users/` 定义稳定内部用户实体、状态和基础资料。

约束：

- 不处理密码、Token、Cookie、JWT 或会话；
- 不感知具体认证供应商；
- 不保存复盘、策略、任务或回测结果；
- 不依赖 `auth` 或量化核心；
- 通过稳定 `user_id` 为业务 owner 归属提供锚点。

更具体规则见 [`users/AGENTS.md`](users/AGENTS.md)。

## `auth/`｜身份认证层

`auth/` 负责确认请求者身份并解析为内部 `UserContext`。

约束：

- 可以依赖 `users`，但用户领域不得反向依赖认证机制；
- 不保存 QRP 业务数据；
- 不依赖 contracts、indicators、strategies 或执行引擎；
- 不明文保存密码或会话 Token；
- `local` 模式必须完全跳过 PostgreSQL；
- `database` 模式必须显式启用，数据库失败时禁止自动降级。

更具体规则见 [`auth/AGENTS.md`](auth/AGENTS.md)。

## `api/`｜应用编排层

API 负责将已有后端能力组织为稳定接口，可以协调多个模块，但不得成为业务算法的存放位置。

约束：

- 不在路由中实现可复用指标；
- 不在路由中实现策略条件或状态机；
- 不在路由中实现成交、成本和绩效算法；
- 请求/响应 DTO 可以使用展示字段，但数据库查询必须尊重真实 schema；
- 涉及新增持久化记录时仍必须遵守相应数据契约；
- 当前用户必须来自可信认证依赖，不接受请求体覆盖；
- 任务、运行、结果、比较和重放必须执行 owner 隔离；
- 安全、只读、临时访问等网关能力不得削弱正式 API 权限模型。

## `config/`｜配置层

配置模块只负责路径、环境变量、外部客户端和运行参数的集中管理。`qrp_atlas.config.settings` 是运行配置的唯一事实来源。

- 普通业务模块禁止直接使用 `os.getenv`、`os.environ.get`、`load_dotenv` 或自行解析同名运行变量；应依赖 `AppSettings`/`get_settings`，旧公开常量只能作为统一配置的兼容适配层；
- 数据库、原始/标准数据、状态、结果、日志和临时目录必须从 settings 路径模型派生，不得按 CWD 拼接 `data/`；
- 不在源码、脚本、unit 或示例中写开发机绝对路径、内网 IP、真实用户名或秘密；通用部署路径和明确的业务服务 URL除外；
- token、DSN、带凭据代理和其他秘密字段必须 `repr=False`，展示仅允许 `configured / not configured`，异常和日志必须脱敏；
- 目录创建应集中、幂等，`QRP_READ_ONLY=true` 时不得创建缺失持久目录或打开写连接；
- 测试必须使用临时目录和显式 `environ`/override，不能读取或污染真实用户目录、仓库 `data/` 或进程私有配置；
- `.env.example` 与 `SUPPORTED_ENV_VARS` 必须同步，并说明类型、默认值、场景和秘密属性；
- `qrp-atlas-config setup` 只能编排 `AppSettings`、`initialize_runtime()`、统一 doctor 和包内数据库初始化 API，不得复制配置解析、路径派生、默认值或 schema SQL；
- setup 的交互层必须可注入输入输出，非 TTY 不得等待；秘密不得进入 CLI 参数、普通输入、摘要、日志、异常或测试快照；
- 配置更新必须保留未知项和未修改秘密，采用原子替换与可恢复备份；现有配置和数据库不得静默覆盖；
- 不提交真实密钥、token、数据库、本机 `.env` 或本地路径；
- 默认值不得悄悄改变安全、PIT、owner 或执行语义。

## 数据与副作用规则

- 核心指标和策略计算优先保持纯函数，不隐式访问网络、数据库或全局状态；
- 数据加载、网络请求和写库必须位于明确边界层；
- 不原地修改调用方传入的 DataFrame，除非接口明确约定；
- 日期排序、分组键、缺失值、重复行和非有限价格必须显式处理；
- 任何可能影响 PIT 正确性的实现都必须避免未来数据泄漏；
- 数据库字段与 contracts 不一致时，优先修复上游契约或生产链路；
- 兼容 Definition 的结果文件、任务文件和声明式版本写入必须考虑并发、原子性和崩溃恢复；正式 Contract 的终态结构化结果以 runtime SQLite `job_result` 为准；
- owner 过滤应在服务或存储边界强制执行，不能只依赖前端隐藏。

## 测试要求

每项行为变更都应有与其风险匹配的测试。

- contracts：字段、schema、主键、可空性、映射和时间语义；
- orchestration：Definition/DAG 校验、认领、lease、heartbeat、并发与资源冲突、进程内执行、timeout/cancellation、retry、恢复、结果审计和业务反向依赖；
- pipeline：外部映射、标准化、PIT 修订、正式 Contract 生命周期、ExecutionControl 传播、非法输入和入库边界；
- indicators：单/多资产、排序、缺失值、窗口、输出和无未来泄漏；
- strategies：定义、参数、注册表、版本、决策和状态转换；
- backtest engine：成交时点、现实约束、成本、持仓、skipped、收益和回归；
- research：样本隔离、未来结果只评价、walk-forward 和稳健性；
- product/results：任务状态、原子结果、目录、比较、重放和复现；
- auth/users/API：认证模式、状态码、owner 隔离和越权失败；
- 发布级能力：代表性真实产品链和多用户历史读取。

开发过程中可以先运行相关测试；交付前原则上运行：

```bash
python -m pytest
```

如果未运行完整测试，必须在交付说明中明确记录未运行项和原因。纯文档任务经明确授权可以不运行测试。不得通过删除断言、放宽测试或跳过真实失败制造绿灯。

## 变更流程

1. 先判断需求属于数据、事实、决策、执行、研究、产品、结果、身份还是应用编排。
2. 检查目标模块及其下层依赖，确认现有契约、公开接口和时间语义。
3. 在正确模块中实现最小完整变更，避免跨层复制逻辑。
4. 涉及公共结构时，先修改最底层事实来源，再依次更新上层消费者。
5. 涉及任务或运行数据时，确认 owner、锁、原子写入和复现边界。
6. 补充测试并运行与变更范围匹配的检查；未运行项必须如实记录。
7. 交付说明记录修改文件、行为变化、兼容性、测试结果和已知限制。

## 范围控制

- 每次任务只修改与目标直接相关的模块；
- 不借局部问题进行架构级重构；
- 不把无关 pipeline、indicators、strategies、backtest、api、web 或 docs 改动混入同一任务；
- 发现其他层问题时优先报告，只有阻塞当前目标或用户明确授权时才一并修复；
- 不修改数据库文件、原始行情、生成结果或本地环境文件，除非任务明确要求；
- 不把临时实验、mock 或 fixture 直接提升为正式产品实现；
- 不为概念统一重写已经稳定工作的 v1.0 接口。

## 最终判断标准

新代码必须能够清楚回答：

```text
它定义数据、生产数据、计算事实、作出决策、模拟交易、评价结果、编排产品、管理身份，还是暴露应用？
```

无法明确归属通常意味着职责混杂，应先重新划定边界再实现。
