# `src/qrp_atlas` Agent Rules

本文件适用于 `src/qrp_atlas/` 及其全部子目录。

QRP v1.0 的权威架构说明见 [`docs/核心架构v1.0/QRP_v1.0_核心架构文档.md`](../../docs/核心架构v1.0/QRP_v1.0_核心架构文档.md)。本文件用于约束代码实现；后续功能可以扩充，但不得破坏已经封版的模块职责和依赖方向。

## 核心原则

QRP 的核心业务抽象由低到高为：

```text
contracts → indicators → strategies
```

各层定位：

```text
contracts   定义数据
pipeline    生产和读取标准数据
indicators  计算客观事实
strategies  输出交易决策
backtest    准备输入并模拟决策结果
results     保存和读取运行结果
api         编排并暴露应用能力
```

必须始终保持：

- 数据定义与数据生产分离；
- 客观事实与交易决策分离；
- 策略定义与策略运行分离；
- 策略决策与成交模拟分离；
- 核心业务逻辑与 API/前端分离；
- 下层模块不得反向依赖上层模块。

## 允许的依赖方向

```text
pipeline → contracts
indicators → contracts
strategies → indicators / contracts
backtest runtime → contracts / indicators / strategies / backtest engine
backtest engine → backtest models
results → backtest result models
api → indicators / strategies / backtest / results
frontend → api
```

禁止的典型依赖：

```text
contracts → indicators / strategies / backtest / api
indicators → strategies / backtest / api
strategies → backtest / api / frontend
backtest engine → 具体指标或具体策略
api → frontend
frontend → DuckDB 或 Python 策略对象
```

依赖方向描述的是模块职责，不要求为了形式统一而制造无意义的 import。

## `contracts/`｜基础契约层

`contracts/` 是持久化数据结构和标准数据语言的唯一事实来源（SSOT），负责：

- 标准字段名；
- DuckDB 表结构；
- 类型、主键和可空性；
- 外部数据源字段映射；
- ticker、日期、交易所、板块和涨跌停等通用约定；
- DataFrame 的 schema 对齐、类型标准化与校验；
- 对外公开的基础常量和契约对象。

约束：

- 不查询数据库；
- 不计算指标；
- 不生成交易决策；
- 不模拟成交；
- 不依赖任何业务上层模块。

新增或修改持久化字段、表名、主键、可空性、字段语义或数据源映射时，先更新 contracts，再更新生产者、消费者和测试。

不得为兼容错误实现而复制、绕过或静默放宽 contracts。

## `pipeline/`｜数据生产与访问层

`pipeline/` 是外部数据进入系统的强契约边界，负责数据获取、清洗、映射、标准化、校验和入库。

正式入库流程原则上为：

```text
外部字段映射
→ schema 对齐
→ 类型标准化
→ 契约校验
→ insert / upsert DuckDB
```

约束：

- 标准字段名、表名、列清单和主键必须复用 `qrp_atlas.contracts`；
- 外部原始列名必须通过 `contracts/mappings.py` 显式映射；
- 通用市场规则必须复用 `contracts/conventions.py`；
- DataFrame 入库前使用 `align_to_schema`、`canonicalize`、`validate_schema`、`quick_validate` 等契约能力；
- 缺列、额外列、非法类型和兼容策略必须显式处理，不得静默吞掉；
- pipeline 不计算策略指标，不生成交易动作，不承载回测逻辑。

如果 contracts 尚未定义所需结构，应先补充 contracts 及其公开导出，再实现 pipeline。

## `indicators/`｜复合指标计算层

`indicators/` 基于标准基础字段计算可复用的客观指标和状态事实，例如 MA、趋势、市场宽度、风险状态和 System B 基础状态。

指标回答：

> 市场或标的已经发生了什么？

约束：

- 核心计算函数优先接收 DataFrame 或明确的结构化输入；
- 不主动查询数据库，不负责数据加载；
- 可以组合其他底层指标，但不得重复实现已有指标；
- 输出应稳定、可测试，并明确字段语义；
- 不输出 `ENTER`、`HOLD`、`EXIT`、`NO_ACTION` 等交易动作；
- 不处理持仓、仓位、成交、成本或收益；
- 不写数据库；
- 不依赖 `strategies`、`backtest` 或 `api`。

一个计算值只要是可复用的客观事实，就应优先进入 indicators，而不是散落在策略、回测或 API 中。

## `strategies/`｜策略定义与决策层

`strategies/` 定义具有明确输入、参数、算法、输出和稳定版本的交易规则单元。

策略回答：

> 面对这些基础数据和指标，应当做什么？

策略可以：

- 声明 `required_fields`；
- 声明 `required_indicators`；
- 定义参数及其类型、默认值和范围；
- 输出标准化的 `ENTER / HOLD / EXIT / NO_ACTION` 决策；
- 通过版本化注册表发布 Python 内置策略；
- 通过受限声明式结构支持前端自定义策略。

约束：

- 策略只消费已经准备好的输入，不自行访问数据库；
- 不主动调用 pipeline 加载数据；
- 不重新计算或复制 indicators 中已有指标；
- 不模拟成交，不计算手续费、滑点、收益、净值或回撤；
- 不依赖 `backtest`、`api` 或前端；
- 策略 code 与 version 必须稳定，行为变化应按兼容性决定是否升级版本；
- Python 内置策略和声明式策略必须遵守统一定义、校验和决策输出模型；
- 声明式策略只能使用受控数据结构和安全求值器，禁止 `eval`、`exec` 或任意代码执行。

策略实现应保持确定性；相同输入、参数和版本应产生可复现的决策结果。

## `backtest/`｜运行与交易模拟层

`backtest/` 在职责上分为 runtime、通用 engine 和 results。

### Backtest Runtime

负责：

- 读取策略定义；
- 解析所需基础字段和指标；
- 准备并校验策略输入；
- 调用 indicators；
- 运行 strategies；
- 将标准决策交给执行引擎。

runtime 可以依赖 contracts、indicators、strategies 和通用 backtest engine，但不得把具体策略知识写进 engine。

### Backtest Engine

负责：

- 入场和退出 bar；
- 成交价格；
- 持仓与资金约束；
- 手续费、印花税和滑点；
- 交易记录；
- MAE/MFE；
- 收益、净值、回撤和绩效指标。

通用 engine 禁止包含五日线、涨停、节点、题材、System A、System B 或任何具体策略概念。

### Results

负责结果加载、保存、查询和可复现信息，不承担策略算法或成交算法。

结果应尽量保留策略版本、参数、数据区间、执行配置、成本配置和必要快照。

## `api/`｜应用编排层

API 负责将已有后端能力组织为稳定接口，可以协调多个模块，但不得成为业务算法的存放位置。

约束：

- 不在路由中实现可复用指标；
- 不在路由中实现策略条件或状态机；
- 不在路由中实现成交、成本和绩效算法；
- 请求/响应 DTO 可以使用自己的展示字段，但数据库查询必须尊重真实 schema；
- API 普通查询不要求机械套用 DataFrame contracts 校验流程；
- 涉及新增持久化记录时，仍必须遵守相应数据契约；
- 安全、只读、临时访问等网关能力不得削弱正式 API 的边界和权限模型。

## `config/`｜配置层

配置模块只负责路径、环境变量、外部客户端和运行参数的集中管理。

- 不在业务模块中散落绝对路径、密钥或环境判断；
- 不提交真实密钥、token、数据库或本地路径；
- 测试应能够通过 fixture 或依赖注入覆盖配置。

## 数据与副作用规则

- 核心指标和策略计算优先保持纯函数，不隐式访问网络、数据库或全局状态；
- 数据加载、网络请求和写库必须位于明确的边界层；
- 不原地修改调用方传入的 DataFrame，除非接口文档明确约定；
- 日期排序、分组键、缺失值、重复行和非有限价格必须显式处理；
- 任何可能影响 point-in-time 正确性的实现都必须避免未来数据泄漏；
- 数据库字段与 contracts 不一致时，优先修复上游契约或生产链路，不在下游静默适配错误结构。

## 测试要求

每项行为变更都应有与其风险匹配的测试。

- contracts 变更：测试字段、schema、主键、可空性、映射和校验；
- pipeline 变更：测试外部映射、标准化、非法输入和入库边界；
- indicators 变更：测试单标的、多标的、排序、缺失值、边界窗口和输出语义；
- strategies 变更：测试定义校验、参数校验、注册表、版本、决策与状态转换；
- backtest 变更：测试成交时点、价格异常、成本、持仓、skipped、收益和回归场景；
- API 变更：测试状态码、响应结构、错误边界和实际调用链。

开发过程中可以先运行相关测试；交付前原则上运行：

```bash
python -m pytest
```

如果未运行完整测试，必须在交付说明中明确记录未运行项和原因。不得通过删除断言、放宽测试或跳过真实失败来制造绿灯。

## 变更流程

1. 先判断需求属于数据、事实、决策、执行、结果还是应用编排。
2. 检查目标模块及其下层依赖，确认现有契约和公开接口。
3. 在正确模块中实现最小完整变更，避免跨层复制逻辑。
4. 涉及公共结构时，先修改最底层事实来源，再依次更新上层消费者。
5. 补充测试并运行与变更范围匹配的检查。
6. 交付说明记录修改文件、行为变化、兼容性、测试结果和已知限制。

## 范围控制

- 每次任务只修改与目标直接相关的模块；
- 不借修复局部问题进行架构级重构；
- 不把无关的 `pipeline`、`indicators`、`strategies`、`backtest`、`api`、`web`、`docs` 改动混入同一任务；
- 发现其它层问题时优先报告，只有在它阻塞当前目标或用户已明确授权时才一并修复；
- 不修改数据库文件、原始行情、生成结果或本地环境文件，除非任务明确要求；
- 不把临时实验代码直接提升为核心模块实现。

## 最终判断标准

新代码必须能够清楚回答：

```text
它定义的是数据、生产数据、计算事实、作出决策、模拟交易，还是编排应用？
```

无法明确归属通常意味着职责混杂，应先重新划定边界再实现。