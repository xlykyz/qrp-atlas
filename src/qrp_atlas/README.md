# `qrp_atlas` 核心包

`qrp_atlas` 是 QRP 的后端核心包，承载标准数据契约、数据生产、指标与策略、研究评价、组合回测、产品运行、认证和 API 能力。

QRP v1.0 后端产品基线已经完成最终验收。完整且权威的架构说明见 [QRP v1.0 核心架构文档](../../docs/核心架构v1.0/QRP_v1.0_核心架构文档.md)，产品终点与验收记录见 [`docs/QRP产品蓝图v1.0/`](../../docs/QRP产品蓝图v1.0/)。本 README 用于帮助开发者快速定位当前代码和模块边界；若与本目录或更下级的 `AGENTS.md` 冲突，以距离目标文件最近的规则为准。

## 当前模块

| 模块 | 当前职责 |
| --- | --- |
| `contracts/` | 数据契约的唯一事实来源（SSOT）：标准字段、表结构、映射、市场约定、PIT 时间语义和数据校验。 |
| `pipeline/` | 外部数据获取、清洗、标准化、契约校验、PIT 历史回补、存储与数据访问。 |
| `indicators/` | 计算可复用的指标、特征、因子与客观状态事实，不产生交易动作。`Factor ⊂ Indicator`。 |
| `strategies/` | 定义稳定版本的内置与声明式交易规则，输出标准化决策，不访问数据库或模拟成交。 |
| `backtest/` | 数据准备、策略运行适配、组合成交模拟、研究评价、产品任务和标准结果。 |
| `backtest/research/` | 因子、事件、残差、暴露和稳健性研究；未来结果只用于评价。 |
| `backtest/product/` | 策略目录、任务状态、真实运行链、运行历史、比较、重放与 owner 隔离。 |
| `backtest/results/` | 标准结果包的原子写入、加载、查询、比较和可复现信息。 |
| `auth/` | 身份认证、密码与会话、Provider、当前用户依赖。 |
| `users/` | 稳定内部用户实体、状态、Repository 与 Service。 |
| `api/` | 组织并暴露认证、复盘、策略、任务、运行和结果等后端能力。 |
| `config/` | 路径、环境变量、外部客户端和运行配置。 |

## 架构方向

核心量化抽象由低到高：

```text
contracts → indicators → strategies
```

完整运行关系：

```text
外部数据 → pipeline → contracts 校验 / 入库
数据库与调用方输入 → backtest runtime / research
backtest runtime → indicators → strategies → execution engine
backtest product → catalog / tasks / runtime / results
api → auth / users / indicators / strategies / backtest / results
frontend → api
```

身份控制面与量化核心保持解耦：

```text
users ← auth
api → auth / users
```

`auth/` 和 `users/` 不进入 `contracts → indicators → strategies` 的量化依赖链。它们为 API 和业务数据提供稳定的当前用户及 owner 归属，不承载指标、策略或回测算法。

## 模块边界

### `contracts`

- 定义数据“是什么”：字段、表、主键、可空性、类型、映射、通用市场约定和时间语义。
- 不查询数据库、不计算指标、不产生交易决策、不模拟成交。
- 不能依赖 `pipeline`、`indicators`、`strategies`、`backtest`、`api` 或前端。

### `pipeline`

- 是外部数据进入系统的强契约边界。
- 必须复用 `contracts` 中的字段常量、schema、映射、约定和校验函数。
- 数据入库前必须显式完成字段映射、schema 对齐、类型标准化和必要校验。
- PIT 数据必须保留发布时间、可用时间、有效时间、修订和来源语义，不得用今天口径覆盖历史。

### `indicators`

- 回答“市场、题材或标的已经发生了什么”。
- 可以依赖 `contracts`，可以计算技术指标、横截面因子、事件特征、残差和复合市场事实。
- 因子属于指标子集，不新增平行顶级 `factors` 模块。
- 不输出交易动作，不处理持仓、成交、成本或未来收益评价，不写数据库，不依赖策略或回测。

更具体的指标与因子边界见 [`indicators/AGENTS.md`](indicators/AGENTS.md)。

### `strategies`

- 回答“面对已经准备好的事实，应当做什么”。
- 定义 required fields / indicators、参数、版本、决策和证据。
- 可以实现时间序列、横截面、事件和残差策略，也可以使用受限声明式结构。
- 不访问数据库、不加载 pipeline、不复制已有指标、不模拟成交、不计算绩效。

### `backtest`

`backtest/` 包含几个相互区分的职责：

- runtime：准备数据和指标、运行策略、将决策转换为执行输入；
- engine：模拟订单、成交、资金、持仓、成本、净值和风险；
- research：使用未来结果评价因子与策略，但不得反向影响历史决策；
- product：组织目录、任务、真实运行、历史、比较和重放；
- results：保存和读取标准结果及可复现快照。

通用执行引擎不得内置任何具体指标、题材或策略知识。产品层不得绕过策略注册表、运行器、标准结果或 owner 隔离。

### `auth` / `users`

- `users` 定义稳定内部用户，不处理密码或会话。
- `auth` 解析身份并返回内部 `UserContext`，可以依赖 `users`，但 `users` 不反向依赖 `auth`。
- PostgreSQL 保存身份控制面；DuckDB 和文件结果保存 QRP 业务数据，通过内部 `user_id` 建立 owner 归属。
- 默认 `local` 模式必须完全跳过 PostgreSQL；显式 `database` 模式失败时禁止自动降级。

### `api`

- 负责组织和暴露应用能力。
- 可以协调认证、用户、指标、策略、任务、回测和结果服务。
- 不应承载可复用指标、策略条件、成交算法或绩效算法。
- 所有按用户归属的任务和运行读取必须执行 owner 隔离，不把前端传入的 owner 当作可信来源。

## 新代码放置指南

| 需求 | 应放置的位置 |
| --- | --- |
| 新增标准字段、表、主键、可用时间或数据源映射 | `contracts/`，再更新生产者、消费者和测试。 |
| 从外部源抓取、清洗、版本化、标准化或入库 | `pipeline/`。 |
| 计算 MA、市场宽度、因子、事件特征、残差等客观事实 | `indicators/`。 |
| 定义入场、持有、退出、排序、选股或目标权重规则 | `strategies/`。 |
| 处理成交、资金、持仓、成本、净值、回撤与绩效 | `backtest/` 的 runtime / engine。 |
| 计算 forward return、IC、事件收益或 walk-forward 评价 | `backtest/research/`。 |
| 组织策略目录、任务、运行、历史、比较或重放 | `backtest/product/` 与 `backtest/results/`。 |
| 定义内部用户、状态和用户资料 | `users/`。 |
| 处理登录、密码、会话和当前用户 | `auth/`。 |
| HTTP 路由、请求/响应 DTO 与应用编排 | `api/`。 |

## 开发检查清单

1. 先判断需求属于数据定义、数据生产、事实计算、交易决策、执行、研究评价、结果、产品编排、身份还是 API。
2. 修改 `pipeline/` 前先阅读相关 `contracts`；不要重复定义标准字段、schema、映射或时间语义。
3. 新计算先判断是“事实”“决策”还是“评价”：事实进入 `indicators`，交易偏好进入 `strategies`，未来结果评价进入 `backtest/research`。
4. 策略只消费已准备输入；通用引擎只执行标准目标或决策；产品层只编排现有公开能力。
5. 所有历史研究和回测都必须遵守 point-in-time 语义，避免未来数据泄漏。
6. 所有用户业务数据读取、比较和重放都必须基于当前用户执行 owner 隔离。
7. 涉及公共结构时先修改最底层事实来源，再依次更新上层消费者。
8. 为行为变更补充与风险匹配的测试；纯文档任务可不运行测试，但必须明确说明。

## 相关文档

- [核心架构封版文档](../../docs/核心架构v1.0/QRP_v1.0_核心架构文档.md)
- [QRP v1.0 产品蓝图与验收](../../docs/QRP产品蓝图v1.0/README.md)
- [本目录 Agent 规则](AGENTS.md)
- [指标与因子 Agent 规则](indicators/AGENTS.md)
- [认证说明](../../docs/用户与认证/README.md)

开发机或工作区根目录可以存在未提交的本地 `AGENTS.md`。该文件由 `.gitignore` 排除，不属于仓库文档入口；存在时仍应与本目录规则同时生效。