# `qrp_atlas` 核心包

`qrp_atlas` 是 QRP 的后端核心包，承载标准数据契约、数据生产、市场事实计算、回测与 API 能力。

完整且权威的产品架构说明见：[QRP v1.0 核心架构文档](../../docs/核心架构v1.0/QRP_v1.0_核心架构文档.md)。本 README 只用于帮助开发者快速定位当前代码和遵守模块边界；若与架构文档或本目录的 [`AGENTS.md`](AGENTS.md) 有冲突，以更具体的规则为准。

## 当前模块

| 模块 | 当前职责 |
| --- | --- |
| `contracts/` | 数据契约的唯一事实来源（SSOT）：标准字段、表结构、映射、市场约定和数据校验。 |
| `pipeline/` | 外部数据的清洗、标准化、契约校验、存储与读取通道。 |
| `indicators/` | 基于行情和 `contracts` 计算可复用的市场、个股与状态事实，不产生交易决策。 |
| `backtest/` | 回测数据准备、策略运行、成交模拟、仓位与绩效计算。 |
| `api/` | 组织后端应用接口，对外暴露已有能力。 |
| `config/` | 路径、运行配置和外部服务客户端配置。 |

## 架构方向

核心业务抽象由低到高：

```text
contracts → indicators → strategies
```

当前实现及调用关系：

```text
外部数据 → pipeline → contracts 校验 / 入库
数据库数据 → indicators / backtest / api
backtest runtime → contracts / indicators / strategies
api → indicators / strategies / backtest
frontend → api
```

`strategies/` 及其声明式策略能力是 v1.0 架构中的规划模块；当前尚未作为本包的顶层目录实现。新增策略功能时应遵循完整架构文档中的统一策略契约，而不是让指标层或回测引擎承担策略定义职责。

## 模块边界

### `contracts`

- 定义数据“是什么”：字段、表、主键、可空性、类型、映射和通用市场约定。
- 不查询数据库、不计算指标、不产生交易决策、不模拟成交。
- 不能依赖 `indicators`、`strategies`、`backtest`、`api` 或前端。

### `pipeline`

- 是数据进入系统的强契约边界。
- 必须复用 `contracts` 中的字段常量、schema、映射、约定和校验函数。
- 数据入库前必须显式完成字段映射、schema 对齐、类型标准化和必要校验。

### `indicators`

- 回答“市场或标的已经发生了什么”。
- 可以依赖 `contracts`，可以计算复合市场事实。
- 不输出 `ENTER`、`HOLD`、`EXIT` 等交易动作，不处理持仓，不写数据库，不依赖策略或回测。

### `backtest`

- 在运行环境中准备数据和指标、执行策略、模拟成交、计算绩效。
- 通用引擎不得内置具体策略知识。

### `api`

- 负责组织和暴露应用能力。
- 不应承载可复用的指标、策略或回测核心规则。

## 新代码放置指南

| 需求 | 应放置的位置 |
| --- | --- |
| 新增标准存储字段、表、主键或数据源映射 | `contracts/`，再更新生产者与消费者。 |
| 从外部源抓取、清洗、标准化或入库 | `pipeline/`。 |
| 基于数据计算 MA、市场宽度、风险状态等客观事实 | `indicators/`。 |
| 定义入场、持有、退出、排序或选股规则 | `strategies/`（规划模块）。 |
| 处理交易成本、成交、仓位、净值、回撤与绩效 | `backtest/`。 |
| HTTP 路由、请求/响应编排 | `api/`。 |

## 开发检查清单

1. 修改 `pipeline/` 前，先阅读相关 `contracts` 定义；不要在 pipeline 中重复定义标准字段、schema 或映射。
2. 新的计算字段先判断它是“事实”还是“决策”：事实进入 `indicators/`，交易规则进入 `strategies/`。
3. 下游模块读取数据库时优先复用 `contracts`；若需要 DTO 或展示字段，应在边界显式转换。
4. 不让下层模块反向依赖上层模块；尤其保持 `contracts → indicators → strategies` 的单向关系。
5. 为改动补充相应测试，并运行与变更范围匹配的测试命令。

## 相关文档

- [核心架构封版文档](../../docs/核心架构v1.0/QRP_v1.0_核心架构文档.md)
- [本目录 Agent 规则](AGENTS.md)
- [仓库根目录开发说明](../../AGENTS.md)
