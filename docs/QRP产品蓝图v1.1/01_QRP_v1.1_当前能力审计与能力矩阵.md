# QRP v1.1 当前能力审计与能力矩阵

> 审计日期：2026-07-22。  
> 代码基线：main `a307e58e138677cadcc38a0da2148639b1f99a5c`。  
> v1.1 集成分支：`develop/v1.1`。  
> 审计原则：只把 qrp-atlas main 中已有稳定代码、接口和测试记为仓库已实现；用户确认的独立本地服务单独标记为外部已实现能力。

## 一、审计结论

QRP v1.0 已经提供完整的量化研究与回测产品地基：

```text
pipeline / contracts
→ indicators
→ strategies
→ backtest runtime / portfolio / research / product
→ standard results
→ api / web
```

v1.1 不需要重做数据契约、指标注册、策略模型、组合回测、现实成交约束、结果包、比较或重放。真正缺失的是把用户交易系统业务语义完整接入，并增加每日生产和业务执行编排能力。

总体判断：

- 量化研究地基：✅；
- System B 最基础价格状态：🟡；
- 市场判断与授权：⚪/🟡，存在旧表和基础市场指标，但没有正式决策闭环；
- 动态产业—题材库：⚪，已有完整设计，尚未代码化；
- 核心身份、资格、评分与组合策略：⛔；
- 每日生产运行产品：⛔；
- qrp-atlas 券商无关执行核心：⛔；
- 独立 qrp-trading MiniQMT 网关：✅/🟡，已完成真实 MiniQMT 连接、查询、REST/WebSocket、权限和交易关闭门禁，但尚未完成与 QRP 业务 OMS 的正式协议封板和联调。

## 二、可直接复用的 v1.0 地基

### 2.1 contracts / pipeline / PIT

已有：

- 日线行情、复权、交易日历、股票基础信息；
- daily basic、成交额、换手、市值和估值；
- 涨跌停、停牌、ST 字段；
- 指数行情；
- 财务三表、财务指标；
- 历史行业归属和历史指数成分；
- 业绩预告事件；
- 来源、修订、发布时间、可用交易日和入库时间；
- PIT 历史回补、as_of 查询和审计恢复。

可直接支撑：趋势计算、历史全市场扫描、可交易性过滤、题材成员指标、回测和复盘。

缺口：

- 历史全 A 活跃股票池的统一服务；
- 历史 ST/风险警示的严格版本化确认；
- 严重异常交易监管事实；
- 题材、产业链、角色、规则、授权、目标组合和业务订单的正式契约。

### 2.2 indicators

已有：

- 参数化指标注册与稳定 alias；
- MA、趋势、动量、突破、波动、回撤、量价和流动性指标；
- 横截面 rank、百分位、winsorize、z-score；
- 行业/规模中性化；
- System B 两日站上/两日跌破布尔状态；
- 市场宽度与市场风险基础指标；
- 事件和残差指标。

缺口：

- 与持仓无关、可全市场复算的 System B 完整生命周期；
- 市场节点、阶段和授权所需客观组件；
- 题材强度、扩散、核心持续性、拥挤和风险；
- M1—M6 与 M1—M3 身份所需稳定指标；
- 每项派生事实的 calculation version、coverage 和诊断统一契约。

### 2.3 strategies

已有：

- `StrategyDefinition`、参数 schema、版本化注册表；
- `StrategyDecision` 的 action、reason_code、score、weight 和 evidence；
- Python 内置策略和受限声明式策略；
- `system_b_basic@1.0.0`；
- 横截面 Top-N、目标权重和排序能力。

`system_b_basic@1.0.0` 当前只完成：

```text
未持仓 + 最近两日 close >= MA5 → ENTER
已持仓 + 最近两日 close < MA5 → EXIT
已持仓                         → HOLD
其他                           → NO_ACTION
```

它是架构验证与兼容策略，不是 v1.1 正式生产策略。

缺口：

- 市场授权、题材授权和角色身份；
- eligible 与 rank 分离；
- 硬否决；
- 完整评分；
- 账户级完整目标组合；
- 正式持有和退出优先级；
- 规则版本和决策链解释。

### 2.4 backtest / product / results

已有：

- 共享现金组合；
- 最大持仓数、单票权重、目标权重；
- T+1、涨跌停、停牌、整数手；
- 佣金、最低佣金、印花税和滑点；
- orders、fills、positions、trades、MAE/MFE；
- 净值、回撤、benchmark、excess、暴露和成本；
- 任务状态机、结果持久化、owner 隔离；
- compare、replay 和数据指纹；
- 代表性 walk-forward 和稳健性研究。

缺口：

- 历史全市场 System B 产品路径；
- 完整判断层、题材和角色输入准备；
- 全系统通用参数验证、规则边际贡献和周期归因；
- 生产日批任务、运行水位和决策快照；
- 当前持仓作为正式运行输入；
- 实盘结果与回测结果的统一对照。

### 2.5 API / web

已有：

- 策略目录；
- 回测任务；
- 标准结果查询；
- compare、replay；
- 认证和 owner 隔离；
- 个股复盘、策略配置和结果分析基础页面。

缺口：

- 每日交易驾驶舱；
- 市场授权、主线题材、核心身份、趋势池；
- 资格与否决解释；
- 评分拆解和排名变化；
- 目标仓位与订单计划；
- 业务订单、成交、失败和对账；
- 数据水位、跨服务状态和系统健康。

### 2.6 qrp-atlas execution

qrp-atlas 当前没有正式 `execution` 顶级模块。

仍需建设：

- 券商无关账户、持仓、订单和成交领域模型；
- 订单计划到订单意图；
- 业务风险门；
- `client_order_id` 和幂等；
- 订单状态机；
- 部分成交、撤单、超时和 UNKNOWN；
- 重启恢复；
- 业务订单与券商事实对账；
- paper adapter；
- MiniQMT 网关客户端适配。

### 2.7 独立 qrp-trading MiniQMT 网关

用户已在本地独立仓库 `E:\projects\qrp-trading` 完成并启动 MiniQMT 局域网网关。该能力不属于 qrp-atlas 仓库，但属于 v1.1 的现实外部地基。

用户确认已经实现并真实验证：

- Windows 本机 MiniQMT / `XtQuantTrader.connect()` 成功；
- 账户自动发现与订阅；
- 资产、持仓、当日委托和当日成交查询；
- 最新行情、合约、板块和交易日历；
- 下单、撤单接口，且 `TRADE_ENABLED=false` 时正确拒绝交易；
- `XtQuantTrader`、`xtdata` 和常量发现的受控通用 RPC；
- WebSocket 订单、成交、持仓、连接和行情事件；
- READONLY / TRADE / ADMIN API Key；
- IP 白名单、账户脱敏、SQLite 网关审计、日志和防火墙脚本；
- Linux 客户端通过局域网访问；
- 离线自动化测试 11 passed。

现有网关边界：

- `start`、`stop`、`connect`、`register_callback`、`run_forever` 不允许远程调用；
- MiniQMT 生命周期由网关统一维护；
- 网关负责券商原始事实和本机机械安全，不负责市场判断、System B、组合目标和业务订单政策。

仍需补齐：

- 面向 QRP 生产执行的窄协议封板；
- `request_id/client_order_id` 幂等契约；
- 稳定错误码和事件版本；
- 网关版本/能力协商；
- QRP OMS 与网关审计、券商订单的三方映射；
- 跨服务恢复、连续仿真和受限实盘验收。

## 三、能力矩阵

| 能力 | 数据/契约 | 指标/知识 | 决策/组合 | 回测/验证 | 产品/实盘 | 状态 |
|---|---|---|---|---|---|---|
| 标准行情与 PIT | 已有 | n/a | n/a | 已接入 | 数据预览 | ✅ |
| A 股现实成交 | 已有 | n/a | 目标权重已有 | 完整 | 回测产品 | ✅ |
| System B 两日基础状态 | 已有 | 已有 | 基础策略 | 已回测 | 产品可选 | 🟡 |
| System B 完整生命周期 | 已有 | 缺 | 缺 | 缺 | 缺 | ⛔ |
| 历史全 A 股票池 | 部分 | n/a | 缺统一资格 | 指数池已有 | 缺 | 🟡 |
| 市场判断与授权 | 旧表/基础指标 | 部分 | 缺正式策略 | 缺 | 缺 | 🟡 |
| 动态产业—题材库 | 设计已有 | 设计已有 | 缺 | 缺 | 原型/文档 | ⚪ |
| M1—M3 身份 | 缺正式契约 | 缺 | 缺 | 缺 | 缺 | ⛔ |
| 资格与硬否决 | 部分事实 | 部分 | 缺 | 缺 | 缺 | ⛔ |
| 多因子评分与排名 | 因子数据已有 | 大量可复用 | 通用适配已有 | CS 研究已有 | 缺 System B 产品 | 🟡 |
| 完整目标组合 | 已有模型 | n/a | 通用适配已有 | 已验证 | 缺生产计划 | 🟡 |
| 全策略稳健性 | 数据已有 | n/a | n/a | 代表策略已有 | 缺通用产品 | 🟡 |
| 每日运行驾驶舱 | 数据 API 部分 | 缺聚合 | 缺 | 缺 | 缺 | ⛔ |
| qrp-atlas execution 核心 | 缺正式契约 | n/a | 目标可复用 | paper/仿真缺 | 缺 | ⛔ |
| qrp-trading MiniQMT 网关 | 外部服务契约待封板 | n/a | 不负责 | 真实连接与查询已验证 | 网关已运行，交易默认关闭 | 🟡 |
| 跨服务调度监控与恢复 | pipeline 部分 | n/a | n/a | 部分 | 生产链缺 | 🟡 |

## 四、v1.1 的关键架构决定

1. 核心量化抽象继续保持 `contracts → indicators → strategies`；
2. qrp-atlas 允许新增唯一顶级业务模块 `execution`，承载业务执行编排；
3. 独立 `qrp-trading` 保持 MiniQMT 边缘网关定位，不并入 qrp-atlas；
4. qrp-atlas 不 import `xtquant`，通过版本化 HTTP / WebSocket 协议调用网关；
5. qrp-trading 不实现策略、资格、评分、目标组合和账户风险政策；
6. `system_b_basic@1.0.0` 保持兼容，不原地改写；
7. 新的正式 System B 策略使用新 code/version；
8. 旧 `market_phase`、`trade_execution` 只做兼容读取，不扩展为 v1.1 权威模型；
9. 题材库沿用既有 `docs/动态产业题材库v0.1/` 的 PIT、事实/评分分离和 owner 边界；
10. 回测和实盘消费同一策略决定与目标组合，但使用不同执行实现；
11. 人工不得盘中直接修改策略目标，只允许制度化暂停或全局 kill switch；
12. v1.1 工作包全部合入 `develop/v1.1`，最终通过 `release/v1.1-acceptance → main` 发布。

## 五、审计结论

v1.1 是一次完整产品能力建设，而不是单一策略开发。其工作重点按依赖顺序为：

```text
规则治理
→ 正式契约
→ 全市场事实
→ 判断与题材
→ 核心身份
→ System B 资格/评分/组合
→ 全系统验证
→ 每日产品
→ qrp-atlas execution
→ qrp-trading 协议联调
→ 生产验收
```

任何跳过上游契约和业务定义、直接把策略接到网关下单的实现，都不能视为 v1.1 主线交付。
