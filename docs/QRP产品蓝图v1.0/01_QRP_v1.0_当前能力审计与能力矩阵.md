# QRP v1.0 当前能力审计与能力矩阵

> 初始审计基线：2026-07-12，策略模块提交 `4159c84 Add QRP strategy module v1` 之后的主分支代码。
> 任务 01 状态更新：2026-07-13，参数化指标、四个经典单标的策略和通用 runtime 准备链路已实现。
> 数据事实：用户已确认本地数据库与 `contracts` 严格一致。

## 一、审计结论

QRP 已经完成了后端核心架构闭环：

```text
contracts → indicators → strategies → backtest runtime → backtest engine
```

当前最成熟的能力是：

- A 股日线行情、估值、市值、涨跌停、停牌、指数、研报与调研数据契约；
- MA5、个股趋势、System B 状态、市场宽度与市场风险指标；
- 版本化策略定义、内置策略、受限声明式策略；
- 固定持有回测和动态 ENTER/HOLD/EXIT 回测；
- 单笔手续费、印花税、滑点、MAE/MFE 和交易级汇总；
- 已有结果 JSON 查询 API 与回测分析页面。

当前最主要的产品缺口不是架构，而是能力密度：

1. 缺横截面指标、因子指标、事件指标和残差指标；
2. 缺财务报表、历史行业归属和标准事件时间轴；
3. 缺组合级资金、目标权重、调仓和资金曲线；
4. 缺 A 股现实成交约束；
5. 缺多因子、事件驱动和代表性相对价值策略；
6. 结果层仍以交易级统计为主，缺组合风险、成本归因和稳健性验证；
7. API/前端只能读取已有回测结果，尚不能完成策略配置与回测任务创建。

## 二、现有模块真实状态

### 2.1 contracts / 数据层

当前数据库契约包含：

- `daily_market_snapshot`
- `daily_basic`
- `stock_info`
- `trading_calendar`
- `adj_factor_changes`
- `index_daily`
- `zt_pool`
- `dt_pool`
- `suspend_d`
- `market_phase`
- `trade_execution`
- `cninfo_research_visits`
- `research_report_stock`
- `research_report_industry`

已覆盖的关键字段包括：OHLCV、涨跌幅、昨收、成交额、换手率、市值、流通市值、PE/PB/PS、股息率、量比、涨跌停、ST、停牌、复权因子、上市退市日期等。

明确缺失：

- 利润表、资产负债表、现金流量表；
- 统一财务指标及其公告可用时间；
- 历史行业分类和历史指数/ETF 成分；
- 业绩预告、业绩快报、分红、回购、增减持、解禁、并购、问询等结构化事件；
- `published_at / effective_at / available_at / revision` 等统一 point-in-time 字段；
- 无风险利率、股指期货、期权、借券与融资成本等相对价值数据。

### 2.2 indicators

已实现：

- `ma5`
- 收盘价相对 MA5 状态；
- 连续站上/跌破 MA5 天数；
- `system_b_trend_valid`
- `system_b_exit_triggered`
- 市场涨跌家数与涨跌停宽度；
- 市场大跌家数和风险等级。
- `IndicatorRequest`、策略参数绑定、稳定 alias 与显式输出字段；
- 统一指标计算注册和参数 schema；
- 参数化 SMA、区间收益、Donchian high/low、rolling mean/std/z-score；
- 所有新增时间序列指标按 ticker 独立并按日期稳定排序；
- Donchian 排除当前 bar，z-score 零标准差输出缺失值。

当前限制：

- 元数据目录与参数化计算注册是两个公开入口，尚未由 API 统一展示；
- 不支持横截面、因子、事件和残差指标族；
- 指标版本尚未进入结果快照。

### 2.3 strategies

已实现：

- `StrategyDefinition / StrategyInput / StrategyDecision / StrategyRunResult`；
- `ENTER / HOLD / EXIT / NO_ACTION`；
- 版本化注册表；
- 参数类型与范围校验；
- `system_b_basic@1.0.0`；
- `time_series_momentum@1.0.0`；
- `dual_sma_trend@1.0.0`；
- `donchian_breakout@1.0.0`；
- `rolling_zscore_mean_reversion@1.0.0`；
- 声明式策略的 `field / indicator / parameter / literal`；
- `eq / ne / gt / gte / lt / lte`；
- `all / any / not`；
- 禁止 `eval` 和任意代码执行。

当前限制：

- long-only、逐标的状态机；
- `score/weight` 尚未连接组合构建；
- 缺策略族、时间框架、适用资产等目录元数据；
- 声明式策略尚不支持横截面排序、调仓、事件窗和多腿关系。

### 2.4 backtest

已实现：

- 原有 `BacktestEngine().run(price_df, signals_df, config)`；
- `signal_close / next_open / next_close`；
- 固定持有 N bar；
- 动态策略运行器；
- 参数化指标请求的通用准备、参数绑定和输出选择；
- ENTER/HOLD/EXIT 到 Trade 的执行适配；
- 手续费、印花税、滑点；
- MAE/MFE；
- 无行情、非法价格、缺未来 bar 等 skipped 记录。

当前限制：

- `PositionRule` 主要用于记录，尚未形成真实组合资金约束；
- `equity_curve` 仍为空；
- 不支持目标权重、组合调仓、现金、复利和同时持仓资金竞争；
- 不支持 T+1、涨停买不到、跌停卖不出、停牌、100 股整数手、最低佣金等 A 股约束；
- 不支持 short、多腿、借券、保证金；
- 不支持 walk-forward、样本内外拆分和参数稳健性验证。

runtime 已移除 MA5/System B 专用准备分支；旧 System B 声明通过 indicators 兼容请求进入统一计算入口。

### 2.5 results / API / frontend

已实现：

- 结果文件 loader/service/schema；
- run、summary、equity、trades、skipped、config 查询 API；
- 回测 run 选择；
- 汇总卡片；
- 净值和回撤图组件；
- 交易、风险、skipped、配置页面。

当前限制：

- API 只读已有结果，不能创建和执行策略回测；
- 策略目录与声明式策略没有 API；
- 前端没有策略编辑、参数配置、股票池和时间区间选择；
- 当前后端策略回测不生成真实组合净值，前端图表依赖结果文件示例；
- 缺 run 对比、滚动分析、成本归因、暴露和稳健性展示。

## 三、模块能力矩阵

| 能力 | 数据/contracts | indicators | strategies | backtest | results/API/frontend | 状态 | v1.0 定位 |
|---|---|---|---|---|---|---|---|
| 日线 OHLCV 与复权 | 已有行情、复权因子 | MA5 已用 | System B 已用 | 可回测 | 可展示交易 | 🟡 | 必须完善复权口径与运行集成 |
| 估值、市值、换手、涨跌停、停牌 | 已有 | 少量使用 | 尚未形成通用策略 | 撮合未使用现实约束 | 未展示 | 🟡 | 必须接入策略和 A 股撮合 |
| 通用时间序列指标 | 基础字段已有 | 参数化 SMA/收益/通道/rolling 统计已完成 | 四个经典策略已用 | 通用动态准备已完成 | API 目录未接入 | ✅ | 第一阶段已完成 |
| 趋势/动量/突破 | 数据足够 | 参数化指标已完成 | 动量/双均线/Donchian 已完成 | 动态回测可用 | 只读结果 | ✅ | 单标的策略能力已完成 |
| long-only 均值回归 | 数据足够 | rolling z-score 已完成 | z-score 均值回归已完成 | 动态回测可用 | 只读结果 | ✅ | 单标的代表策略已完成 |
| 横截面排序与调仓 | 市值和行情部分已有 | 缺 rank/标准化 | 未实现 | 缺组合与目标权重 | 缺组合结果 | ⛔ | v1.0 必须完成 |
| 多因子 long-only | 缺财务报表和历史行业 | 缺因子与中性化 | 未实现 | 缺组合调仓 | 缺归因 | ⛔ | v1.0 必须完成代表实现 |
| 事件驱动 | 研报/调研已有，标准事件缺失 | 缺事件特征 | 未实现 | 缺事件时点规则 | 缺事件分析 | ⛔ | v1.0 必须打通一条链路 |
| 残差/相对价值研究 | 指数已有，行业历史/期货缺 | 缺 beta/残差/半衰期 | 未实现 | 缺多腿/short | 缺价差结果 | ⛔ | v1.0 至少完成研究级残差策略 |
| 完整配对多腿交易 | 数据不足 | 缺 | 缺 | 缺 short、多腿、保证金 | 缺 | ⛔ | 增强项，不阻塞 v1.0 |
| 监督学习研究规范 | 部分特征数据 | 缺通用特征集 | 无模型策略 | 缺时间切分框架 | 缺样本外分析 | ⛔ | 只要求准备边界，不要求复杂内置模型 |
| 强化学习 | 数据/环境不足 | 无 | 无 | 无仿真环境 | 无 | ➡ | v1.0 后 |
| 高频微结构 | 无订单簿数据 | 无 | 无 | 无低延迟撮合 | 无 | ➡ | 明确排除 |
| 组合资金曲线 | 数据足够 | 不适用 | weight 未使用 | equity_curve 为空 | 前端已有组件 | 🟡 | v1.0 必须完成 |
| A 股现实成交约束 | 字段基本已有 | 不适用 | 不应负责 | 尚未实现 | skipped 可扩展 | 🟡 | v1.0 必须完成 |
| 成本与换手分析 | 基础成本已有 | 不适用 | 不应负责 | 简单费率已实现 | 缺成本归因和换手 | 🟡 | v1.0 必须完善 |
| 稳健性与样本外验证 | 交易日数据已有 | 不适用 | 不应负责 | 未实现 | 未实现 | ⛔ | v1.0 必须具备基础 walk-forward |
| 策略 API 与编辑器 | 不适用 | 注册表可支撑 | 声明式底座已有 | 运行器已有 | 尚未接入 | 🟡 | v1.0 必须完成 |
| 结果可复现 | created_at 部分已有 | 指标无版本快照 | 策略有版本 | config 可保存 | 结果读取已有 | 🟡 | v1.0 必须保存完整快照 |

## 四、策略族能力矩阵

| 策略族 | 当前基础 | 缺口 | v1.0 代表策略 |
|---|---|---|---|
| 时间序列趋势/动量 | 参数化指标、三个经典策略和动态 runtime 已完成 | 组合资金和现实成交约束 | `time_series_momentum`、`dual_sma_trend`、`donchian_breakout` |
| 均值回归 | rolling z-score 和 long-only 状态机已完成 | 组合资金、稳健性验证 | `rolling_zscore_mean_reversion` |
| QRP 专属系统 | System B 基础状态与策略 | 判断层、目标池和完整规则尚未系统化 | `system_b_basic` 保留为架构验证策略 |
| 横截面动量 | 行情、市值字段 | 排名、股票池、目标权重、调仓 | `cross_sectional_momentum_long_only` |
| 多因子 | daily_basic 部分估值因子 | 财务数据、历史行业、中性化、组合 | `multifactor_long_only` |
| 事件驱动 | 研报和调研数据 | 标准事件表、时间语义、事件指标 | `event_drift_basic` |
| 相对价值 | 股票和指数日线 | rolling beta、残差、关系稳定性 | `market_residual_mean_reversion` |
| 机器学习 | 暂无统一训练管线 | 时间切分、特征快照、模型版本 | v1.0 不要求内置复杂模型 |
| RL/HFT | 无必要基础 | 数据、环境、基础设施均不足 | 不进入 v1.0 |

## 五、优先级结论

### P0：产品闭环阻塞项

- 真实组合资金曲线；
- 横截面排序、目标权重和调仓；
- A 股成交约束；
- 财务/行业/事件的 point-in-time 数据；
- 策略运行 API 与前端配置；
- 结果快照与复现。

### P1：代表性研究能力

- 趋势/动量/突破/均值回归；
- 横截面动量；
- 多因子 long-only；
- 基础事件驱动；
- 市场残差均值回归；
- walk-forward 与成本归因。

### P2：v1.0 增强但不阻塞

- 完整多腿配对；
- short、借券和保证金；
- 监督学习参考策略；
- 更复杂风险模型。

### 明确延后

- 强化学习；
- 高频做市；
- 订单簿预测；
- 延迟套利；
- 分布式和超高性能回测；
- 实盘自动下单。
