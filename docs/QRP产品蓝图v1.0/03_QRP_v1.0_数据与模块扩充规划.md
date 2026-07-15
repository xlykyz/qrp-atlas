# QRP v1.0 数据与模块扩充规划

> 任务 00 状态校准：2026-07-15。财务/行业/指数与 `earnings_forecast_event` 已落地；泛化 `corporate_event` 不再作为 v1.0 唯一验收对象。

## 一、规划原则

本规划严格沿用已封版架构，不新增顶级业务模块：

```text
contracts → indicators → strategies → backtest → results → api → frontend
```

现代量化能力通过扩充现有模块实现：

- 数据表、字段和时间语义：`contracts / pipeline / data`；
- 二次计算特征：`indicators`；
- 算法和参数：`strategies`；
- 数据准备、组合、成交和验证：`backtest`；
- 结果、归因和复现：`backtest/results`；
- 产品接口和交互：`api / web`。

## 二、数据层扩充

### 2.1 现有数据保留并标准化使用

当前已有：

- 日线行情；
- daily basic；
- 复权因子；
- 股票基础信息；
- 交易日历；
- 指数日线；
- 涨跌停池；
- 停牌；
- 研报；
- 机构调研；
- 市场阶段和交易执行记录。

后续重点不是重复建表，而是：

- 明确价格口径；
- 补齐查询服务；
- 统一时间可用性；
- 在回测运行器中按策略声明加载；
- 保存数据版本和运行快照。

### 2.2 财务与基本面数据

建议在 contracts 中补齐：

```text
income_statement
balance_sheet
cashflow_statement
financial_indicator
```

每张表至少包含：

```text
ticker
report_period
announcement_date
published_at
available_trade_date
revision_id
source
source_record_id
ingested_at
```

核心要求：

- 使用公告时间而不是报告期结束日决定可用性；
- 财报修订不能覆盖历史版本；
- 回测默认使用当时最新已公开版本；
- TTM 和同比指标在 indicators 计算，不在 pipeline 随意衍生多套口径。

### 2.3 历史行业与成分数据

建议新增：

```text
industry_membership_history
index_component_history
```

至少包含：

```text
asset_id
classification_system
industry_code
industry_name
effective_from
effective_to
published_at
available_trade_date
source
```

用途：

- 行业中性化；
- 行业暴露；
- 同行业比较；
- 历史股票池；
- 防止用今天的行业归属和指数成分回测过去。

### 2.4 结构化事件数据

#### v1.0 当前标准（已实现）

v1.0 第一类标准事件不是泛化 `corporate_event`，而是：

```text
earnings_forecast_event
+ EventFrame
+ query_earnings_forecast_as_of / to_earnings_forecast_event_frame
```

已覆盖公告日、available_trade_date、revision、source 与 as_of 查询。产品化入口见任务 07-B2。

#### v1.1 可扩展统一事件主表（非 v1.0 阻塞）

后续可建立统一事件事实表：

```text
corporate_event
```

核心字段：

```text
event_id
ticker
event_type
announcement_title
published_at
effective_date
available_trade_date
source
source_record_id
payload_json
revision_id
ingested_at
```

首批事件类型：

- 业绩预告；
- 业绩快报；
- 财报披露；
- 分红；
- 回购；
- 增减持；
- 限售解禁；
- 并购重组；
- 重大合同；
- 监管问询。

专项事件如果未来需要结构化专属字段，可增加专项表，但统一事件主表继续作为检索和回测入口。

### 2.5 数据来源与版本

所有面向研究的数据建议逐步统一以下语义：

| 字段 | 含义 |
|---|---|
| `source` | 数据来源 |
| `source_record_id` | 来源记录唯一标识 |
| `published_at` | 市场首次可见时间 |
| `effective_at/effective_date` | 业务事实生效时间 |
| `available_trade_date` | 在 QRP 回测中最早允许使用的交易日 |
| `ingested_at` | 进入本地库时间 |
| `revision_id` | 同一事实的修订版本 |
| `calculation_version` | 派生数据算法版本 |

`created_at` 不能替代 `published_at` 或 `available_trade_date`。

## 三、indicators 扩充

### 3.1 参数化指标基础

新增统一 `IndicatorRequest` 或等价模型：

```text
code
parameters
alias
version
```

运行器通过注册表寻找计算实现，不再对每个指标硬编码准备路径。

### 3.2 时间序列指标族

第一批：

```text
sma / ema
rolling_return
rolling_volatility
rolling_mean / rolling_std
rolling_zscore
rolling_high / rolling_low
atr
drawdown
volume_ratio
turnover_mean
rolling_beta
residual_return
```

### 3.3 横截面算子

```text
cross_section_rank
percentile_rank
winsorize
zscore
neutralize_by_industry
neutralize_by_size
neutralize_by_beta
```

要求所有横截面操作显式按 `trade_date` 分组。

### 3.4 因子指标族

代表性因子：

- value；
- quality；
- profitability；
- growth；
- investment；
- momentum；
- volatility；
- liquidity；
- size。

因子定义必须记录：

- 原始字段；
- 数据滞后；
- 去极值方式；
- 标准化方式；
- 中性化方式；
- 版本。

### 3.5 事件和相对价值指标

事件：

```text
event_age
event_window_return
announcement_gap
earnings_surprise
post_event_drift
```

相对价值：

```text
rolling_correlation
rolling_beta
spread
residual_return
residual_zscore
half_life
relationship_stability
```

## 四、strategies 扩充

### 4.1 策略目录元数据

在现有 `StrategyDefinition` 上兼容扩展：

```text
family
timeframe
asset_scope
directions
tags
reference_strategy
```

### 4.2 内置策略阶段

第一批单标的：

- time series momentum；
- dual SMA；
- Donchian breakout；
- rolling z-score mean reversion；
- system_b_basic。

第二批横截面：

- cross-sectional momentum long-only；
- multifactor long-only。

第三批信息与相对价值：

- event drift basic；
- market residual mean reversion。

### 4.3 声明式策略

在现有安全模型上逐步扩充：

- 参数化指标请求；
- cross/突破等状态操作；
- 横截面 rank 和 Top N；
- 调仓频率；
- 股票池过滤；
- 目标权重规则；
- 事件条件。

仍禁止：

- `eval`；
- 任意 Python；
- 动态导入；
- 访问文件、网络或数据库。

## 五、backtest 扩充

### 5.1 Runtime

负责统一：

- 读取字段依赖；
- 读取 parameterized indicator requests；
- 数据区间和预热区间；
- point-in-time 过滤；
- 股票池；
- 横截面快照；
- 策略运行；
- 调仓计划。

### 5.2 组合和账户

在现有 backtest 包内增加：

- cash；
- positions；
- orders/fills；
- target weights；
- portfolio snapshots；
- rebalance；
- equity curve。

不要求新增顶级 `portfolio` 模块。

### 5.3 A 股执行约束

至少实现：

- T+1；
- 涨停买入拒绝；
- 跌停卖出拒绝；
- 停牌不可成交；
- 100 股整数手；
- 现金不足；
- 最低佣金；
- 印花税；
- 滑点；
- 可选成交量参与率。

所有拒绝必须进入 `Skipped` 或统一执行诊断。

### 5.4 验证能力

- benchmark；
- train/validation/test；
- walk-forward；
- warm-up；
- 参数敏感性结果；
- 交易成本压力测试；
- 数据缺失和幸存者偏差审计。

## 六、results / API / frontend 扩充

### 6.1 结果

新增：

- 真实组合净值和回撤；
- 日收益；
- 持仓快照；
- 订单与成交；
- 换手；
- 成本拆分；
- 暴露；
- benchmark；
- 滚动指标；
- 样本内外标签；
- 完整快照。

### 6.2 API

新增：

- `/api/indicators`；
- `/api/strategies`；
- 声明式策略校验；
- 创建回测 run；
- 查询任务状态；
- run 比较；
- 参数/配置 schema。

### 6.3 前端

新增：

- 策略目录；
- 参数表单；
- 声明式规则编辑器；
- 股票池和日期配置；
- 成本与执行配置；
- 回测任务状态；
- run 对比；
- 换手、成本、暴露和滚动表现。

## 七、禁止的架构漂移

- 不为因子单独建立顶级 `factors/`；因子属于 indicators；
- 不为事件建立顶级业务模块；数据进入 contracts/pipeline，特征进入 indicators，算法进入 strategies；
- 不为组合建立与 backtest 平级的新核心模块；组合执行先属于 backtest；
- 不把数据库查询写进策略；
- 不把具体策略写进回测引擎；
- 不因 ML/RL/HFT 研究存在而提前建设不需要的基础设施。
