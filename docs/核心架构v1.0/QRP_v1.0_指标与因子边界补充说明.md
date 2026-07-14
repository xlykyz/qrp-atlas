# QRP v1.0 指标与因子边界补充说明

> **文档状态：v1.0 核心架构规范性补充**  
> **适用版本：QRP v1.0**  
> **性质：显式化既有职责，不新增顶级模块，不改变依赖方向**

## 一、结论

QRP 中的 `factor`（量化因子）是 `indicator`（量化指标）的专业子类型：

```text
Factor ⊂ Indicator
```

因此：

- 不新增顶级 `qrp_atlas/factors` 模块；
- 因子定义、因子计算和可复用因子变换继续位于 `qrp_atlas.indicators`；
- 顶级依赖方向保持不变：

```text
contracts → indicators → strategies
```

本说明只明确已经形成的事实边界，不构成 v1.0 架构解封或重构。

## 二、术语定义

### 2.1 Indicator｜指标

指标是基于标准基础字段或其他指标，通过确定性计算得到的、可复用的客观事实、特征或状态。

典型形式包括：

- 连续数值：收益率、均线、斜率、波动率、回撤；
- 离散状态：是否位于均线上方、趋势是否有效；
- 计数与聚合：涨停次数、题材内趋势股数量；
- 横截面结果：排名、分位数、Z-score、中性化残差。

指标回答：

> 市场、题材或标的已经发生了什么？

### 2.2 Factor｜因子

因子是用于比较、排序、过滤、评分或模型输入的一类指标。因子仍然是客观计算结果，本身不包含交易授权、仓位和买卖动作。

典型因子包括：

- 动量与趋势因子；
- 相对强度因子；
- 流动性因子；
- 波动率与回撤因子；
- 价值、质量和规模因子；
- 涨停与价格结构因子；
- 题材扩散、集中度和持续性因子。

因子可以是原始值，也可以是经过 winsorize、rank、percentile、z-score 或 neutralize 后的标准化结果。

### 2.3 Composite indicator / composite factor｜组合指标或组合因子

固定定义、可被多个策略复用、只描述客观特征的组合结果，仍属于 `indicators`。

例如：

```text
trend_strength
= momentum_rank
+ ma_slope_rank
+ trend_duration_rank
- drawdown_rank
```

如果权重、阈值或组合方式表达某个具体策略的偏好，并直接决定准入、排序或仓位，则该组合属于 `strategies`，而不是通用因子。

## 三、模块职责边界

| 能力 | 归属模块 | 边界说明 |
|---|---|---|
| 标准字段、表结构、字段语义 | `contracts` | 定义数据，不计算因子 |
| 基础指标与状态 | `indicators` | 可复用客观事实 |
| 原始因子生成 | `indicators` | 不访问数据库，不生成交易决策 |
| rank / percentile / z-score / winsorize | `indicators` | 可复用横截面变换 |
| 行业、市值中性化 | `indicators` | 输出客观残差因子 |
| 固定、跨策略复用的组合因子 | `indicators` | 不包含策略授权与仓位偏好 |
| PIT 查询、版本选择、目标日期数据准备 | `pipeline` / backtest runtime | 先准备当时可用数据，再交给 indicators |
| 因子权重、硬门槛、否决规则 | `strategies` | 表达策略偏好与交易授权 |
| 综合评分、Top-N、目标权重 | `strategies` | 形成选择与决策 |
| ENTER / HOLD / EXIT / NO_ACTION | `strategies` | 标准交易决策 |
| forward return、IC、Rank IC、分组收益 | `backtest/research` | 使用未来结果进行事后评价 |
| 成交、成本、持仓、净值和回撤 | `backtest` | 模拟策略结果 |

## 四、严格禁止的越界

### 4.1 indicators 不得承担

- 访问 DuckDB、网络或 pipeline；
- 解析策略专属权重和交易参数；
- 决定某因子具有绝对否决权；
- 执行 Top-N、目标仓位或调仓；
- 输出交易动作；
- 使用未来收益、未来行业或未来暴露；
- 计算 IC、分组未来收益或策略绩效。

### 4.2 strategies 不得承担

- 重复实现已有指标或因子；
- 为方便使用而复制收益率、波动率、排名或中性化算法；
- 查询数据库或自行处理 PIT 财务版本；
- 计算成交成本、资金曲线和绩效评价。

## 五、现有实现映射

当前实现已经符合本说明：

```text
qrp_atlas.indicators.parameterized
    参数化时间序列指标

qrp_atlas.indicators.cross_section.factors
    正式原始因子定义与生成

qrp_atlas.indicators.cross_section.operators
    rank / percentile / winsorize / z-score

qrp_atlas.indicators.cross_section.neutralize
    行业与市值中性化

qrp_atlas.strategies.builtin.cross_section
    因子权重、综合评分、Top-N 和目标选择

qrp_atlas.backtest.research
    forward return、IC、分组收益和暴露评价
```

`FactorDefinition` / `FactorRequest` 与 `IndicatorDefinition` / `IndicatorRequest` 当前允许作为 `indicators` 内部的两套专用接口并存。这是实现层差异，不代表 factors 是独立架构层。

## 六、版本范围

### v1.0 允许

- 在 `indicators` 内扩充基础因子库；
- 增加新的因子族、参数、元数据和计算函数；
- 增加横截面变换和可复用组合因子；
- 在不破坏现有接口的前提下整理子目录；
- 补充因子测试、时间语义和 NaN 语义。

### v1.0 不要求

- 合并 `FactorDefinition` 与 `IndicatorDefinition`；
- 合并 `FactorRequest` 与 `IndicatorRequest`；
- 重写现有策略或横截面研究闭环；
- 引入新的顶级 factors 模块。

### v1.1 可评估

- 统一 indicator / factor 元数据模型；
- 统一依赖解析和 runtime 自动准备；
- 因子依赖 DAG、缓存和增量计算；
- 因子版本、血缘与快照；
- 因子目录 API 和前端研究工作台。

## 七、最终定位

`indicators` 的正式定位为：

> **量化指标、特征与因子计算层。**

其中：

> **指标描述客观事实；因子是面向比较、评分和模型使用的一类指标；策略决定如何使用这些事实。**
