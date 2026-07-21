# QRP v1.1 数据与模块扩充规划

> 状态：范围基线草案。  
> 原则：完整能力先定义，按模块和任务包逐步交付；实施过程中不得通过“先做最小版”改变最终目标。

## 一、架构总览

v1.1 保留 v1.0 的核心量化抽象：

```text
contracts → indicators → strategies
```

形成两个执行环境：

```text
                         ┌→ backtest runtime / engine → results
contracts → indicators → strategies
                         └→ trading runtime → broker adapter → QMT
```

完整产品依赖：

```text
pipeline → contracts
indicators → contracts
strategies → indicators / contracts
backtest → contracts / indicators / strategies / engine / results
trading → contracts / strategies / trading domain / broker adapter
api → auth / users / research product / trading product
web → api
```

禁止：

- `contracts` 依赖上层；
- `indicators` 输出交易动作；
- `strategies` 查询数据库、调用 backtest 或 QMT；
- `backtest engine` 包含题材、System B 或券商知识；
- `trading` 重新实现策略和指标；
- `qmt` 适配器把 SDK 类型传播到核心领域模型；
- 前端直接访问 DuckDB、订单目录或 xtquant。

## 二、contracts 扩充

### 2.1 规则与参数治理

建议正式模型：

```text
rule_definition
rule_version
parameter_definition
parameter_set
change_request
approval_record
```

核心字段：

```text
rule_id / rule_code / rule_name
system_layer
source_document / source_locator
status
priority
version
valid_from / valid_to
approved_by / approved_at
supersedes_version
logic_json / description
```

要求：

- 规则版本不可原地覆盖；
- 参数值与规则结构分离；
- 每次生产决策绑定 `rule_version_set_id` 和 `parameter_set_id`；
- `LEGACY` 和 `REJECTED` 规则可查询但不得进入生产运行。

### 2.2 市场、题材、角色和状态

建议正式模型：

```text
market_state_observation
market_authorization
theme / theme_version / theme_cycle
theme_membership
theme_metric_observation
theme_lifecycle_transition
member_role_observation
asset_state_observation
```

必须保留：

- 业务有效时间；
- 市场可用时间；
- QRP 观察和入库时间；
- 修订；
- calculation version；
- coverage；
- source / evidence；
- owner 边界；
- cycle id。

旧 `market_phase` 只保留兼容，不再扩展为权威市场授权模型。

### 2.3 资格、评分和决策

建议正式模型：

```text
eligibility_snapshot
eligibility_reason
score_snapshot
score_component
rank_snapshot
strategy_decision
portfolio_target
portfolio_target_snapshot
```

必须区分：

- 事实；
- 硬资格；
- 硬否决；
- 分数；
- 排名；
- 容量选择；
- 目标权重；
- 最终策略动作。

不得只保存最终入选名单而丢失被淘汰标的和原因。

### 2.4 生产运行与实盘

建议正式模型：

```text
production_run
run_stage
run_input_snapshot
order_plan
order_plan_item
live_account_snapshot
live_position_snapshot
live_order
live_order_event
live_fill
reconciliation_run
reconciliation_difference
manual_intervention
trading_control_state
```

关键约束：

- `order_plan` 在发送前冻结；
- 每个订单具有稳定 `client_order_id` / idempotency key；
- 订单状态通过事件追加或等价可审计方式变化；
- 券商订单 ID 与 QRP 订单 ID 分离；
- 人工操作必须记录来源、原因和时间；
- 对账差异不可被静默覆盖。

旧 `trade_execution` 只保留历史兼容，不作为 v1.1 OMS。

## 三、pipeline / data 扩充

### 3.1 历史全 A 股票池

正式支持：

- 按交易日生成当时已上市且未退市的全 A 股票池；
- 板块、上市天数和证券状态；
- 历史 ST/风险警示；
- 停牌和长期停牌；
- 数据覆盖和缺口；
- 明确排除 ETF、指数、B 股等非目标资产；
- 历史查询不得使用当前状态回看过去。

### 3.2 可交易性事实

生产：

```text
is_listed
is_st
is_suspended
limit_status
has_valid_price
is_liquid
is_recent_listing
is_severe_abnormal
is_data_complete
```

这些是客观事实；是否构成资格或否决由策略版本决定。

### 3.3 市场和指数输入

补齐：

- 上证、深证、创业板、科创 50 等主要指数；
- 节点窗口输入；
- 全市场涨跌分布；
- 跌停、超 5%/10% 下跌和高波动分布；
- 涨停、连板、炸板、成交额和高度；
- 指数与市场数据水位。

### 3.4 产业—题材数据

沿用 `docs/动态产业题材库v0.1/`：

```text
L0 source snapshot
L1 canonical fact
L2 curated knowledge
L3 derived signal
L4 serving view
owner-private workspace
```

pipeline 只负责来源快照、标准事实、版本和审计，不计算题材分数或直接写策略结论。

### 3.5 生产数据水位

每个数据源和派生任务必须输出：

```text
business_date
observed_until
expected_rows
actual_rows
coverage
status
error_code
source_version
schema_version
```

上游失败时，下游不得把“没有数据”解释为“没有信号”。

## 四、indicators 扩充

### 4.1 System B 状态族

在兼容字段之外，正式输出：

```text
system_b_state
system_b_active
system_b_entry_confirmed
system_b_first_break
system_b_recovered
system_b_exit_confirmed
system_b_restart_confirmed
state_started_at
state_age
above_ma5_streak
below_ma5_streak
```

状态必须：

- 与账户是否持仓无关；
- 对全市场逐日可复算；
- 多标的隔离；
- 乱序输入确定性排序；
- 等于 MA5 的语义明确；
- 缺失值和停牌行为明确；
- 保留事件与状态两个维度。

### 4.2 个股因子族

复用并统一：

- 趋势斜率和 R²；
- 价格效率；
- 中短期动量；
- 相对市场、行业和题材收益；
- 波动、下行波动、回撤；
- 成交额、换手、量比和流动性；
- 涨停、连板和炸板；
- 拥挤和异常波动；
- 可交易性 coverage。

### 4.3 市场状态指标族

输出判断层所需客观组件：

- 指数节点；
- 市场宽度；
- 高度和容量核心持续性；
- 赚钱效应扩散；
- 跌停和大跌风险；
- 主线集中度；
- 退潮持续性；
- 数据覆盖和置信度。

指标不直接输出 A/B/C 阶段；阶段由策略/判断政策产生。

### 4.4 题材指标族

至少包括：

```text
catalyst_score
attention_score
strength_score
breadth_score
leadership_score
capital_score
evidence_score
novelty_score
crowding_score
risk_score
theme_activity_score
```

所有综合分数：

- 返回 components、coverage 和 calculation version；
- 缺失不按 0；
- 风险与活跃并列，不简单相减；
- 绑定 `theme_cycle`；
- 规模偏差和供应商黑盒权重受控。

### 4.5 角色指标族

为 M1—M3 输出客观组成：

- 成交容量；
- 空间高度；
- 领涨贡献；
- 题材共振；
- 排名稳定；
- 人气和辨识度；
- 承压能力；
- 业务相关度；
- 反证和风险。

角色最终判定属于策略/角色政策，不属于指标层。

## 五、strategies 扩充

### 5.1 市场判断策略

新增版本化判断策略，输入市场和题材事实，输出：

```text
phase A / B / C
authorized strategy codes
new position permission
veto reasons
confidence
evidence
```

### 5.2 角色策略

对每个 `theme_cycle × asset` 输出：

```text
role_code
eligible
role_score
confidence
reason_code
evidence
```

### 5.3 正式 System B 策略

新 code/version，不改写 `system_b_basic@1.0.0`。

职责：

- 合并市场授权、题材授权、角色、趋势和风险；
- 先资格后评分；
- 输出全候选结果；
- 生成完整组合目标；
- 处理既有持仓；
- 输出入场、持有、退出和未行动原因；
- 绑定规则和参数版本。

### 5.4 组合目标规划

纯决策逻辑应放在可被 backtest 和 trading 共同消费的位置。不得让 live trading 依赖历史回测引擎，也不得复制目标权重算法。

建议将通用“StrategyDecision → PortfolioTarget”纯适配能力提升到 `strategies` 内部公共组件，保留原 backtest 导出兼容层。

## 六、backtest / research 扩充

### 6.1 完整输入准备

runtime 负责：

- 历史全 A 股票池；
- PIT 市场授权；
- PIT 题材和角色；
- System B 状态；
- 资格、评分和完整组合；
- 预热；
- 正式区间隔离；
- 数据缺失和 coverage 诊断。

### 6.2 全系统验证

扩展为通用：

- walk-forward；
- 参数网格；
- 参数选择隔离；
- 成本压力；
- 容量压力；
- 子周期和滚动表现；
- 规则移除/加入对照；
- 合法与非法错过；
- 题材、角色、阶段和规则归因；
- 旧策略对照；
- 生产配置 replay。

### 6.3 结果扩充

标准结果新增：

```text
market_state
strategy_authorization
themes
roles
eligibility
scores
ranks
portfolio_targets
rule_versions
parameter_set
missed_opportunities
rule_attribution
```

## 七、trading 顶级模块

### 7.1 核心子模块

```text
trading/
├── models
├── planning
├── risk
├── oms
├── broker
├── qmt
├── reconciliation
├── recovery
├── store
└── service
```

### 7.2 BrokerProtocol

核心协议至少支持：

```text
connect / disconnect
query_account
query_positions
query_orders
query_fills
submit_order
cancel_order
subscribe_events
health
```

核心层只认识标准领域对象，不认识 xtquant SDK 对象。

### 7.3 OMS 状态机

至少覆盖：

```text
PLANNED
VALIDATED
BLOCKED
SUBMITTING
SUBMITTED
PARTIALLY_FILLED
FILLED
CANCEL_PENDING
CANCELLED
REJECTED
EXPIRED
UNKNOWN
```

所有迁移必须可审计，未知状态不得自动当作失败重下。

### 7.4 风险门

至少包括：

- 交易日和时段；
- 账户身份；
- 数据与计划水位；
- 计划冻结状态；
- 目标与当前持仓差异；
- 资金、仓位和集中度；
- 价格和数量合法性；
- 重复订单；
- 当日风险限制；
- 系统暂停和 kill switch。

### 7.5 对账与恢复

- 券商事实优先确认订单和成交；
- QRP 保存目标、计划和业务原因；
- 重启后先查询券商，再恢复本地状态；
- 外部人工成交必须识别；
- 差异必须进入人工处理队列；
- 不允许通过覆盖本地持仓“自动消除”差异。

## 八、API / web 扩充

API 分为：

- 研究和回测；
- 每日生产运行；
- 只读决策解释；
- 订单计划；
- 实盘状态；
- 风险控制；
- 对账；
- 系统健康。

写操作必须受控：

- 运行生产任务；
- 冻结或取消计划；
- 启用/暂停交易；
- 触发 kill switch；
- 执行受限人工对账处置。

前端不得提供随意修改已冻结目标、盘中临时选股或绕过风险门的入口。

## 九、配置与部署

配置分层：

```text
code defaults
rule version
parameter set
environment config
account config
run snapshot
```

敏感信息：

- 不进入 Git；
- 不写结果快照明文；
- 账户标识最小化展示；
- API 和远程访问必须认证；
- 不直接将交易服务端口暴露公网。

## 十、迁移原则

1. 先新增契约和兼容读路径，再迁移消费者；
2. 已发布 v1.0 策略和结果不可原地破坏；
3. 旧 `market_phase`、`trade_execution` 和 `system_b_basic` 保持兼容；
4. 新生产事实使用新表/新模型，不继续堆叠旧字段；
5. 任何移动公共逻辑必须保留稳定导出或明确版本迁移；
6. 每个任务完成后更新本蓝图的能力矩阵和验收状态。