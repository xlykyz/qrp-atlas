# 任务 07：System B 资格、评分与组合策略

## 一、目标

实现 v1.1 正式 System B 生产策略：把市场授权、题材周期、核心身份、趋势状态、可交易性和风险事实组合为资格、硬否决、评分、排名、持仓决策和完整账户目标组合。

本任务完成后，系统应能回答：

> 明日是否允许交易、哪些股票可以交易、为什么、优先级如何、账户应持有什么、每只持仓应占多少。

## 二、前置

- 任务 00—06 全部验收；
- 规则、参数、市场授权、题材、角色和状态契约稳定；
- 所有争议退出和仓位规则已在任务 00 裁决。

## 三、正式策略版本

新增正式 code/version，例如：

```text
system_b_production@1.0.0
```

最终名称在任务 00 确认。

要求：

- 不原地修改 `system_b_basic@1.0.0`；
- 注册表、目录 API 和结果快照可区分两者；
- 正式策略声明全部字段、指标、规则和参数依赖；
- 相同输入、规则和参数产生相同结果。

## 四、资格链

资格必须按层计算：

```text
market_authorized
AND theme_authorized
AND role_eligible
AND system_b_state_eligible
AND tradability_eligible
AND risk_eligible
= eligible
```

每层输出：

```text
passed
reason_codes
coverage
evidence
```

最终资格状态至少支持：

```text
ELIGIBLE
INELIGIBLE
VETOED
UNAVAILABLE
```

### 4.1 市场授权

- 使用任务 04 已冻结的下一交易日授权；
- 不授权时禁止新开仓；
- 已有持仓继续走正式持有/退出规则；
- 数据 unavailable 时按规则基线 fail closed。

### 4.2 题材授权

按任务 00 定义使用：

- 题材 cycle 状态；
- 活跃、扩散、核心和风险；
- 是否属于当前主线；
- 数据 coverage；
- 多主线并存规则。

### 4.3 角色资格

- 只使用任务 06 的 M1/M2/M3 观察；
- 明确哪些角色可独立获得资格、哪些需组合；
- 多角色不得重复计入资产；
- 角色失效和题材 cycle 结束后不能继续授权新仓。

### 4.4 趋势资格

- 使用任务 03 的完整 System B 状态；
- 入场资格、持有资格和退出触发分离；
- WARNING 等中间状态是否允许新增仓位由规则版本决定；
- 不能仅用当前两日布尔字段覆盖完整历史状态。

### 4.5 可交易性和风险

按任务 00 批准规则处理：

- ST/风险警示；
- 停牌；
- 上市天数；
- 流动性；
- 数据完整性；
- 涨跌停计划限制；
- 严重异常交易监管；
- 其他账户外硬风险。

硬否决不能被任何分数覆盖。

## 五、评分

仅对满足评分前置条件的标的计算。建议维度：

```text
trend_state_score
trend_strength_score
relative_strength_score
theme_strength_score
role_score
liquidity_score
volume_price_score
risk_penalty
```

最终维度、权重和惩罚以任务 00 参数集为准。

要求：

- 每一维有方向和范围；
- 返回 raw、normalized、weight、contribution；
- 返回 coverage；
- 缺失不自动按 0；
- 风险硬否决与风险软惩罚分离；
- 评分不使用账户是否持有、当前可用资金或最大持仓数；
- 评分和排名版本化；
- 未来收益不进入评分。

## 六、排名和容量

排名顺序必须确定性：

1. 已批准角色/优先级；
2. total score；
3. 正式 tie-breaker；
4. asset_id 仅作为最终稳定排序，不表达经济偏好。

容量选择：

- `max_positions`；
- 新增仓位数量；
- 单题材数量/集中度；
- 已有持仓占用；
- 现金缓冲；
- 未入选原因 `CAPACITY_NOT_SELECTED`；
- 不把未入选者改成 ineligible。

## 七、持仓决策

对每个标的每日输出：

```text
ENTER
HOLD
EXIT
NO_ACTION
```

并区分：

- 未持仓候选；
- 已持仓且状态继续；
- 已持仓首次预警；
- 已持仓正式退出；
- 资格撤销但未触发退出；
- 监管/账户硬风险退出；
- 目标组合容量替换；
- 数据不可用。

退出优先级严格使用任务 00 结果，不在本任务临时创造止损或保护规则。

## 八、完整目标组合

输出全量目标快照：

```text
target_date
asset_id
target_weight
priority
rank
source_action
reason_code
evidence
```

要求：

- 包含目标为 0 的退出资产；
- 权重和不超过允许总暴露；
- 单票和题材集中度生效；
- 已有持仓和新候选在同一目标组合中处理；
- 同日退出先释放容量，再按规则分配新仓；
- 目标快照不可依赖订单实际成交结果反向改写；
- 无合格标的时合法输出现金目标；
- 决策和目标保留规则、参数和输入快照。

## 九、公共目标规划能力

将纯 `StrategyDecision → PortfolioTarget` 逻辑放在 backtest 和 trading 可共同使用的位置。

要求：

- 不让 trading 依赖历史回测引擎；
- 不复制两套排序/权重实现；
- 原 `backtest.portfolio.strategy` 公共入口保持兼容或提供清晰迁移；
- 目标规划函数无数据库、无券商、无未来结果。

## 十、产品运行器

产品路径必须支持：

- 历史全 A；
- PIT 市场、题材、角色和状态输入；
- 正式策略运行；
- 完整目标组合；
- A 股回测执行；
- 标准结果、诊断和 replay；
- 每日生产模式生成次日目标但不下单（任务 09 接管）。

## 十一、结果和解释

标准结果至少新增：

```text
all_candidates
eligibility
vetoes
score_components
ranks
capacity_selection
portfolio_targets
rule_versions
parameter_set
```

必须能解释：

- 为什么某股不合格；
- 为什么合格但未入选；
- 为什么持有；
- 为什么退出；
- 为什么权重是该值；
- 哪个规则具有最终决定权。

## 十二、测试

覆盖：

- 所有资格层；
- 每类硬否决；
- unavailable 和缺失；
- 分数分解；
- 排名并列；
- 空池子；
- 容量不足；
- 多题材集中度；
- 已有持仓与新候选；
- 授权撤销但持仓继续；
- 全部批准的退出路径；
- 同日退出/入场；
- 不可成交后的目标不被篡改；
- 多标的确定性；
- PIT；
- 版本快照；
- 与 `system_b_basic` 兼容和对照；
- 端到端产品回测和 replay。

## 十三、禁止范围

- 不在本任务修改题材 canonical；
- 不重新定义市场阶段或角色；
- 不用总分覆盖否决；
- 不以近期回测选择规则结构；
- 不直接调用 QMT；
- 不处理订单、成交和券商状态；
- 不允许盘中人工临时修改目标；
- 不把实际未成交当作策略分数反馈。

## 十四、验收

- 完整 System B 从业务事实到目标组合闭环；
- eligibility、score、rank、capacity 和 target 明确分层；
- 所有规则可解释且版本化；
- 旧策略兼容；
- 回测和未来 live trading 可消费同一目标；
- 专项、全量、PIT、产品和 replay 测试通过；
- PR 等待独立验收。