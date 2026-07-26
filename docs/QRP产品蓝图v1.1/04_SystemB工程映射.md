# System B 工程映射

## 1. 映射原则

本文件只做“业务规则 → 工程落位”映射，不重新解释或修改交易规则。

| 业务对象 | 工程层 | 主要输出 |
| --- | --- | --- |
| 前复权、交易日、停牌、新股实际交易日 | contracts / pipeline | 标准行情与可交易性事实 |
| NEW_LISTING_WARMUP | indicators | 个股客观状态 |
| BASE / CANDIDATE / ACTIVE | indicators | `asset_state_observation` |
| 行情轮次 | indicators | `episode` 与统计事实 |
| 涨停、高度、容量、辨识度池 | indicators | 股票池成员和出入事实 |
| 题材关系与证据 | contracts / pipeline | theme membership / evidence |
| 题材指数、M4、M5、M6 输入 | indicators | 题材及市场客观观察 |
| 主线和新增仓授权 | strategies | `strategy_authorization` |
| eligible / veto | strategies | 资格快照和原因码 |
| score / rank / M1—M3 | strategies | 评分、排名与结果身份 |
| 开仓、持有、退出 | strategies | 标准策略决定 |
| 单票和账户仓位约束 | strategies / portfolio | 完整目标组合 |
| T+1、停牌、涨跌停、整数手、成本 | backtest engine | 模拟订单与成交 |
| 每日生产阶段 | backtest product / production | `production_run` 与产物 |
| 订单计划和 OMS | execution | order plan / intent / live state |
| MiniQMT 与券商事件 | qrp-trading | 外部网关和券商事实 |
| 三方对账 | execution | reconciliation difference |

## 2. 基础状态与行情轮次

### 输入

- 前复权日线；
- 个股实际交易日；
- 上市日期；
- 停复牌事实。

### indicators 输出

```text
trade_date
asset_id
state
previous_state
state_transition
above_ma5_streak
below_ma5_streak
warmup_trading_days
calculation_version
input_snapshot_id
observed_at
```

行情轮次至少输出：

```text
episode_id
episode_start_date
episode_end_date
episode_trading_days
ma5_reentry_count
episode_return
max_return
max_drawdown
```

约束：任一股票同一时点最多一个未结束轮次。

## 3. 股票池

每个股票池独立维护：

```text
pool_code
asset_id
entry_date
exit_date
entry_reason
exit_reason
source_observation_ids
calculation_version
```

股票池是发现和筛选路径，不直接产生 M1—M3 身份或开仓授权；其他股票池身份只有被本池规则明确引用时才影响出入。

## 4. 题材与市场结构

题材 canonical、版本、周期、成员关系、产业链关系、证据和提案必须分离。

题材等权指数：

- 剔除上市实际交易日数不超过 5 的股票；
- 剔除观察日停牌股票；
- 对其余有效成员当日涨跌幅取简单算术平均；
- 连续序列复用基础状态机和行情轮次。

M4、M5、M6 的原始指标先作为 observation 保存，不提前合成为总分。

## 5. 授权、资格、评分和身份

顺序必须明确：

```text
市场与题材事实
→ 新增仓授权
→ eligibility
→ hard veto
→ score
→ rank
→ M1 / M2 / M3 result
→ portfolio target
```

M1—M3 不得反向作为自身评分依据。身份变化不中断评分，不触发已有持仓退出。

评分未配置时：

```text
score_status = NOT_CONFIGURED
new_position_authorized = false
reason_code = SCORE_MODEL_NOT_APPROVED
```

不得把空评分当作 0 分后继续下单。

## 6. 组合目标

策略必须输出完整目标组合快照，而不是只输出“买入某票”的增量信号。

每个目标项至少包含：

```text
asset_id
target_weight
target_shares
priority
rank
source_strategy_code
source_strategy_version
reason_codes
rule_version_set_id
parameter_set_id
input_snapshot_id
```

组合计算约束：

- 每次新增仓按总资产 1/8 计算，整手向下取整；
- 单票已有原始仓位加新开仓位不得超过 30%；
- 同一股票最多两次买入；
- 同时持有不同股票不得超过 6 只；
- 已有持仓 1—3 只时，新候选需高于已有持仓最低分；
- 已有持仓 4—5 只时，新候选需高于全部已有持仓；
- 已有持仓 6 只时禁止新增不同股票；
- 评分只决定新增仓准入，不自动替换或卖出已有持仓。

## 7. 退出与严重异动

正式趋势退出：

```text
连续两个实际交易日收盘价低于 MA5
→ target = 0
```

不得恢复已明确舍弃的：

- 固定 -5% 刚性止损；
- 半仓止盈；
- 浮盈 20% 后的 40% 峰值回撤保护。

严重异动先实现计算、预警、监管期资格、审计和人工处置记录；在业务规则明确批准自动执行前，不得自动生成实盘减仓或清仓订单。

## 8. 每日运行时点

收盘后运行至少保存：

```text
business_date
market_data_cutoff
input_snapshot_id
rule_version_set_id
parameter_set_id
calculation_versions
stage_statuses
target_snapshot_id
order_plan_id
```

任一输入不完整、规则版本不明确或评分未批准时，必须显式阻断新增仓，并保留原因。