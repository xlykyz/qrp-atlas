# System B 2.0 事实派生基础状态模型

> 本文取代本工作包此前的递推状态机说明。旧的“前一状态驱动后一状态”和“预热底层默认为 BASE”语义已经作废。

## 1. 单向依赖

```text
截至观察日的原始市场事实
+ 可验证的历史统计事实
→ 当日趋势状态
```

`BASE`、`CANDIDATE`、`ACTIVE` 和 `previous_trend_state` 均不作为当日状态计算输入。任意日期可以在没有既有 System B 状态记录的情况下独立求值。

## 2. 三个业务状态

业务枚举只有：

```text
BASE
CANDIDATE
ACTIVE
```

独立事实定义：

- `BASE`：状态依据的最近有效实际交易日满足 `close < MA5`；
- `CANDIDATE`：目标依据日 `close >= MA5`，且紧邻前一实际交易日 `close < MA5`；
- `ACTIVE`：目标依据日和紧邻前一实际交易日均满足 `close >= MA5`。

三个谓词天然互斥；若实现同时匹配多个谓词，以 `CONFLICTING_STATE_FACTS` fail-closed，不设置优先级，也不以 BASE 兜底。

## 3. NULL

`trend_state=NULL` 不是第四种状态，只表示截至目标日的可验证事实不足以唯一计算三个业务状态之一。不得新增 `UNKNOWN`、`UNDETERMINED` 或 `UNRESOLVED` 状态，也不存在以 NULL 为端点的业务迁移。

原因通过 `diagnostics` 表达，包括：

```text
NEW_LISTING_WARMUP
INSUFFICIENT_STATE_FACTS
BROKEN_TRADING_SEQUENCE
MISSING_PREVIOUS_ACTUAL_TRADING_FACT
NO_UNIQUE_STATE_MATCH
INCOMPLETE_MA5_WINDOW
UNCERTAIN_LISTING_TRADING_DAY_NUMBER
```

后续日期仍重新根据截至当日的历史事实独立求值。

## 4. 生命周期

精确实际交易日序号可证明时，前 10 个实际交易日为 `NEW_LISTING_WARMUP`，第 11 日起为 `NORMAL`。发生 `UNRESOLVED_MISSING` 后，`listing_trading_day_number=NULL`、`listing_trading_day_number_is_exact=false`，并另存 `confirmed_listing_trading_day_count`。确认交易日已超过 10 个时仍可证明 `NORMAL`；否则生命周期也为 NULL，不提前解除预热。

预热不删除历史行情事实。第 11 个实际交易日直接使用第 10 日及此前事实判定，不从 BASE 启动、不清零任何事实统计。

## 5. 停牌、零成交量与缺口

- `ACTUAL_TRADING`：有效实际交易日；进入实际交易日序列；
- `EXPLICIT_NON_TRADING`：明确停牌或正式零成交量日；不进入实际交易日序列，状态从截至当日最近的完整实际交易事实派生；
- `UNRESOLVED_MISSING`：无日线且无明确停牌等无法解释的缺口；破坏实际交易日连续性证明。

MA5 必须由同一连续可证明片段中的当前实际交易日和前 4 个实际交易日组成。`UNRESOLVED_MISSING` 会切断片段，禁止跳过缺口并使用更早价格补窗。缺口后第 1—4 个确认实际交易日 MA5 为 NULL，第 5 日首次形成可信 MA5：当日线下可判定 BASE；CANDIDATE/ACTIVE 还要求紧邻前一实际交易日也具有可信 MA5，通常从第 6 日起恢复。

## 6. 标准事实输入

每行输入除观察日本身的 `close/ma5` 外，还携带集合化 SQL 从原始事实计算出的充分统计：

```text
latest_actual_trade_date
latest_actual_close
latest_actual_ma5
latest_actual_ma5_window_complete
latest_actual_is_above_or_equal_ma5
previous_actual_trade_date
previous_actual_is_above_or_equal_ma5
previous_actual_ma5_window_complete
ma5_window_complete
confirmed_listing_trading_day_count
listing_trading_day_number_is_exact
state_basis_sequence_intact
actual_pair_contiguous
```

这些字段可以从截至目标日的行情、停牌、交易日历和复权事实重新生成，不依赖状态历史。

## 7. 审计字段

当日状态完成后，才按观察日序列生成：

```text
previous_trend_state
state_changed
```

无前一观察日，或当前/前一状态为 NULL 时，`state_changed=NULL`；两日状态均非空时才比较。审计比较不回流到状态判定。

## 8. checkpoint

正式模型不需要状态 checkpoint，也不维护 checkpoint 表。生产 SQL 直接为目标观察日生成充分历史统计事实；删除全部 System B 状态历史后仍可计算任意目标日。

未来如果为性能引入缓存，只能缓存上述原始事实派生统计，并必须与直接历史重放逐列一致；不得缓存趋势状态用于计算。

## 9. 版本

```text
rule_version_set_id = system_b_2_0_fact_derived_ma5_complete_1__user_20260726
parameter_set_id = system_b_2_0_fact_derived_ma5_complete_1_params_1
calculation_version = system_b_fact_derived_state@2.1.0
```

旧递推计算版本不覆盖、不复用。
