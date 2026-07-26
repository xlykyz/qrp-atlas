# System B 基础状态机工程实施说明

> 实施范围：System B 2.0 基础趋势状态与新股预热
> 目标分支：`develop/v1.1`
> QRP 工程映射基线：`e13f427b38b569b3845613471f1b611b355ab164`

## 1. 规则来源与本次范围

本实现只引用以下固定外部事实来源，不复制完整业务规则正文：

```text
repository: xlykyz/MyTradingSystem
commit: 82369650d16914e42c03da7635f410b12a38220e
primary_document: docs/15_交易系统2.0(初稿).md
source_locator: §8, §9, §10, §11, §11.1
```

本次执行的稳定规则 ID：

- `SB20.DATA.001`：前复权与最终收盘口径；
- `SB20.DATA.002`：个股实际交易日和停牌连续性；
- `SB20.STATE.001`：上市前 10 个实际交易日预热；
- `SB20.STATE.002`：基础趋势状态机。

未执行、也未在结果中声明任何 `DEFERRED` 规则。

## 2. 实际模块落位

| 模块 | 职责 |
|---|---|
| `src/qrp_atlas/contracts/system_b.py` | 状态枚举、参数集、版本 ID、输入/输出列、冻结 checkpoint、请求和结果契约 |
| `src/qrp_atlas/indicators/system_b/state_machine_v2.py` | 纯计算、输入校验、排序、重复拒绝、全量计算和增量续算 |
| `src/qrp_atlas/indicators/system_b/__init__.py` | 保留 1.0 公开入口，同时公开独立 2.0 入口 |
| `src/qrp_atlas/indicators/__init__.py` | 指标层顶级公开入口 |
| `tests/indicators/test_system_b_state_machine_v2.py` | 表驱动迁移、预热、停牌、恢复、审计和边界测试 |

基础状态机不访问 DuckDB、PostgreSQL、Tushare、AKShare、QMT、系统时间或隐式全局状态。

## 3. 正式版本与参数契约

```text
rule_version_set_id = system_b_2_0_draft_1__mts_8236965
parameter_set_id = system_b_2_0_draft_1_params_1
price.adjustment = FORWARD_ADJUSTED
new_listing.warmup_trading_days = 10
trend.ma_period = 5
trend.active_confirm_days = 2
trend.exit_confirm_days = 2
```

调用方必须显式提交 `SystemBStateMachineParameters`、输入价格复权声明、规则版本集、参数集和初始 checkpoint 集合。实现不使用缺省参数补齐未提供值；与冻结定义不一致时显式拒绝。

## 4. 输入契约

每条输入观察至少包含：

```text
asset_id
trade_date
is_trading_day
listing_trading_day_number
close
ma5
```

其中：

- `listing_trading_day_number` 是由正式数据管道或调用方注入的上市实际交易日事实；指标内部不查询上市日期或数据库；
- `is_trading_day=true` 的行必须提供有限的最终前复权 `close` 和 `ma5`；
- `is_trading_day=false` 表示停牌或非该股实际交易日，上市交易日序号必须与上一观察一致；该行不读取价格，不推进连续计数；
- 无 checkpoint 的全量历史必须从上市实际交易日 1 开始，避免对截断历史猜测先前状态；
- `trade_date` 是交易日标签。timezone-aware 输入保留本地日历日期，不先转 UTC；
- 输入按 `asset_id, trade_date` 稳定排序；排序动作写入批次诊断；规范化后的重复主键显式拒绝。

## 5. 正式输出契约

每条结果包含：

```text
asset_id
trade_date
trend_state
underlying_trend_state
previous_trend_state
state_changed
is_trading_day
listing_trading_day_number
close
ma5
is_above_or_equal_ma5
consecutive_above_ma5_days
consecutive_below_ma5_days
price_adjustment
rule_version_set_id
parameter_set_id
source_rule_ids
diagnostics
```

`trend_state` 使用枚举 `SystemBTrendState` 定义的正式值：

- `NEW_LISTING_WARMUP`
- `BASE`
- `CANDIDATE`
- `ACTIVE`

`underlying_trend_state` 在预热期固定为 `BASE`，正常状态期与 `trend_state` 一致，从而同时表达“对外正式预热状态”和“预热期基础状态强制为 BASE”。

## 6. 状态迁移表

以下迁移只在实际交易日最终收盘后执行，`close == ma5` 归入线上：

| 上一基础状态 | 当日最终关系 | 当日连续计数 | 新状态 |
|---|---|---:|---|
| `BASE` | `close < ma5` | 线下继续累计 | `BASE` |
| `BASE` | `close >= ma5` | 线上 1 日 | `CANDIDATE` |
| `CANDIDATE` | `close >= ma5` | 线上 2 日 | `ACTIVE` |
| `CANDIDATE` | `close < ma5` | 线下 1 日 | `BASE` |
| `ACTIVE` | `close >= ma5` | 线上累计、线下清零 | `ACTIVE` |
| `ACTIVE` | `close < ma5` | 线下 1 日 | `ACTIVE` |
| `ACTIVE` | `close < ma5` | 线下 2 日 | `BASE` |

进入相反方向时重置另一方向的连续计数。同一输入序列不依赖调用时刻或外部状态，结果确定且可复现。

## 7. 新股预热边界

- 上市实际交易日 1—10：`trend_state=NEW_LISTING_WARMUP`；
- 同期 `underlying_trend_state=BASE`，线上/线下计数均为 0；
- 第 10 个实际交易日仍处于预热；
- 停牌日不增加 `listing_trading_day_number`，也不会使预热提前结束；
- 第 11 个实际交易日从 `BASE` 开始执行正常迁移：线上进入 `CANDIDATE`，线下保持 `BASE`；
- 预热期价格相对 MA5 仅作为审计事实输出，不带入第 11 日的连续确认计数。

## 8. 停牌处理

停牌观察行：

- 保持上一 `trend_state` 和 `underlying_trend_state`；
- 保持线上/线下连续计数；
- `is_above_or_equal_ma5=null`，输出价格字段为空；
- 写入稳定诊断 `NON_TRADING_DAY_STATE_HELD`；
- 复牌后按下一实际交易日继续既有计数。

因此“线上一天—停牌—再次线上”进入 `ACTIVE`，“线下一天—停牌—再次线下”触发从 `ACTIVE` 回到 `BASE`。

## 9. 全量与增量一致性

`SystemBStateCheckpoint` 冻结每个标的续算所需的最小状态：

```text
asset_id
last_observation_date
trend_state
underlying_trend_state
listing_trading_day_number
consecutive_above_ma5_days
consecutive_below_ma5_days
```

全量计算从上市实际交易日 1 开始；增量计算显式传入上一批次 `final_states`。checkpoint 会验证日期前进、状态/计数一致性和上市交易日序号推进。多标的 checkpoint 按 `asset_id` 隔离，未使用共享可变状态。

专项测试将同一历史序列分别执行全量计算和分段续算，并对全部输出行及最终 checkpoint 做完全一致比较。

## 10. 与 system_b_basic@1.0.0 的兼容关系

旧能力继续位于：

```text
src/qrp_atlas/indicators/system_b/detector.py
src/qrp_atlas/strategies/builtin/system_b_basic.py
```

本任务没有修改其计算语义、字段或策略版本。System B 2.0 使用新的请求、结果、状态枚举和入口 `calculate_system_b_2_0_states`，不在旧 detector 上原地扩写。兼容测试同时运行旧 detector 和旧 `system_b_basic@1.0.0` 测试集。

## 11. 明确未实现边界

本任务未实现：

- 行情轮次、`episode_status`、轮次统计或 MA10 结束逻辑；
- 涨停状态池、高度池、容量池、辨识度池；
- 题材、市场结构、评分或 M 身份；
- 判断层、新增仓授权、组合目标；
- 回测成交、订单计划、execution 或实盘连接；
- 数据下载、数据库读取、上市交易日事实生产和 MA5 计算管道。

这些能力只能在后续工作包消费本任务正式结果，不得反向写入基础状态机。

## 12. 测试证据与已知限制

专项测试覆盖：

- 预热 1—10 日与第 11 日边界；
- 预热跨停牌；
- 表驱动状态迁移；
- 两类停牌连续语义；
- 等于 MA5；
- 缺失字段和值；
- 重复键、乱序和多标的隔离；
- 全量与增量一致；
- timezone-aware 日期；
- 版本、参数、规则来源审计；
- 旧 1.0 行为兼容；
- 禁止数据库、时钟、行情轮次和上层领域依赖。

验证命令：

```powershell
python -m pytest tests\indicators\test_system_b_state_machine_v2.py -q
python -m pytest tests\indicators\test_detector.py tests\strategies\test_system_b_basic.py tests\strategies\test_registry.py -q
python -m pytest -q
python -m compileall -q src tests
```

本分支最终证据：

- System B 2.0 专项：26 passed；
- System B 专项加旧版兼容集合：45 passed；
- 全量测试：776 passed；
- `python -m compileall -q src tests`：通过；
- `git diff --check`：通过。

为使既有全量测试在 Windows 上可执行，`tests/config/test_setup.py` 将仅适用于 POSIX 的 `0600` 权限位断言加上 `os.name == "posix"` 条件；生产配置代码未改，该断言在基线提交中已与同文件的备份权限测试不一致。

已知限制：本组件消费而不生产前复权行情、MA5、停复牌和上市实际交易日序号；这些上游事实缺失或不一致时，计算会显式失败，不做推断或数据库回补。
