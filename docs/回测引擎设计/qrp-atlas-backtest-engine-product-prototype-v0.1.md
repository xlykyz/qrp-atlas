# qrp-atlas Backtest Engine 产品原型 v0.1

## 1. 产品定位

Backtest Engine 是 qrp-atlas 的通用量化回测底座。

它不负责发现机会，不负责定义策略，不负责解释市场，只负责一件事：

> 给定标准行情数据、标准信号数据和交易规则配置，模拟交易过程，并输出交易明细与统计结果。

**核心输入：**

```text
price_df + signals_df + config
```

**核心输出：**

```text
BacktestResult = trades + summary + skipped + equity_curve
```

第一版目标不是做完整量化平台，而是搭建一个稳定、可扩展、可测试的后端交易模拟器。

## 2. 设计原则

### 2.1 引擎必须通用

引擎内部不允许出现以下策略概念：

- M 节点
- W 节点
- 涨停
- 跌停
- 五日线
- Ryan 系统
- 退潮
- 主线
- 题材

这些都属于外部信号生成器或策略模块。

引擎只认识：

- 资产
- 日期
- 价格
- 信号
- 入场
- 出场
- 仓位
- 成本
- 交易
- 收益
- 风险

### 2.2 数据、信号、回测三者解耦

回测流程拆成三步：

1. **数据读取**：从 DuckDB / CSV / 其他来源读取行情。
2. **信号生成**：由外部模块生成标准 `signals_df`。
3. **回测执行**：`BacktestEngine` 消费 `price_df + signals_df + config`。

引擎不读取数据库，也不生成信号。

### 2.3 第一版只做最小闭环

v0.1 只支持：

- long 方向
- 信号日收盘 / 次日开盘 / 次日收盘入场
- 固定持有 N 根 bar 后出场
- 单笔交易收益统计
- 基础成本模型
- MAE / MFE
- skipped 记录

暂不支持：

- 复杂资金曲线
- 动态调仓
- 多策略组合
- 涨跌停不可成交
- 停牌特殊处理
- 融资融券
- 做空
- 分钟级数据
- 参数优化
- 机器学习
- 前端页面

## 3. 用户故事

### 3.1 作为研究者，我希望手工构造一组信号并快速验证

**示例：**

> 我给出某几只股票在某几个日期的信号，引擎帮我计算信号日收盘买入、持有 5 天后的收益、MAE、MFE 和胜率。

### 3.2 作为策略开发者，我希望信号生成和回测执行分离

**示例：**

> 我可以先写一个“涨停信号生成器”，生成 `signals_df`；再把 `signals_df` 交给通用引擎。以后换成“指数节点信号”或“均线突破信号”，引擎不用改。

### 3.3 作为交易系统维护者，我希望后续能逐步扩展专属规则

**示例：**

> 第一版固定 N 日出场；第二版增加跌破均线出场；第三版增加 -5% 收盘止损；第四版加入市场阶段过滤。

## 4. 核心对象

### 4.1 PriceFrame

标准行情输入。

**必需字段：**

- `trade_date`
- `asset_id`
- `asset_name`
- `asset_type`
- `open`
- `high`
- `low`
- `close`

**可选字段：**

- `volume`
- `amount`
- `turnover`
- `market_cap`
- `float_cap`
- `is_st`
- `is_limit_up`
- `is_limit_down`

**字段说明：**

| 字段 | 说明 |
|---|---|
| `asset_id` | 统一资产代码，例如股票 ticker、指数 index_code |
| `asset_name` | 资产名称 |
| `asset_type` | `stock` / `index` / `sector` / `fund` / `future` / `custom` |
| `trade_date` | 交易日期 |
| `open/high/low/close` | 行情价格 |

**示例：**

| trade_date | asset_id | asset_name | asset_type | open | high | low | close |
|---|---|---|---|---:|---:|---:|---:|
| 2024-01-02 | 000001.SZ | 平安银行 | stock | 9.80 | 9.95 | 9.70 | 9.88 |
| 2024-01-03 | 000001.SZ | 平安银行 | stock | 9.90 | 10.10 | 9.85 | 10.02 |

### 4.2 SignalFrame

标准信号输入。

**必需字段：**

- `signal_date`
- `asset_id`
- `direction`

**可选字段：**

- `asset_type`
- `signal_name`
- `score`
- `weight`
- `meta`

**字段说明：**

| 字段 | 说明 |
|---|---|
| `signal_date` | 信号触发日期 |
| `asset_id` | 信号对应资产 |
| `direction` | `long` / `short`，v0.1 只支持 `long` |
| `signal_name` | 信号名称，只作为标签，不影响引擎逻辑 |
| `score` | 信号强度 |
| `weight` | 建议权重 |
| `meta` | 外部信号模块附带的信息 |

**示例：**

| signal_date | asset_id | direction | signal_name | score | weight |
|---|---|---|---|---:|---:|
| 2024-01-10 | 000001.SZ | long | manual_test | 1.0 | 1.0 |
| 2024-01-12 | 000002.SZ | long | manual_test | 1.0 | 1.0 |

### 4.3 BacktestConfig

回测配置。

```python
BacktestConfig(
    name="manual_test_hold_5d",
    entry=EntryRule(
        timing="signal_close",
        price_field="close",
    ),
    exit=ExitRule(
        type="hold_n_bars",
        bars=5,
        price_field="close",
    ),
    position=PositionRule(
        initial_cash=1_000_000,
        position_pct=1.0,
        max_positions=999999,
        allow_overlap=True,
        compound=False,
    ),
    cost=CostRule(
        commission_rate=0.00025,
        stamp_tax_rate=0.0005,
        slippage_bps=0,
    ),
)
```

## 5. 规则模型

### 5.1 EntryRule

入场规则。

v0.1 支持：

| timing | 含义 |
|---|---|
| `signal_close` | 信号日收盘买入 |
| `next_open` | 下一交易日开盘买入 |
| `next_close` | 下一交易日收盘买入 |

**字段：**

- `timing`
- `price_field`

**示例：**

```python
EntryRule(
    timing="next_open",
    price_field="open",
)
```

### 5.2 ExitRule

出场规则。

v0.1 只支持：

- `hold_n_bars`

**含义：**

> 入场后持有 N 根交易 bar，然后按指定价格字段出场。

**字段：**

- `type`
- `bars`
- `price_field`

**示例：**

```python
ExitRule(
    type="hold_n_bars",
    bars=5,
    price_field="close",
)
```

### 5.3 PositionRule

仓位规则。

v0.1 先不做复杂组合管理，但预留字段。

**字段：**

- `initial_cash`
- `position_pct`
- `max_positions`
- `allow_overlap`
- `compound`

**字段解释：**

| 字段 | 说明 |
|---|---|
| `initial_cash` | 初始资金 |
| `position_pct` | 单笔交易占用资金比例 |
| `max_positions` | 最大同时持仓数 |
| `allow_overlap` | 同一资产是否允许重复开仓 |
| `compound` | 是否复利 |

v0.1 中，主要用于记录和后续扩展，不强制生成完整资金曲线。

### 5.4 CostRule

成本规则。

**字段：**

- `commission_rate`
- `stamp_tax_rate`
- `slippage_bps`

**计算方式：**

```python
buy_cost = commission_rate + slippage_bps / 10000
sell_cost = commission_rate + stamp_tax_rate + slippage_bps / 10000
net_return = gross_return - buy_cost - sell_cost
```

## 6. 回测流程

整体流程：

1. 校验 `price_df`
2. 校验 `signals_df`
3. 校验 `config`
4. 按 `asset_id` 建立行情索引
5. 遍历每条 signal
6. 找到入场 bar
7. 找到出场 bar
8. 计算单笔交易收益
9. 计算 MAE / MFE
10. 生成 Trade
11. 汇总 Metrics
12. 返回 BacktestResult

伪流程：

```python
engine = BacktestEngine()

result = engine.run(
    price_df=price_df,
    signals_df=signals_df,
    config=config,
)
```

## 7. Trade 输出结构

每一笔交易输出为：

- `asset_id`
- `asset_name`
- `asset_type`
- `signal_date`
- `signal_name`
- `direction`
- `entry_date`
- `entry_price`
- `exit_date`
- `exit_price`
- `holding_bars`
- `gross_return`
- `net_return`
- `mae`
- `mfe`
- `meta`

**示例：**

```json
{
  "asset_id": "000001.SZ",
  "asset_name": "平安银行",
  "asset_type": "stock",
  "signal_date": "2024-01-10",
  "signal_name": "manual_test",
  "direction": "long",
  "entry_date": "2024-01-10",
  "entry_price": 10.00,
  "exit_date": "2024-01-17",
  "exit_price": 10.50,
  "holding_bars": 5,
  "gross_return": 0.05,
  "net_return": 0.049,
  "mae": -0.03,
  "mfe": 0.08,
  "meta": {}
}
```

## 8. Skipped 输出结构

当信号无法完成交易时，不中断整个回测，而是记录 skipped。

**常见原因：**

- `NO_PRICE_DATA`
- `SIGNAL_DATE_NOT_FOUND`
- `NO_NEXT_BAR_FOR_ENTRY`
- `NO_EXIT_BAR`
- `INVALID_DIRECTION`
- `INVALID_PRICE`

**示例：**

```json
{
  "asset_id": "000001.SZ",
  "signal_date": "2024-01-10",
  "reason": "NO_EXIT_BAR",
  "detail": "not enough future bars for hold_n_bars=20"
}
```

## 9. Summary 指标

v0.1 输出：

- `trade_count`
- `skipped_count`
- `win_count`
- `loss_count`
- `win_rate`
- `avg_return`
- `median_return`
- `max_return`
- `min_return`
- `avg_win`
- `avg_loss`
- `profit_loss_ratio`
- `avg_holding_bars`
- `avg_mae`
- `avg_mfe`
- `worst_mae`
- `best_mfe`

**说明：**

收益率统一用小数表示：

```text
0.05 = 5%
-0.03 = -3%
```

空交易时，summary 不报错，返回：

```json
{
  "trade_count": 0,
  "skipped_count": 0,
  "win_rate": null,
  "avg_return": null
}
```

## 10. BacktestResult 输出结构

```python
BacktestResult(
    config=config,
    summary={
        "trade_count": 120,
        "win_rate": 0.56,
        "avg_return": 0.018,
        "median_return": 0.009,
        "max_return": 0.42,
        "min_return": -0.18,
        "avg_mae": -0.045,
        "avg_mfe": 0.067,
    },
    trades=[...],
    skipped=[...],
    equity_curve=[],
)
```

v0.1 中 `equity_curve` 可以先为空，后续再实现组合资金曲线。

## 11. 模块结构

建议目录：

```text
src/qrp_atlas/backtest/
├── __init__.py
├── models.py
├── data.py
├── engine.py
├── broker.py
├── metrics.py
├── validators.py
└── examples/
    └── smoke.py
```

职责划分：

| 文件 | 职责 |
|---|---|
| `models.py` | 数据结构 |
| `data.py` | 从项目数据库读取并标准化行情 |
| `validators.py` | 输入校验 |
| `broker.py` | 撮合交易、生成 trades |
| `metrics.py` | 汇总指标 |
| `engine.py` | 编排主流程 |
| `examples/` | 最小可运行示例 |

## 12. data.py 原型

`data.py` 不属于引擎核心，只是项目适配层。

提供：

- `load_stock_prices(...)`
- `load_index_prices(...)`
- `normalize_price_frame(...)`

### 12.1 load_stock_prices

从 `daily_market_snapshot` 读取股票行情，标准化成 PriceFrame。

**映射关系：**

| 来源字段 | 目标字段 / 值 |
|---|---|
| `ticker` | `asset_id` |
| `name` | `asset_name` |
| 固定值 | `"stock" -> asset_type` |
| `trade_date` | `trade_date` |
| `open/high/low/close` | `open/high/low/close` |

### 12.2 load_index_prices

从 `index_daily` 读取指数行情，标准化成 PriceFrame。

**映射关系：**

| 来源字段 | 目标字段 / 值 |
|---|---|
| `index_code` | `asset_id` |
| `index_name` | `asset_name` |
| 固定值 | `"index" -> asset_type` |
| `trade_date` | `trade_date` |
| `open/high/low/close` | `open/high/low/close` |

## 13. v0.1 示例使用方式

```python
import pandas as pd

from qrp_atlas.backtest.data import load_stock_prices
from qrp_atlas.backtest.engine import BacktestEngine
from qrp_atlas.backtest.models import (
    BacktestConfig,
    EntryRule,
    ExitRule,
    PositionRule,
    CostRule,
)

price_df = load_stock_prices(
    tickers=["000001.SZ", "000002.SZ"],
    start_date="2024-01-01",
    end_date="2024-12-31",
)

signals_df = pd.DataFrame([
    {
        "signal_date": "2024-03-01",
        "asset_id": "000001.SZ",
        "direction": "long",
        "signal_name": "manual_test",
        "score": 1.0,
        "weight": 1.0,
    },
    {
        "signal_date": "2024-04-10",
        "asset_id": "000002.SZ",
        "direction": "long",
        "signal_name": "manual_test",
        "score": 1.0,
        "weight": 1.0,
    },
])

config = BacktestConfig(
    name="manual_hold_5_bars",
    entry=EntryRule(
        timing="signal_close",
        price_field="close",
    ),
    exit=ExitRule(
        type="hold_n_bars",
        bars=5,
        price_field="close",
    ),
    position=PositionRule(
        initial_cash=1_000_000,
        position_pct=1.0,
        max_positions=999999,
        allow_overlap=True,
        compound=False,
    ),
    cost=CostRule(
        commission_rate=0.00025,
        stamp_tax_rate=0.0005,
        slippage_bps=0,
    ),
)

result = BacktestEngine().run(
    price_df=price_df,
    signals_df=signals_df,
    config=config,
)

print(result.summary)
print(result.trades[:10])
print(result.skipped)
```

## 14. 验收标准

v0.1 完成后，应满足：

1. 不依赖任何具体策略定义
2. 不修改数据库
3. 不写入数据库
4. 不做前端
5. 能用手工 `signals_df` 跑通完整回测
6. 能从 `daily_market_snapshot` 读取股票行情
7. 能从 `index_daily` 读取指数行情
8. 能生成 trades
9. 能生成 summary
10. 能记录 skipped
11. 空信号、空行情、缺失未来数据时不崩溃
12. 后续可以无缝接入节点信号、涨停信号、五日线信号等外部模块

## 15. 后续路线

### v0.2：信号生成器层

新增外部 signal generators：

- `manual_signal`
- `custom_sql_signal`
- `limit_up_signal`
- `index_node_signal`
- `moving_average_signal`

注意：这些不写入 engine 内部。

### v0.3：更真实的交易规则

新增：

- `stop_loss`
- `take_profit`
- `ma_break_exit`
- `max_hold_bars`
- `intraday_limit_down_exit`

### v0.4：组合资金曲线

新增：

- capital allocation
- max_positions
- cash management
- equity_curve
- drawdown curve
- monthly returns
- yearly returns

### v0.5：Ryan 专属系统插件

新增：

- 涨停 + 五日线强趋势
- -5% 收盘止损
- 断板容忍
- 节点过滤
- 尾部风险统计
- 真实交易对照

这些全部作为外部策略模块接入，不污染通用引擎。
