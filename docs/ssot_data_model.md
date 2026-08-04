# qrp-atlas 数据契约 SSOT 总览

> 本文件自动整理自 `contracts/` 下 3 个核心单事实来源(SSOT)文件：
> `fields.py`（字段常量）、`schema.py`（表结构）、`mappings.py`（数据源映射）
>
> **原则：所有数据定义以此为唯一权威来源，其他模块只引用不重定义。**

---

## 一、字段常量 (fields.py)

所有字段名在此统一定义，全项目通过 `from qrp_atlas.contracts import TICKER, ...` 引用。

### 1.1 通用标识字段

| 常量名 | 值 | 类型 |
|--------|-----|------|
| TICKER | ticker | 股票代码 |
| TRADE_DATE | trade_date | 交易日期 |
| NAME | name | 股票名称 |
| CREATED_AT | created_at | 创建时间戳 |

### 1.2 OHLCV 行情字段

| 常量名 | 值 | 说明 |
|--------|-----|------|
| OPEN | open | 开盘价 |
| HIGH | high | 最高价 |
| LOW | low | 最低价 |
| CLOSE | close | 收盘价 |
| VOLUME | volume | 成交量（股） |
| AMOUNT | amount | 成交额（元） |

### 1.3 涨跌/衍生字段

| 常量名 | 值 | 说明 |
|--------|-----|------|
| PCT_CHANGE | pct_change | 涨跌幅（%） |
| PRE_CLOSE | pre_close | 昨收价 |
| TURNOVER | turnover | 换手率（%） |
| MARKET_CAP | market_cap | 总市值（元） |
| FLOAT_CAP | float_cap | 流通市值（元） |
| ADJ_FACTOR | adj_factor | 复权因子 |

### 1.4 状态标记字段

| 常量名 | 值 | 类型 |
|--------|-----|--------|
| IS_ST | is_st | BOOLEAN |
| IS_LIMIT_UP | is_limit_up | BOOLEAN |
| IS_LIMIT_DOWN | is_limit_down | BOOLEAN |

### 1.5 市场阶段字段

| 常量名 | 值 |
|--------|-----|
| PHASE | phase |
| M1_CORE | M1_core |
| M2_FRONT | M2_front |
| M3_IDENTIFIABLE | M3_identifiable |
| V_TRIGGERED | V_triggered |
| NOTES | notes |

### 1.6 交易执行字段

| 常量名 | 值 | 说明 |
|--------|-----|------|
| TRADE_ID | trade_id | 交易ID |
| ENTRY_DATE | entry_date | 入场日期 |
| ENTRY_PRICE | entry_price | 入场价格 |
| PATH_TYPE | path_type | 路径类型 |
| HALF_SELL_TRIGGER | half_sell_trigger | 半卖触发价 |
| HALF_SELL_DATE | half_sell_date | 半卖日期 |
| HALF_SELL_PRICE | half_sell_price | 半卖价格 |
| EXIT_DATE | exit_date | 出场日期 |
| EXIT_PRICE | exit_price | 出场价格 |
| POSITION_PCT | position_pct | 仓位占比 |

### 1.7 股票信息字段

| 常量名 | 值 |
|--------|-----|
| EXCHANGE | exchange |
| MARKET | market |
| LIST_DATE | list_date |
| DELIST_DATE | delist_date |
| IS_ACTIVE | is_active |
| UPDATED_AT | updated_at |

### 1.8 交易日历字段

| 常量名 | 值 |
|--------|-----|
| IS_OPEN | is_open |
| YEAR_FIELD | year |
| MONTH_FIELD | month |
| QUARTER | quarter |

### 1.9 字段分组常量

| 分组名 | 字段列表 |
|--------|-----------|
| OHLCV_FIELDS | open, high, low, close, volume |
| PRICE_FIELDS | open, high, low, close, pre_close, entry_price, exit_price, half_sell_price |
| NUMERIC_FIELDS | open, high, low, close, volume, amount, pct_change, turnover, market_cap, float_cap, pre_close, entry_price, exit_price, half_sell_price, half_sell_trigger, position_pct |
| BOOLEAN_FIELDS | is_st, is_limit_up, is_limit_down, M1_core, M2_front, M3_identifiable, V_triggered |
| DATE_FIELDS | trade_date, entry_date, exit_date, half_sell_date |
| IDENTIFIER_FIELDS | ticker, trade_id |

---

## 二、表结构定义 (schema.py)

### 2.1 daily_market_snapshot — 每日全市场行情快照

- **主键**: (trade_date, ticker)
- **17 列**

| 列名 | 类型 | 非空 | 说明 |
|------|------|:----:|------|
| trade_date | DATE | ✅ | 交易日 |
| ticker | VARCHAR | ✅ | 股票代码 |
| name | VARCHAR | | 股票名称 |
| open | DOUBLE | | 开盘价 |
| high | DOUBLE | | 最高价 |
| low | DOUBLE | | 最低价 |
| close | DOUBLE | | 收盘价 |
| pct_change | DOUBLE | | 涨跌幅(%) |
| pre_close | DOUBLE | | 昨收价 |
| volume | BIGINT | | 成交量(股) |
| amount | DOUBLE | | 成交额(元) |
| turnover | DOUBLE | | 换手率(%) |
| market_cap | DOUBLE | | 总市值(元) |
| float_cap | DOUBLE | | 流通市值(元) |
| is_st | BOOLEAN | | 是否ST |
| is_limit_up | BOOLEAN | | 是否涨停 |
| is_limit_down | BOOLEAN | | 是否跌停 |
| created_at | TIMESTAMP | | 创建时间(默认当前) |

### 2.2 market_phase — 每日市场阶段判断

- **主键**: (trade_date)
- **8 列**

| 列名 | 类型 | 非空 | 说明 |
|------|------|:----:|------|
| trade_date | DATE | ✅ | 交易日 |
| phase | VARCHAR | | 市场阶段标签 |
| M1_core | BOOLEAN | | M1 核心阶段 |
| M2_front | BOOLEAN | | M2 前排阶段 |
| M3_identifiable | BOOLEAN | | M3 可识别阶段 |
| V_triggered | BOOLEAN | | V 型反转触发 |
| notes | VARCHAR | | 备注 |
| created_at | TIMESTAMP | | 创建时间 |

### 2.3 trade_execution — 交易执行记录

- **主键**: (trade_id)
- **12 列**

| 列名 | 类型 | 非空 | 说明 |
|------|------|:----:|------|
| trade_id | VARCHAR | ✅ | 交易ID |
| ticker | VARCHAR | | 股票代码 |
| entry_date | DATE | | 入场日期 |
| entry_price | DOUBLE | | 入场价格 |
| path_type | VARCHAR | | 路径类型 |
| half_sell_trigger | DOUBLE | | 半卖触发价 |
| half_sell_date | DATE | | 半卖日期 |
| half_sell_price | DOUBLE | | 半卖价格 |
| exit_date | DATE | | 出场日期 |
| exit_price | DOUBLE | | 出场价格 |
| position_pct | DOUBLE | | 仓位占比 |
| notes | VARCHAR | | 备注 |

### 2.4 stock_info — 股票基础信息

- **主键**: (ticker)
- **Tushare `stock_basic` 完整当前快照 + 兼容字段**
- **20 列**

该表只保留最近一次成功同步的当前状态，不保存历史快照。正式来源为
Tushare `stock_basic`，Pipeline 按 `L/D/P/G` 状态和 `SSE/SZSE/BSE`
交易所分区拉取后，在一个事务中全量替换本表。

| 列名 | 类型 | 非空 | 说明 |
|------|------|:----:|------|
| ts_code | VARCHAR | | Tushare TS 代码 |
| symbol | VARCHAR | | 股票代码 |
| name | VARCHAR | | 股票名称 |
| area | VARCHAR | | 地域 |
| industry | VARCHAR | | 所属行业 |
| fullname | VARCHAR | | 股票全称 |
| enname | VARCHAR | | 英文全称 |
| cnspell | VARCHAR | | 拼音缩写 |
| market | VARCHAR | | 市场标识 |
| exchange | VARCHAR | | 交易所代码 |
| curr_type | VARCHAR | | 交易货币 |
| list_status | VARCHAR | | 上市状态 L/D/P/G |
| list_date | DATE | | 上市日期 |
| delist_date | DATE | | 退市日期 |
| is_hs | VARCHAR | | 沪深港通标的 N/H/S |
| act_name | VARCHAR | | 实际控制人名称 |
| act_ent_type | VARCHAR | | 实际控制人企业性质 |
| ticker | VARCHAR | ✅ | 兼容字段，等于 ts_code |
| is_active | BOOLEAN | | 兼容字段，等于 list_status = L |
| updated_at | TIMESTAMP | | 最近一次同步时间，不表示历史版本 |

### 2.5 trading_calendar — 交易日历

- **主键**: (trade_date)
- **5 列**

| 列名 | 类型 | 非空 | 说明 |
|------|------|:----:|------|
| trade_date | DATE | ✅ | 交易日 |
| is_open | BOOLEAN | | 是否开盘 |
| year | INTEGER | | 年份 |
| month | INTEGER | | 月份 |
| quarter | INTEGER | | 季度 |

### 2.6 adj_factor_changes — 复权因子变更

- **主键**: (ticker, trade_date)
- **3 列**

| 列名 | 类型 | 非空 | 说明 |
|------|------|:----:|------|
| ticker | VARCHAR | ✅ | 股票代码 |
| trade_date | DATE | ✅ | 发生日期 |
| adj_factor | DOUBLE | | 复权因子 |

---

## 三、数据源字段映射 (mappings.py)

### 3.1 tushare_daily — Tushare 日线

| 源字段 | 标准字段 |
|--------|----------|
| ts_code | ticker |
| trade_date | trade_date |
| open | open |
| high | high |
| low | low |
| close | close |
| pre_close | pre_close |
| pct_chg | pct_change |
| vol | volume |
| amount | amount |

### 3.2 akshare_daily_bar — AKShare 日线

| 源字段 | 标准字段 |
|--------|----------|
| 代码 | ticker |
| 日期 | trade_date |
| 开盘 | open |
| 最高 | high |
| 最低 | low |
| 收盘 | close |
| 成交量 | volume |
| 成交额 | amount |
| 涨跌幅 | pct_change |
| 换手率 | turnover |

### 3.3 akshare_realtime — AKShare 实时行情

| 源字段 | 标准字段 |
|--------|----------|
| 代码 | ticker |
| 名称 | name |
| 最新价 | close |
| 涨跌幅 | pct_change |
| 成交量 | volume |
| 成交额 | amount |
| 换手率 | turnover |
| 总市值 | market_cap |
| 流通市值 | float_cap |

### 3.4 sina_realtime — 新浪实时行情

| 源字段 | 标准字段 |
|--------|----------|
| 代码 | ticker |
| 名称 | name |
| 最新价 | close |
| 涨跌额 | change（无常量） |
| 涨跌幅 | pct_change |
| 昨收 | pre_close |
| 今开 | open |
| 最高 | high |
| 最低 | low |
| 成交量 | volume |
| 成交额 | amount |
