# QRP Atlas API 文档

Base URL: `http://localhost:8000`

---

## GET /api/daily

查询每日行情数据。单日查询自动计算 5/10/20 日累计涨跌幅，自动过滤退市股，自动标注板块分类。

### Parameters

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `date` | string | 否 | 交易日期，`YYYYMMDD` 或 `YYYY-MM-DD` |
| `ticker` | string | 否 | 股票代码，如 `000001.SZ` |
| `start_date` | string | 否 | 起始日期 `YYYYMMDD` |
| `end_date` | string | 否 | 截止日期 `YYYYMMDD` |
| `limit` | int | 否 | 最大返回行数（默认 10000） |

**查询模式：**

| 传参方式 | 行为 |
|----------|------|
| 只传 `date` | 单日查询，用窗口函数计算 5/10/20 日涨跌幅 |
| 只传 `ticker` | 查询该股票全部历史 |
| `start_date` + `end_date` | 范围查询 |
| `date` + `ticker` | 查某股票某日 |
| `start_date` + `end_date` + `ticker` | 查某股票某时间段 |
- 不传任何参数返回全表（最多 limit 行）

### Response

```json
[
  {
    "trade_date": "2026-05-14",
    "ticker": "000001.SZ",
    "name": "平安银行",
    "open": 11.14,
    "high": 11.15,
    "low": 11.05,
    "close": 11.05,
    "pct_change": -0.808,
    "pre_close": 11.14,
    "volume": 79436841,
    "amount": 881960631.0,
    "turnover": null,
    "market_cap": null,
    "float_cap": null,
    "is_st": false,
    "is_limit_up": false,
    "is_limit_down": false,
    "pct_5d": -2.81,
    "pct_10d": -2.9,
    "pct_20d": -0.18,
    "board": "深证主板",
    "created_at": null
  }
]
```

### 字段说明

| 字段 | 类型 | 说明 |
|------|------|------|
| `trade_date` | string | 交易日期 `YYYY-MM-DD` |
| `ticker` | string | 股票代码（含交易所后缀） |
| `name` | string? | 股票名称 |
| `open` | float? | 开盘价 |
| `high` | float? | 最高价 |
| `low` | float? | 最低价 |
| `close` | float? | 收盘价 |
| `pct_change` | float? | 涨跌幅（%） |
| `pre_close` | float? | 昨收价 |
| `volume` | int? | 成交量（股） |
| `amount` | float? | 成交额（元） |
| `turnover` | float? | 换手率（%） |
| `market_cap` | float? | 总市值（元） |
| `float_cap` | float? | 流通市值（元） |
| `is_st` | bool | 是否 ST |
| `is_limit_up` | bool | 是否涨停 |
| `is_limit_down` | bool | 是否跌停 |
| `pct_5d` | float? | 5 日累计涨跌幅（单日查询时） |
| `pct_10d` | float? | 10 日累计涨跌幅（单日查询时） |
| `pct_20d` | float? | 20 日累计涨跌幅（单日查询时） |
| `board` | string | 板块分类：科创板/创业板/上证主板/深证主板/北交所/其他 |

### 板块分类规则

| 代码前缀 | 板块 |
|----------|------|
| `68*` | 科创板 |
| `60*` | 上证主板 |
| `30*` | 创业板 |
| `00*` | 深证主板 |
| `43*`、`83*`、`87*`、`88*`、`92*`、`.BJ` | 北交所 |

### 退市股过滤

名称包含"退市"的股票在 API 层过滤，不返回。

### 涨跌停规则

| 类型 | 阈值 |
|------|------|
| 普通股票 | ≥ 9.9% (涨停) / ≤ -9.9% (跌停) |
| ST 股票 | ≥ 4.9% / ≤ -4.9% |
| 科创/创业板 | ≥ 19.9% / ≤ -19.9% |

---

## GET /api/daily/dates

查询交易日列表。

### Parameters

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `start_date` | string | 否 | 起始日期 `YYYY-MM-DD` |
| `end_date` | string | 否 | 截止日期 `YYYY-MM-DD` |
| `limit` | int | 否 | 最大返回天数（默认 100） |

### Response

```json
["2026-05-14", "2026-05-13", "2026-05-12"]
```

仅返回 `is_open = 1` 的交易日，按日期降序排列。

---

## GET /api/phase

查询市场判读记录（market_phase 表）。

### Parameters

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `date` | string | 否 | 交易日期 `YYYY-MM-DD` |
| `start_date` | string | 否 | 起始日期 |
| `end_date` | string | 否 | 截止日期 |

### Response

```json
[
  {
    "trade_date": "2026-05-14",
    "phase": "上升期",
    "M1_core": true,
    "M2_front": false,
    "M3_identifiable": true,
    "V_triggered": false,
    "notes": "主线方向明确，聚焦核心标的",
    "created_at": "2026-05-14T15:30:00"
  }
]
```

### 判读标记说明

| 字段 | 类型 | 说明 |
|------|------|------|
| `phase` | string? | 市场阶段：上升期/震荡期/下降期/混沌期 |
| `M1_core` | bool? | 是否有核心主线 |
| `M2_front` | bool? | 主线是否轮动 |
| `M3_identifiable` | bool? | 分支是否可识别 |
| `V_triggered` | bool? | 验证信号是否触发 |
| `notes` | string? | 判读笔记 |

---

## POST /api/phase

写入/更新市场判读记录。

### Request Body

```json
{
  "trade_date": "2026-05-14",
  "phase": "上升期",
  "M1_core": true,
  "M2_front": false,
  "M3_identifiable": true,
  "V_triggered": false,
  "notes": "聚焦核心标的"
}
```

所有字段可选，同 `trade_date` 已存在时覆盖。

---

## GET /api/trades

查询交易记录。

### Parameters

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `trade_id` | string | 否 | 指定交易 ID |

不传 `trade_id` 返回全部交易记录。

### Response

```json
[
  {
    "trade_id": "uuid-v4",
    "ticker": "000001.SZ",
    "entry_date": "2026-05-10",
    "entry_price": 10.50,
    "path_type": "主升",
    "half_sell_trigger": 11.50,
    "half_sell_date": "2026-05-13",
    "half_sell_price": 11.55,
    "exit_date": null,
    "exit_price": null,
    "position_pct": 0.3,
    "notes": "第一笔建仓"
  }
]
```

### 字段说明

| 字段 | 类型 | 说明 |
|------|------|------|
| `trade_id` | string | 交易唯一 ID（UUIDv4） |
| `ticker` | string? | 股票代码 |
| `entry_date` | string? | 入场日期 |
| `entry_price` | float? | 入场价 |
| `path_type` | string? | 路径类型：主升/轮动/反抽/潜伏/打板 |
| `half_sell_trigger` | float? | 半卖触发价 |
| `half_sell_date` | string? | 半卖日期 |
| `half_sell_price` | float? | 半卖执行价 |
| `exit_date` | string? | 出场日期 |
| `exit_price` | float? | 出场价 |
| `position_pct` | float? | 仓位占比（0~1） |
| `notes` | string? | 备注 |

---

## POST /api/trades

创建新交易记录。自动生成 `trade_id`。

### Request Body

```json
{
  "ticker": "000001.SZ",
  "entry_date": "2026-05-10",
  "entry_price": 10.50,
  "path_type": "主升",
  "half_sell_trigger": 11.50,
  "notes": "第一笔建仓"
}
```

---

## PATCH /api/trades/{trade_id}

局部更新交易记录（常用半卖/平仓操作）。

### Request Body

```json
{
  "exit_date": "2026-05-14",
  "exit_price": 12.00,
  "half_sell_date": "2026-05-13",
  "half_sell_price": 11.55,
  "notes": "止盈出局"
}
```

### 半卖触发器

当 `half_sell_trigger` 非空且 `half_sell_price` 被填入时，系统自动将 `position_pct` 减半。

---

## GET /api/health

健康检查。

### Response

```
OK
```

---

## GET /api/stats

数据库统计概览。

### Response

```json
{
  "total_rows": 18049034,
  "trading_days": 8638,
  "stock_count": 5845,
  "date_range": ["1990-12-19", "2026-05-14"]
}
```
