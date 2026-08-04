# QRP v1.0 × Tushare Pro 数据调研报告

> **调研时间**：2026-07-12  
> **依据文档**：`docs/QRP产品蓝图v1.0/`（尤其 01 能力审计、02 产品终点、03 数据与模块扩充规划）  
> **官方入口**：[Tushare Pro 数据接口](https://tushare.pro/document/2)  
> **权限参考**：[关于权限](https://tushare.pro/document/1?doc_id=108)、[积分与频次](https://tushare.pro/document/1?doc_id=290)  
> **本机现状**：项目已通过 `qrp_atlas.config.tushare_client.get_tushare_pro()` 接入第三方网关（`.env.example` 标注 15000 积分版），并对公开方法做了约 0.6s 调用间隔限速。

---

## 1. 调研目标

把产品蓝图 v1.0 的数据层缺口，映射到 **Tushare Pro 可落地接口清单**，回答四件事：

1. **哪些蓝图表/能力可直接由 Tushare 覆盖**；  
2. **字段能否支撑 point-in-time（当时已可用）回测**；  
3. **积分、限量、VIP 接口对全市场历史回填的影响**；  
4. **建议的接入优先级与 contracts 设计要点**。

本报告只做数据调研与接入建议，不改 pipeline 实现，也不触碰本地 `data/` 与数据库文件。

---

## 2. 蓝图数据需求摘要

### 2.1 现有本地契约（已完成 / 可继续标准化）

| 本地表 | 用途 | 当前主要来源 |
|---|---|---|
| `daily_market_snapshot` | 日线 OHLCV、涨跌幅、量额 | `pro.daily`（主）/ 新浪兜底 |
| `daily_basic` | 估值、市值、换手、股本 | `pro.daily_basic` |
| `adj_factor_changes` | 复权因子 | `pro.adj_factor` |
| `stock_info` | `stock_basic` 完整当前快照及兼容字段 | `pro.stock_basic` |
| `trading_calendar` | 交易日 | 已有契约；Tushare 有 `trade_cal` |
| `index_daily` | 指数日线 | 契约已有；Tushare 有 `index_daily` |
| `zt_pool` / `dt_pool` | 涨跌停池 | 当前更偏东财/业务口径 |
| `suspend_d` | 停复牌 | `pro.suspend_d` |
| `research_report_*` / `cninfo_research_visits` | 研报与调研 | 东财等，非 Tushare |

### 2.2 蓝图明确要求新增的数据层

| 蓝图目标 | 建议契约 | 关键要求 |
|---|---|---|
| 财务三表 + 财务指标 | `income_statement` / `balance_sheet` / `cashflow_statement` / `financial_indicator` | 公告可用时间、修订版本、不覆盖历史 |
| 历史行业与成分 | `industry_membership_history` / `index_component_history` | `effective_from/to`、`published_at`、`available_trade_date` |
| 结构化事件 | `corporate_event`（统一入口） | 预告/快报/分红/回购/增减持/解禁等 |
| 统一时间语义 | 全研究表 | `published_at` / `available_trade_date` / `revision_id` / `source` |

### 2.3 v1.0 策略对数据的真实依赖

| 策略/能力 | 最低数据 | 更优数据 |
|---|---|---|
| 趋势/动量/突破/均值回归 | 日线 + 复权 + 日历 | 停牌、涨跌停价、成交量 |
| 横截面动量 / 多因子 long-only | 日线 + daily_basic（市值/估值/换手） | 行业归属、财务因子、指数成分池 |
| 事件漂移 | 公告时间 + 事件类型 | 预告/快报/财报/分红/增减持 |
| 残差/相对价值 | 个股与市场/行业收益 | 行业指数、成分权重、无风险利率 |
| A 股成交约束 | 停牌、涨跌停、T+1、整手 | 官方涨跌停价 `stk_limit` |

---

## 3. 本机 Tushare 接入现状

### 3.1 已落地接口

| 接口 | 代码位置 | 用途 |
|---|---|---|
| `daily` | `pipeline/daily_update/fetch.py` | 日线行情主源 |
| `daily_basic` | `pipeline/daily_basic/` + `scripts/backfill_daily_basic_*` | 每日估值/市值 |
| `suspend_d` | `pipeline/suspend_d/` | 停复牌 |
| `stock_basic` | `pipeline/stock_basic_contracts.py` | 完整股票当前状态快照 |
| `adj_factor` | `scripts/pull_adj_factor*.py` | 复权因子 |
| `index_basic` / `pro_bar` | `config/tushare_client.py` 自检 | 连通性验证 |

### 3.2 工程约束

- Token 仅来自 `.env` / 环境变量，不进仓库。  
- 自定义网关：`https://fastapic.stockai888.top`（`.env.example` 标注 15000 积分版，限速约 100 次/分钟）。  
- 客户端对 `pro` 公开方法做了统一限速；`ts.pro_bar(...)` 需显式 `api=pro`。  
- 字段映射已有 `TUSHARE_DAILY`；财务/事件类映射尚未进入 `contracts.mappings`。

**结论**：行情底座与 Tushare 已打通；蓝图缺口集中在 **财务、行业/成分历史、结构化事件、PIT 时间语义**。

---

## 4. Tushare Pro 能力总览（与 QRP 相关）

### 4.1 权限模型要点

1. **积分是门槛，不按次扣费**；积分越高，每分钟频次越高。  
2. 多数研究级接口门槛约 **2000 积分**；部分特色接口 **3000/5000/6000+**。  
3. 财务类常规接口常只能 **按股票循环**；全市场按报告期批量需 `*_vip`，通常 **5000 积分**。  
4. 分钟、Level-2、部分实时/特色数据常 **单独计费**，不在 v1.0 默认范围。  
5. 官方权限表未逐条列出的基础接口（如 `stock_basic`、`adj_factor`、`suspend_d`）仍在各接口页声明积分要求，落地前需用本机 token 实测。

### 4.2 与蓝图对齐的核心接口池

#### A. 基础与行情（底座，多数已覆盖）

| 接口 | 文档 | 最低积分（文档） | 限量/更新 | QRP 映射 |
|---|---|---:|---|---|
| `stock_basic` | [doc_id=25](https://tushare.pro/document/2?doc_id=25) | 2000 起 | 单次最多 6000 行，按状态和交易所分区 | `stock_info` |
| `stock_company` | [doc_id=112](https://tushare.pro/document/2?doc_id=112) | 120 | 单次约 4500 | 公司静态信息增强 |
| `namechange` | [doc_id=100](https://tushare.pro/document/2?doc_id=100) | 低门槛基础接口 | 历史曾用名 | 名称历史 / ST 名称追踪辅助 |
| `trade_cal` | [doc_id=26](https://tushare.pro/document/2?doc_id=26) | 2000 | 交易所日历 | `trading_calendar` |
| `daily` | [doc_id=27](https://tushare.pro/document/2?doc_id=27) | 120 起 | 单次 6000；15–17 点更新 | `daily_market_snapshot` 主源 |
| `adj_factor` | [doc_id=28](https://tushare.pro/document/2?doc_id=28) | 2000 起 | 按日或按票 | `adj_factor_changes` |
| `daily_basic` | [doc_id=32](https://tushare.pro/document/2?doc_id=32) | 2000 起 | 单次 6000；15–17 点 | `daily_basic` |
| `pro_bar` | [doc_id=109](https://tushare.pro/document/2?doc_id=109) / [146](https://tushare.pro/document/2?doc_id=146) | 2000 | SDK 动态复权 | 研究侧便捷复权；生产建议本地因子计算 |
| `weekly` / `monthly` | [144](https://tushare.pro/document/2?doc_id=144) / [145](https://tushare.pro/document/2?doc_id=145) | 2000 | 周/月线 | 非 v1.0 必需，可由日线聚合 |
| `suspend_d` | [doc_id=214](https://tushare.pro/document/2?doc_id=214) | 文档页未强调门槛 | 按日/区间 | `suspend_d` |
| `stk_limit` | [doc_id=183](https://tushare.pro/document/2?doc_id=183) | 2000 起 | 交易日约 9 点 | 官方涨跌停价；可校正本地规则派生 |
| `limit_list_d` | [doc_id=298](https://tushare.pro/document/2?doc_id=298) | 约 5000 | 2020 起，不含 ST | 可补 `zt_pool`/`dt_pool` 研究口径 |

#### B. 财务与基本面（蓝图 2.2 主缺口）

| 接口 | 文档 | 最低积分 | 关键时间字段 | 建议本地表 |
|---|---|---:|---|---|
| `income` / `income_vip` | [33](https://tushare.pro/document/2?doc_id=33) | 2000 / VIP 5000 | `ann_date`, `f_ann_date`, `end_date`, `update_flag` | `income_statement` |
| `balancesheet` / `*_vip` | [36](https://tushare.pro/document/2?doc_id=36) | 2000 / 5000 | 同上 | `balance_sheet` |
| `cashflow` / `*_vip` | [44](https://tushare.pro/document/2?doc_id=44) | 2000 / 5000 | 同上 | `cashflow_statement` |
| `fina_indicator` / `*_vip` | [79](https://tushare.pro/document/2?doc_id=79) | 2000 / 5000 | `ann_date`, `end_date` | `financial_indicator` |
| `fina_audit` | [80](https://tushare.pro/document/2?doc_id=80) | 2000 | 公告相关 | 质量过滤增强 |
| `fina_mainbz` | [81](https://tushare.pro/document/2?doc_id=81) | 2000 | 随财报 | 业务结构研究（非 v1.0 阻塞） |
| `disclosure_date` | [162](https://tushare.pro/document/2?doc_id=162) | 2000 | `pre_date`, `actual_date`, `ann_date` | 披露计划 / 事件日历 |

**PIT 关键结论（财务）**

- 可用性应优先用 **`f_ann_date`（实际公告日）**，其次 `ann_date`；**绝不能用 `end_date`（报告期）当可用日**。  
- `update_flag` / 多次公告必须保留 **revision**，禁止用最新修订覆盖旧版本。  
- TTM、同比、增速等建议在 `indicators` 计算，不在 pipeline 固化多套口径。  
- 全历史全市场回填：优先 `*_vip`；若只有 2000 积分，只能按 `ts_code` 慢速循环，工期显著更长。

#### C. 行业 / 指数成分（蓝图 2.3）

| 接口 | 文档 | 积分 | 历史能力 | 建议本地表 |
|---|---|---:|---|---|
| `index_classify` | [181](https://tushare.pro/document/2?doc_id=181) | 2000 | 申万分类字典 | 分类维表 |
| `index_member_all` | [335](https://tushare.pro/document/2?doc_id=335) | 2000 | `in_date`/`out_date`/`is_new`，支持三级 | `industry_membership_history`（申万） |
| `sw_daily` | [327](https://tushare.pro/document/2?doc_id=327) | 视网关权限 | 申万行业日线 | 行业基准收益 |
| `index_basic` | [94](https://tushare.pro/document/2?doc_id=94) | 2000 | 指数主数据 | 指数字典 |
| `index_daily` | [95](https://tushare.pro/document/2?doc_id=95) | 2000 起 | 指数行情 | `index_daily` |
| `index_weight` | [96](https://tushare.pro/document/2?doc_id=96) | 2000 | **月度**成分权重 | `index_component_history` |
| `index_dailybasic` | [128](https://tushare.pro/document/2?doc_id=128) | 4000 起 | 少量宽基每日指标 | 市场估值背景 |
| `ths_index` / `ths_member` | [259](https://tushare.pro/document/2?doc_id=259) / [261](https://tushare.pro/document/2?doc_id=261) | 约 6000 | 概念板块；**in/out 日期字段文档标注“暂无”** | 概念研究增强，不适合严格历史成分回测 |

**PIT 关键结论（行业/成分）**

- **申万行业历史**：`index_member_all` 是 v1.0 最优先选择。  
- **指数成分权重**：`index_weight` 为月度，足够做月频调仓股票池/暴露；日频精确权重不够。  
- **同花顺概念成分**：当前缺少可靠纳入/剔除日期，**不能**当作严格 point-in-time 股票池。

#### D. 结构化事件（蓝图 2.4）

| 事件类型 | Tushare 接口 | 文档 | 积分 | 关键时间字段 | 映射建议 |
|---|---|---|---:|---|---|
| 业绩预告 | `forecast` / `forecast_vip` | [45](https://tushare.pro/document/2?doc_id=45) | 2000 / 5000 | `ann_date`, `first_ann_date`, `end_date` | `event_type=earnings_forecast` |
| 业绩快报 | `express` / `express_vip` | [46](https://tushare.pro/document/2?doc_id=46) | 2000 / 5000 | `ann_date`, `end_date` | `earnings_express` |
| 财报披露 | 财务三表 + `disclosure_date` | [33](https://tushare.pro/document/2?doc_id=33) 等 | 2000+ | `f_ann_date`/`actual_date` | `earnings_report` |
| 分红送股 | `dividend` | [103](https://tushare.pro/document/2?doc_id=103) | 2000 | `ann_date`, `ex_date`, `div_proc` | `dividend` |
| 股票回购 | `repurchase` | [124](https://tushare.pro/document/2?doc_id=124) | 2000 | 公告/进度相关字段 | `repurchase` |
| 增减持 | `stk_holdertrade` | [175](https://tushare.pro/document/2?doc_id=175) | 2000 | 公告日 | `holder_increase` / `holder_decrease` |
| 限售解禁 | `share_float` | [160](https://tushare.pro/document/2?doc_id=160) | 3000 | `float_date`, `ann_date` | `share_unlock` |
| 股东人数 | `stk_holdernumber` | [166](https://tushare.pro/document/2?doc_id=166) | 600–2000 档 | `ann_date`, `end_date` | 事件或截面特征 |
| 十大股东 | `top10_holders` / `top10_floatholders` | [61](https://tushare.pro/document/2?doc_id=61) / [62](https://tushare.pro/document/2?doc_id=62) | 2000 | `ann_date`, `end_date` | 持仓结构研究 |
| 大宗交易 | `block_trade` | [161](https://tushare.pro/document/2?doc_id=161) | 2000 | 交易日 | 可选事件 |
| 龙虎榜 | `top_list` / `top_inst` | [106](https://tushare.pro/document/2?doc_id=106) / [107](https://tushare.pro/document/2?doc_id=107) | 2000 / 5000 | `trade_date` | 短线事件增强 |
| 并购/问询/重大合同 | **无稳定标准接口** | — | — | — | 需公告/NLP/其他源 |

**PIT 关键结论（事件）**

- 事件研究默认：`published_at = ann_date/first_ann_date`（按接口选最“首次可见”字段），  
  `available_trade_date = 公告日后的下一交易日`（盘中公告需更保守规则）。  
- 分红事件要区分 **公告日** 与 **除权除息日**；策略信号用公告日，价格调整用除权日 + `adj_factor`。  
- 蓝图中的 **并购重组、重大合同、监管问询**：Tushare 标准接口覆盖弱，v1.0 若要做应另选巨潮公告/NLP 源，不要假装“已有结构化事件”。

#### E. 增强但非 v1.0 阻塞

| 接口 | 用途 | 积分/备注 | 蓝图位置 |
|---|---|---|---|
| `moneyflow` / `moneyflow_dc` / `moneyflow_ths` | 资金流因子 | 2000 起，特色更高 | 增强因子 |
| `margin` / `margin_detail` | 融资融券 | 2000 | 相对价值/杠杆研究增强 |
| `shibor` / `shibor_lpr` | 无风险利率近似 | 120–2000 | 相对价值增强 |
| `fund_basic` / `fund_daily` / `fund_nav` | ETF/基金 | 2000+ | v1.0 后增强 |
| `fut_daily` / `opt_daily` | 期现/期权 | 2000–5000+ | 明确增强/排除边界 |
| 分钟/实时/新闻/公告 PDF 库 | 高频与文本 | 常单独计费 | v1.0 排除或延后 |

---

## 5. 蓝图缺口 × Tushare 覆盖矩阵

| 蓝图缺口 | 覆盖度 | 推荐接口 | 可用性/版本字段 | 风险 |
|---|---|---|---|---|
| 利润表 | ✅ 高 | `income(_vip)` | `f_ann_date`/`ann_date`/`update_flag` | 非 VIP 回填慢；金融行业科目差异大 |
| 资产负债表 | ✅ 高 | `balancesheet(_vip)` | 同上 | 同上 |
| 现金流量表 | ✅ 高 | `cashflow(_vip)` | 同上 | 同上 |
| 财务指标 | ✅ 高 | `fina_indicator(_vip)` | `ann_date`/`end_date` | 指标口径需版本化 |
| 历史行业归属 | ✅ 中高 | `index_member_all` + `index_classify` | `in_date`/`out_date` | 仅申万等分类体系；需本地 SCD2 化 |
| 历史指数成分 | 🟡 中 | `index_weight` | 月度 `trade_date` | 非日频；宽基/主题覆盖需核对 |
| 业绩预告/快报 | ✅ 高 | `forecast`/`express`(+vip) | `ann_date`/`first_ann_date` | 修订与多次预告 |
| 分红 | ✅ 高 | `dividend` | `ann_date`/`ex_date` | 进度状态过滤 |
| 回购 | ✅ 中高 | `repurchase` | 公告/进度 | 字段粒度需实测 |
| 增减持 | ✅ 中高 | `stk_holdertrade` | 公告日 | 需清洗方向与比例 |
| 解禁 | ✅ 中高 | `share_float` | `float_date`/`ann_date` | 门槛 3000 |
| 并购/问询/重大合同 | ⛔ 低 | — | — | 需公告文本或其他源 |
| 统一 PIT 字段 | 🟡 需本地加工 | 全接口组合 | 本地生成 `available_trade_date` 等 | Tushare 不直接给 QRP 语义字段 |
| 无风险利率/借券融资成本 | 🟡 部分 | `shibor`/`margin*` | 市场级 | 个券借券成本不足 |
| 概念板块严格历史 | ⛔/🟡 弱 | `ths_member` | in/out 暂无 | 不可直接做严格回测池 |

图例：✅ 可主用；🟡 可用但有口径限制；⛔ 难支撑 v1.0 严格定义。

---

## 6. 推荐接入优先级（服务 v1.0 终点）

### P0 — 直接服务发布阻塞项

目标：让 **财务/事件回测可证明当时可用**，并支撑横截面与事件演示场景。

1. **财务四表契约 + 拉取**  
   `income` / `balancesheet` / `cashflow` / `fina_indicator`  
   - contracts 先立表，不先写策略。  
   - 主键建议：`(ticker, report_period, statement_type, ann_date, revision_id)` 或等价。  
   - 强制保留 `ann_date`/`f_ann_date`/`update_flag`/`source_record_id`。  
2. **事件主表 `corporate_event` 首批类型**  
   `forecast`、`express`、财报披露（由财务 ann 派生）、`dividend`、`stk_holdertrade`、`share_float`、`repurchase`。  
3. **行业历史**  
   `index_classify` + `index_member_all` → `industry_membership_history`。  
4. **指数成分历史（月频）**  
   `index_weight` → `index_component_history`（先覆盖沪深300/中证500/中证1000/创业板指等）。  
5. **成交约束增强**  
   已有 `suspend_d`；补 `stk_limit` 作官方涨跌停价校验源。

### P1 — 明显提升研究密度，但不阻塞最小闭环

- `sw_daily`：行业基准与残差。  
- `disclosure_date`：财报日程与事件窗。  
- `top10_holders` / `stk_holdernumber`：拥挤度与筹码。  
- `moneyflow`：流动性/资金流因子。  
- `limit_list_d`：涨跌停行为研究（注意 2020 起、不含 ST）。  
- `shibor`：简单无风险利率。

### P2 — v1.0 后 / 增强项

- ETF/基金、期货期权、港美股、分钟线、新闻公告库、同花顺/开盘啦题材库。  
- 复杂 long-short 中性、多腿、借券成本精确建模。

---

## 7. 本地建模建议（对齐蓝图，不新增顶级模块）

### 7.1 统一时间语义（所有新表强制）

| 字段 | 生成规则（建议） |
|---|---|
| `source` | 固定 `tushare` |
| `source_record_id` | 接口名 + 业务主键 hash/拼接 |
| `published_at` | 优先 `f_ann_date`/`first_ann_date`/`ann_date` 00:00+08:00（无时刻时） |
| `effective_date` | 报告期 `end_date`、解禁 `float_date`、除权 `ex_date` 等 |
| `available_trade_date` | `trade_cal` 上，严格大于 `published_at` 对应日期的下一交易日（可配置“当日收盘后可用”） |
| `revision_id` | `update_flag` + 公告日 + 抓取序号 |
| `ingested_at` | 入库时间 |

> 蓝图已强调：`created_at` 不能替代 `published_at` / `available_trade_date`。

### 7.2 财务表最小字段集

除金额科目外，至少：

```text
ticker
report_period          # end_date
report_type
comp_type
ann_date
f_ann_date
published_at
available_trade_date
update_flag
revision_id
source
source_record_id
ingested_at
```

### 7.3 事件表最小字段集

```text
event_id
ticker
event_type
announcement_title     # 可空
published_at
effective_date
available_trade_date
source
source_record_id
payload_json           # 原始关键字段
revision_id
ingested_at
```

专项结构化表可后补，但 **事件回测入口应统一走 `corporate_event`**。

### 7.4 行业/成分历史

```text
# industry_membership_history
ticker, classification_system, industry_code, industry_name,
level, effective_from, effective_to, is_current,
published_at, available_trade_date, source, source_record_id

# index_component_history
index_code, ticker, trade_date_or_month, weight,
effective_from, effective_to, source, source_record_id
```

对 `index_weight` 的月度快照，本地可展开为“当月生效区间”，并在文档中标明 **月频近似**。

### 7.5 指标层边界

- Tushare 给原始/官方衍生字段；  
- QRP `indicators` 负责：TTM、YoY、z-score、行业中性、事件年龄、PEAD 窗收益等；  
- 避免在 pipeline 中散落多套同义财务衍生列。

---

## 8. 工程落地注意事项

### 8.1 拉取策略

| 数据类型 | 推荐抽取方式 | 原因 |
|---|---|---|
| 日线 / daily_basic / adj_factor / suspend / stk_limit | **按 trade_date 全市场** | 官方也建议按日期循环，而非按票扫历史 |
| 财务三表 / fina_indicator / forecast / express | **有 VIP 按 period；无 VIP 按 ts_code** | 接口限流与权限结构决定 |
| index_weight | 按月 + index_code | 月度数据 |
| index_member_all | 按行业或按票增量 | 单次 2000 行 |
| 事件增量 | 按 `ann_date` 窗口 | 便于日更 |

### 8.2 单位与口径陷阱

| 项目 | Tushare 常见口径 | 本地现状/建议 |
|---|---|---|
| `daily.vol` | 手 | 现有 daily_update 已转 **股** |
| `daily.amount` | 千元 | 现有清洗转 **元** |
| 财务金额 | 元（部分预告净利润为万元） | 入库前统一，并在 payload 保留原单位 |
| 复权 | `pro_bar` 动态复权依赖 `end_date` | 生产库存 **未复权价 + adj_factor**，研究时再算 |
| ST/涨跌停 | `limit_list_d` 不含 ST | 继续保留本地规则与 `zt_pool`/`dt_pool` 业务口径 |
| 名称 | 曾用名独立接口 | 不要假设 `stock_basic.name` 可回溯历史 |

### 8.3 限速与回填工期（粗估）

假设网关稳定 **100 次/分钟**（与本机客户端 0.6s 间隔一致）：

| 任务 | 粗估调用量 | 粗估耗时 |
|---|---:|---:|
| 1 年 daily 按日 | ~250 次 | 数分钟 |
| 10 年 daily | ~2500 次 | <1 小时 |
| 财务四表全市场（无 VIP，按票） | 约 5000×4 次量级 | 数小时到一天+（含失败重试） |
| 财务四表（VIP 按季） | 约 4 表 × 报告期数 | 显著更快 |
| 事件首批 5 类近 10 年 | 视窗口分片 | 通常小时级 |

> 以上为数量级估算，实际受网关稳定性、积分频次、空窗重试影响。回填脚本必须可断点续跑。

### 8.4 与现有 contracts 的兼容原则

1. **不推翻** 已有 `daily_market_snapshot` / `daily_basic` 主链路。  
2. 新表先进入 `contracts`（fields/schema/validate/mappings），再写 pipeline。  
3. 映射层新增 `TUSHARE_INCOME` 等，而不是在 fetch 里散落 rename。  
4. raw 层保留 Tushare 原始 JSON/Parquet，canonical 层只保留研究字段。  
5. 任何“当时是否可交易/可用”的规则进 `backtest` 数据准备，不进策略代码。

---

## 9. 本机积分与网关核验清单（实施前必做）

在开始大规模回填前，用本机 token 实测：

1. `stock_basic` / `trade_cal` / `daily` / `daily_basic` / `adj_factor`  
2. `income` 单票、`income_vip` 单期（若有权限）  
3. `forecast` / `express` / `dividend` / `stk_holdertrade` / `share_float` / `repurchase`  
4. `index_member_all` / `index_weight` / `index_daily` / `sw_daily`  
5. `suspend_d` / `stk_limit` / `limit_list_d`  
6. 记录：是否 401/权限不足、单次行数上限、是否被代理裁剪字段  

若网关宣称 15000 积分但仍无 `*_vip`，需以实测为准，不把宣传积分等同官方权限表。

---

## 10. 结论

### 10.1 总判断

**Tushare Pro 足以支撑 QRP v1.0 数据层的主路径**：

- 财务三表 + 财务指标；  
- 申万行业历史成分；  
- 宽基指数月度成分权重；  
- 业绩预告/快报/分红/回购/增减持/解禁等标准事件；  
- 对现有日线、估值、停牌、复权体系的补强。

它 **不足以单独覆盖** 蓝图中全部信息源：

- 并购/问询/重大合同等非标事件；  
- 严格日频指数成分；  
- 可靠的概念板块历史纳入剔除；  
- 精确借券/融券个券成本；  
- 高频订单簿与实时微观结构（亦非 v1.0 目标）。

### 10.2 对产品蓝图的直接建议

1. **数据源主策略**：A 股量价/财务/标准事件/行业历史以 Tushare 为主；研报与互动继续现有东财/全景等源。  
2. **先 contracts 后 pipeline**：按 P0 表清单扩契约，再写 fetch/clean/load。  
3. **PIT 是第一验收标准**：没有 `available_trade_date` 与修订保留，不算完成财务/事件数据。  
4. **VIP 权限影响工期**：有 `*_vip` 再做全市场财务历史；否则先小股票池打通事件策略闭环。  
5. **不要用同花顺概念成分做 v1.0 严格回测池**；行业中性优先申万历史。  
6. **保持架构边界**：原始字段进 contracts/pipeline，衍生因子进 indicators，策略只消费声明输入。

### 10.3 建议的下一步任务包（供 Agent 领取）

1. 在 `contracts` 增加财务四表 + `corporate_event` + 行业/成分历史表与字段常量。  
2. 扩展 `mappings.py`：Tushare 财务/事件/行业映射。  
3. 新增 pipeline 骨架：`pipeline/finance_*`、`pipeline/events_*`、`pipeline/industry_*`。  
4. 写 token 权限探针脚本，输出“本机可用接口矩阵”。  
5. 用 1 个指数成分池 + 近 3 年财务/预告，打通 `event_drift_basic` 数据前置条件。  

---

## 11. 参考链接

- 接口总览：https://tushare.pro/document/2  
- 权限说明：https://tushare.pro/document/1?doc_id=108  
- 积分与频次：https://tushare.pro/document/1?doc_id=290  
- 日线：https://tushare.pro/document/2?doc_id=27  
- 每日指标：https://tushare.pro/document/2?doc_id=32  
- 复权因子：https://tushare.pro/document/2?doc_id=28  
- 利润表：https://tushare.pro/document/2?doc_id=33  
- 资产负债表：https://tushare.pro/document/2?doc_id=36  
- 现金流量表：https://tushare.pro/document/2?doc_id=44  
- 财务指标：https://tushare.pro/document/2?doc_id=79  
- 业绩预告/快报：https://tushare.pro/document/2?doc_id=45 / https://tushare.pro/document/2?doc_id=46  
- 分红：https://tushare.pro/document/2?doc_id=103  
- 解禁：https://tushare.pro/document/2?doc_id=160  
- 增减持：https://tushare.pro/document/2?doc_id=175  
- 申万分类/成分：https://tushare.pro/document/2?doc_id=181 / https://tushare.pro/document/2?doc_id=335  
- 指数权重：https://tushare.pro/document/2?doc_id=96  
- 停复牌：https://tushare.pro/document/2?doc_id=214  
- 涨跌停价：https://tushare.pro/document/2?doc_id=183  

---

## 12. 文档关系

| 文档 | 关系 |
|---|---|
| `docs/QRP产品蓝图v1.0/03_QRP_v1.0_数据与模块扩充规划.md` | 需求来源 |
| `docs/QRP产品蓝图v1.0/01_QRP_v1.0_当前能力审计与能力矩阵.md` | 现状缺口 |
| `src/qrp_atlas/contracts/*` | 落地契约目标 |
| `src/qrp_atlas/config/tushare_client.py` | 本机接入点 |
| 本报告 | Tushare 侧可行性与优先级 |

**一句话**：QRP v1.0 缺的不是“再找一个行情源”，而是把 Tushare 的财务、行业历史与标准事件 **按 point-in-time 契约** 接进现有 `contracts → pipeline → indicators → backtest` 链路。
