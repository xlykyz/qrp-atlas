# Task04-C｜M6 市场情绪完整事实能力设计书 v0.1

> 状态：业务算法封板，可进入实现  
> 版本：v0.1  
> 封板日期：2026-09-04  
> 业务来源：`xlykyz/MyTradingSystem/docs/09_M5人气与M6市场情绪规格.md`  
> 业务来源版本：commit `65ab03a5f838ba6cdccbd1724ea6a2f1ffaa8d9b`

## 1. 任务定位

Task04-C 完成 M6｜市场情绪的事实层能力。

M6 是：

```text
全市场级公共环境指标
```

不是 Theme 独有属性，不参与本任务中的主线判断、评分或交易授权。

Task04-C 只负责：

```text
现有市场事实
→ 按 market_scope 聚合
→ 形成稳定、可查询、可审计、可复现的 M6 observations
```

M6 不需要新增 Provider 数据底座，不需要新增 Theme / StockCollection / Membership 身份体系，也不需要引入 M4/M5 的题材映射逻辑。

## 2. 正式输出范围

每个交易日 `D` 固定按以下五个市场范围形成 Observation：

```text
ALL_MARKET
MAIN_BOARD
CHINEXT
STAR_MARKET
BSE
```

含义：

- `ALL_MARKET`：全市场；
- `MAIN_BOARD`：沪深主板；
- `CHINEXT`：创业板；
- `STAR_MARKET`：科创板；
- `BSE`：北交所。

市场范围必须复用 QRP 已有 canonical 市场分类事实。

禁止：

- 为 M6 新建第二套市场身份；
- 仅凭股票代码前缀自行定义正式 market scope；
- 在 M6 内维护独立证券分类生命周期。

## 3. 正式输出字段

M6 v0.1 每个 `trade_date × market_scope` 输出：

```text
trade_date
market_scope
limit_up_count
limit_down_count
consecutive_limit_up_count
max_consecutive_limit_up_height
pre_limit_up_premium
```

建议正式持久化表：

```text
market_m6_observation
```

建议业务唯一键：

```text
(trade_date, market_scope)
```

建议算法版本：

```text
market_m6_observation@0.1.0
```

技术追踪字段、production run、input snapshot、created_at、查询审计与事务语义优先复用 Task04 已有正式模式，不在本设计中重新发明。

## 4. 输入事实

M6 v0.1 只消费现有 canonical / formal facts。

至少需要：

```text
D 日个股最终收盘涨停 / 跌停状态
D 日个股自然连板高度
个股正式市场分类
D-1 与 D 的收盘价
D-1 / D 实际交易日事实
```

当前 QRP 已存在可复用能力包括：

- `daily_market_snapshot.is_limit_up / is_limit_down`；
- `limit_step.consecutive_boards`；
- `stock_info` 等 canonical 市场分类；
- 既有 actual trading day / suspension / trading calendar 事实。

实现前应优先验证现有 `limit_step.consecutive_boards` 是否满足本设计的自然连板语义；满足则直接复用，不重复建设连板计算底座。

若现有字段语义不能满足，则也只允许基于已有收盘最终涨停 + actual trading day 事实确定性派生，不新增 Provider 或身份体系。

## 5. `limit_up_count`

定义：

```text
limit_up_count(D, market_scope)
=
D 日观察范围内收盘处于最终涨停状态的股票数量
```

只认收盘最终状态。

不统计：

- 盘中触板；
- 炸板；
- 收盘未封住涨停的股票。

## 6. `limit_down_count`

定义：

```text
limit_down_count(D, market_scope)
=
D 日观察范围内收盘处于最终跌停状态的股票数量
```

只认收盘最终跌停状态。

## 7. `consecutive_limit_up_count`

定义：

```text
consecutive_limit_up_count(D, market_scope)
=
D 日观察范围内当前连续收盘涨停天数 >= 2 的股票数量
```

自然连板连续性规则：

```text
按个股实际交易日计算
停牌日不参与
停牌日不打断连续性
只使用收盘最终涨停状态
```

M6 只读取 / 派生“当前自然连板高度”，不接管高度池生命周期。

## 8. `max_consecutive_limit_up_height`

定义：

```text
max_consecutive_limit_up_height(D, market_scope)
=
D 日观察范围内当前连续收盘涨停天数的最大值
```

若该 scope 当日不存在连板股票，则输出：

```text
0
```

该值是自然连板高度，不等同于高度池 `H(n)`、断板反包或其他生命周期身份。

## 9. `pre_limit_up_premium`

### 9.1 时间语义

正式字段：

```text
pre_limit_up_premium
```

它回答：

> 前一实际交易日最终涨停的股票，在观察日完整交易结束后获得了多少平均价格正反馈。

对观察日 `D`：

```text
D-1 最终涨停股票
        ↓
观察 D 日实际交易结果
        ↓
pre_limit_up_premium(D)
```

不使用 `D+1` 未来信息。

### 9.2 单股溢价

对 `D-1` 收盘最终涨停、且 `D` 日实际交易的股票 `i`：

```text
premium_i(D)
=
close_i(D) / close_i(D-1) - 1
```

### 9.3 market scope 聚合

```text
pre_limit_up_premium(D, market_scope)
=
AVG(premium_i(D))
```

采用股票等权平均。

### 9.4 正式边界

1. 样本必须是前一实际交易日 `D-1` 的收盘最终涨停股票；
2. 只纳入 `D` 日实际交易的样本；
3. `D-1` 涨停、`D` 停牌：不纳入分母；
4. `D` 日低开、跌停或负收益：正常计入，不剔除；
5. 不做成交额加权、市值加权；
6. 不 Winsorize，不人为截尾；
7. `ALL_MARKET` 直接对所有有效样本股票等权平均，不对四个子市场的 premium 二次等权平均；
8. 无有效样本时返回 `NULL`，不是 0。

因此：

```text
0
= 有有效样本，平均收益恰好为 0

NULL
= 没有可观察的昨日涨停样本
```

## 10. 五项指标的统一算法

```text
D 日最终市场事实
        +
D 日自然连板高度
        +
正式 market_scope
        +
D-1 最终涨停样本在 D 日的收盘表现
        ↓
按 trade_date × market_scope 聚合
        ↓
limit_up_count
limit_down_count
consecutive_limit_up_count
max_consecutive_limit_up_height
pre_limit_up_premium
```

其中：

```text
limit_up_count
= COUNT(final_limit_up = TRUE)

limit_down_count
= COUNT(final_limit_down = TRUE)

consecutive_limit_up_count
= COUNT(consecutive_limit_up_height >= 2)

max_consecutive_limit_up_height
= MAX(consecutive_limit_up_height), 无连板时 0

pre_limit_up_premium
= AVG(D-1 最终涨停且 D 实际交易股票的 D 日收盘收益率)
```

## 11. 输出完整性

正常交易日原则上应形成五个 market scope 的完整输出：

```text
ALL_MARKET
MAIN_BOARD
CHINEXT
STAR_MARKET
BSE
```

某 scope 没有涨停、跌停或连板时：

```text
count / height = 0
```

某 scope 没有可观察的昨日涨停样本时：

```text
pre_limit_up_premium = NULL
```

输入事实缺失或无法证明目标日完整时，不得把“缺失数据”解释成 0；生产路径应 fail-closed，不产生部分市场范围结果。

## 12. 历史与可复现语义

M6 是 D 日收盘后的派生事实。

要求：

- 使用观察日当时有效的正式市场分类与市场事实；
- 不因后续身份变化无条件重写已 finalized 的历史 M6；
- 显式 Replay / Audit 可按对应模式重建；
- 原始 Provider mirror 是否允许历史修订，继续遵循其自身 Contract，不由 M6 改写。

M6 不新增独立历史身份机制，直接复用 Task04 已有生产 / Replay / Audit 治理模式。

## 13. 工程实现原则

Task04-C 是薄聚合层。

应优先：

```text
set-based read
→ 一次生成 5 个 scope observations
→ 单事务原子落库
```

禁止：

- 每个 market_scope 单独打开数据库连接；
- 每只股票 N+1 查询；
- 为 M6 新建市场身份 resolver；
- 新增无必要的数据采集 Pipeline；
- 把高度池生命周期逻辑复制进 M6。

正式持久化能力应按 `src/qrp_atlas/AGENTS.md` 要求，先更新 Contracts SSOT，再实现独立正式 PipelineContract、查询与审计。

## 14. 明确非目标

M6 v0.1 不做：

```text
M6 综合总分
强弱分档
阈值
市场板块权重
历史标准化 / Z-Score
涨跌停比值
情绪周期标签
主线确认
系统 B 仓位调整
交易授权
额外 M6 指标扩展
```

Task04-C 结束在：

> 五项原始市场情绪事实已经稳定、可查询、可复现。

## 15. 最小验收标准

实现至少覆盖：

1. 同一交易日固定输出五个 `market_scope`；
2. 最终涨停 / 跌停计数正确；
3. 连板数量只统计自然连板高度 `>= 2`；
4. 最高连板高度正确，无连板时为 0；
5. 停牌不参与也不打断自然连板连续性；
6. `pre_limit_up_premium` 只使用 `D-1` 最终涨停股票；
7. `D` 日停牌样本从 premium 分母剔除；
8. 负收益 / 跌停样本正常计入 premium；
9. `ALL_MARKET` premium 按全部股票样本直接等权，不对子市场均值二次平均；
10. premium 无有效样本返回 `NULL`；
11. market scope 复用正式市场分类，不依赖自造代码前缀身份；
12. 缺失必要输入 fail-closed，不产生部分结果；
13. 重跑幂等；
14. Contract / schema / PK / 查询审计通过；
15. 全量回归测试通过。

## 16. 封板结论

M6 v0.1 业务设计已完整封板。

正式五项事实为：

```text
limit_up_count
limit_down_count
consecutive_limit_up_count
max_consecutive_limit_up_height
pre_limit_up_premium
```

其中 `pre_limit_up_premium` 已不再是待定候选指标，而是正式定义为：

> 前一实际交易日最终涨停股票，在观察日实际交易样本中的等权平均收盘收益率。

Task04-C 可以直接进入实现阶段，不需要再拆分数据底座子任务。
