# Task 04｜题材和市场结构事实

> 状态：工作包总览已建立  
> 当前顺序：M4 → M5 → M6 串行推进  
> 边界：只建设事实层与可复现数据能力，不进入评分、主线判断或交易授权。

## 1. 工作包定位

Task 04 对应 v1.1 正式路线图中的：

```text
04 题材和市场结构事实
```

目标是把题材、板块和市场结构所需的客观事实建设为可 PIT 查询、可历史回放、可生产运行、可审计的数据与指标能力。

Task 04 不负责：

- M1—M6 综合评分；
- M1—M3 最终身份映射；
- 主线题材确认；
- A/B/C 市场阶段判断；
- 新增仓授权；
- 组合、持仓或交易执行。

未批准的阈值、权重和综合公式不得由实现层自行补全。

## 2. 总体设计思路

Task 04 不再作为一个不可拆分的大任务推进，而是按三类事实能力串行收口：

```text
04-A M4｜板块效应完整事实能力
        ↓
04-B M5｜人气完整事实能力
        ↓
04-C M6｜市场情绪完整事实能力
```

三个子包共享的原则：

1. 先建设可复现的客观事实，再讨论阈值和评分；
2. 事实层与判断层严格分离；
3. 所有历史读取遵守 PIT；
4. 原始采集、标准化事实、派生指标和业务判断分层；
5. 不以未来才获得的信息回填历史决策输入；
6. 生产计算优先集合化处理，不采用逐成员 N+1 查询。

## 3. 04-A｜M4 板块效应完整事实能力

M4 当前业务定义已经足以推进事实层建设，具体阈值参数不构成阻塞。

最终目标是：

> 给定一个 Theme 及其 PIT 成员关系，QRP 能确定性重建任意历史日的有效成员，计算题材等权指数及其价格结构，并产出 M4 所需全部客观 observations。

M4 核心事实：

```text
theme_daily_return
theme_limit_up_count
theme_return_rank
```

当前不要求确定：

```text
return_threshold
limit_up_threshold
rank_threshold
```

在正式参数未批准前，M4 只生产原始 observations，不用默认阈值生成伪 `m4_qualified` 结果。

### 3.1 StockCollection 是 M4 的基础能力

M4 的核心前置不是新的评分公式，而是稳定回答：

> 某个 Theme 在指定 PIT 时点有哪些股票成员？

因此 Task 04 首先引入 StockCollection：

```text
StockCollection
= 股票集合的统一身份层
+ 统一 PIT 成员解析层
```

它不替代原始采集和既有股票池，也不把所有 membership 复制进万能大表。

Industry、Index、System Pool、Theme 等原生 membership 继续保留各自语义和物理 owner，通过 Adapter 接入统一 Resolver。

StockCollection 最终设计见：

```text
StockCollection_设计书_Final.md
```

### 3.2 Theme Membership 与 M4 计算资格分离

正式 invariant：

```text
M4 Calculation Eligibility
≠
Theme Membership
```

上市实际交易日数 `<= 5` 的新股以及观察日停牌股票：

- 可以正常属于 Theme；
- 不被自动调出 Theme；
- 只在该交易日不参与题材等权收益、涨跌幅聚合、涨停统计等 M4 计算；
- 新股从第 6 个实际交易日起自动具备计算资格；
- 停牌股复牌后自动恢复计算资格。

Task04-A 的 M4 成员纳入时点、有效成员资格、逐日事实化、规则版本锚点与 finalized 历史冻结语义，以以下正式规则文档为准：

```text
M4 Effective Member Rule v1.0.0.md
Rule ID: theme_m4_effective_member@1.0.0
```

该规则属于 Task04-A 正式生产语义基线；实现与验收不得以当前代码行为、运行时刻或聊天约定替代该规则。

### 3.3 M4 能力链

```text
Theme
→ THEME StockCollection
→ PIT Theme Membership
→ M4 Effective Member Filter
→ Equal-weight Theme Index
→ Theme Index State / Episode
→ M4 Raw Observations
```

M4 子包完成后，应具备完整历史回放、每日生产、查询和审计能力，但仍不进入主线判断。

## 4. 04-B｜M5 人气完整事实能力

M5 当前业务指标定义已经明确，主要阻塞来自数据基座而不是公式。

目标事实：

```text
theme_hot_stock_count
theme_hot_list_appearance_count
theme_hot_source_count
```

核心前置是扩展 Pipeline，生产可 PIT 使用的人气榜单快照，例如保存：

```text
trade_date
source
list_name
asset_id
rank_position
snapshot_time
source_record_id
ingested_at
```

随后将榜单股票映射到观察日有效的 Theme Membership，聚合得到 M5 observations。

当前不要求：

- 平台权重；
- 榜单名次权重；
- 综合总分；
- M5 成立阈值；
- 多日衰减或累计规则。

这些属于后续业务参数与评分层，不阻塞 M5 事实能力建设。

## 5. 04-C｜M6 市场情绪完整事实能力

M6 大部分底层数据已经存在，目标是把全市场及不同交易板块的短线结构形成稳定 observations。

基础范围：

```text
ALL_MARKET
MAIN_BOARD
CHINEXT
STAR_MARKET
BSE
```

核心事实：

```text
limit_up_count
limit_down_count
consecutive_limit_up_count
max_consecutive_limit_up_height
limit_up_premium
```

其中前四项可以基于现有市场、涨跌停和连板事实推进。

`limit_up_premium` 的最终观察口径尚未封板，可候选为次日集合竞价、开盘、收盘或最高价等口径；该单项未配置不得阻塞其余 M6 observations。

M6 当前同样不定义：

- 综合 M6 总分；
- 强弱阈值；
- 不同市场板块权重；
- M6 对主线或仓位的直接影响。

## 6. 三个子包之间的关系

三个子包严格串行：

```text
04-A M4
完成并满足退出条件
        ↓
04-B M5
完成并满足退出条件
        ↓
04-C M6
```

不以并行实现绕过前序尚未冻结的领域语义。

其中：

- M4 的关键基础设施是 StockCollection + PIT Theme Membership；
- M5 的关键基础设施是 Popularity Data Pipeline；
- M6 主要是现有市场事实的标准化派生与少量口径裁决。

## 7. Task 04 最终边界

Task 04 完成后，QRP 应拥有：

```text
动态 Theme / Membership 基础事实
Theme 等权指数
M4 Raw Observations
M5 Raw Observations
M6 Raw Observations
```

并保证：

```text
canonical fact
≠ proposal

raw observation
≠ score

M4 / M5 / M6
≠ mainline decision

market fact
≠ trading authorization
```

Task 04 的终点是“事实已经准备好”，而不是“交易决策已经完成”。
