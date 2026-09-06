# Task06 System B 横截面相对评分与排名设计书 v0.1

## 1. 产品身份

```text
semantic_owner = SYSTEM_B
delivery_mode = BUILTIN
capability_type = STRATEGY_COMPONENT
runtime = QRP_COMMON
persistence = QRP_COMMON
```

Task06 是 System B 的横截面相对比较组件。

它不定义 QRP Core 自身的股票评价标准，不产生 ENTER / HOLD / EXIT，不接管仓位，不参与 Task05 新增仓授权。

Task06 的首要目标是建立一套稳定、可解释、可回放的相对比较语言：

```text
事实
→ eligibility
→ raw metric
→ normalized rank score
→ dimension score
→ rank snapshot
```

本设计书将已冻结规则与仍待后续封板的规则明确分开，避免 Agent 为了完成实现自行补齐未冻结语义。

---

## 2. Task06 当前结构

Task06 分两层：

```text
Task06-A  Asset Relative Ranking
Task06-B  Theme Relative Ranking
```

### 2.1 Task06-A

回答：

> 指定交易日，一只股票在 System B 的 M1 / M2 / M3 三个评价维度上，相对当前有效评价域中的其他股票处于什么位置？

M1 / M2 / M3 不再是三类股票身份，也不绑定题材。

canonical identity：

```text
trade_date
ticker
```

同一 ticker 在同一 trade_date 只有一组：

```text
M1
M2
M3
```

题材归属不会改变该股票本身的 M1 / M2 / M3。

### 2.2 Task06-B

回答：

> 指定交易日，当前有效题材之间的相对强弱如何？

Task06-B 会读取 PIT Theme Membership、Task06-A 的股票级结果以及 M4 / M5 等题材事实。

当前仅冻结：

```text
M6 = OUT OF SCOPE
```

以及：

```text
Theme Rank 必须是独立于股票 M1/M2/M3 的上一级横截面比较体系
```

Theme Rank 的具体 component、权重与最终公式尚未完成业务封板。

因此 Task06 v0.1 实现阶段不得自行发明 Theme Rank 权重或公式；可先定义接口、输入契约、结果占位与 `NOT_CONFIGURED` 状态，待业务规则单独封板后实现计算。

---

## 3. M1 / M2 / M3 核心语义

M1 / M2 / M3 是某交易日任意 ticker 独立的三个评价维度。

它们的前置计算依赖只来自：

```text
历史行情数据
System B 状态机事实
System B Episode / Segment 事实
容量池 / 高度池 / 辨识度池事实
ticker 级人气热榜事实（仅 M3）
```

明确不依赖：

```text
theme_id
PIT Theme Membership
M4
M5 Theme aggregation
M6
market_phase
V_triggered
new_position_authorized
账户状态
已有持仓
未来收益
```

三池决定 eligibility：

```text
ticker ∉ Capacity Pool     → M1 = null
ticker ∉ Height Pool       → M2 = null
ticker ∉ Recognition Pool  → M3 = null
```

因此：

```text
(null, null, null)
```

是完全合法的股票日级结果。

---

## 4. 统一相对评分语言

### 4.1 命名

Task06 不使用含义可能不统一的 `percentile` 作为 canonical 算法名称。

统一命名：

```text
normalized_rank_score
```

### 4.2 原始 rank

对同一 trade_date、同一评价 universe 内的有效对象，默认方向：

```text
HIGHER_IS_BETTER
```

完全相同的 raw value 必须获得相同业务 rank。

推荐使用 average rank 处理 tie；稳定序列化可以额外使用 ticker / identity key 排序，但不得因此改变业务 rank。

### 4.3 normalized_rank_score

设：

```text
N = 有效比较对象数量
rank = 业务排名，1 为最强
```

当：

```text
N >= 2
且横截面存在有效差异
```

定义：

```text
normalized_rank_score
= 100 × (N - rank) / (N - 1)
```

因此：

```text
第一名 = 100
最后一名 = 0
中间位置线性映射到 0~100
```

`N - 1` 表示第一名到最后一名之间共有 `N - 1` 个排名间隔。

### 4.4 特殊情况

```text
N = 0
→ 不产生有效 score
```

```text
N = 1
→ rank = 1
→ normalized_rank_score = null
→ status = INSUFFICIENT_UNIVERSE
```

如果：

```text
N >= 2
但所有有效 raw value 完全相同
```

则：

```text
normalized_rank_score = null
status = NO_VARIATION
```

禁止将“没有横截面比较信息”解释为 100 分。

---

## 5. Raw / Rank / Score 分层

Task06 必须保留可追溯链路：

```text
raw_value
→ raw_rank
→ normalized_rank_score
→ dimension_raw
→ final_dimension_rank
→ final_dimension_score
```

Rank / Score 不得替代原始事实。

任何未来算法替换都应尽量复用同一 raw fact，而不是重写底层历史事实。

---

# 6. M1 算法 —— 容量核心强度

## 6.1 Eligibility Universe

```text
当日 Capacity Pool 中 membership_state = IN_POOL 的全部 ticker
```

未入池：

```text
M1 = null
M1_status = NOT_ELIGIBLE
```

## 6.2 Component 1：Episode 涨幅

使用现有：

```text
episode_return
```

注意：M1 不使用“容量池入池以来涨幅”。

原因：M1 评价的是股票在当前 System B 行情轮次中的综合容量核心强度，不应由股票何时恰好满足容量池门槛来重新定义价格表现起点。

在当日 Capacity Pool 内计算：

```text
m1_episode_rank_score
= normalized_rank_score(episode_return)
```

## 6.3 Component 2：5 日平均成交额

复用容量池现有事实：

```text
avg5_amount
```

在当日 Capacity Pool 内计算：

```text
m1_avg5_amount_rank_score
= normalized_rank_score(avg5_amount)
```

## 6.4 M1 合成

已冻结权重：

```text
M1 episode component     = 0.5
M1 avg5 amount component = 0.5
```

中间值：

```text
m1_raw
= 0.5 × m1_episode_rank_score
+ 0.5 × m1_avg5_amount_rank_score
```

两个 component 均有效时才形成 `m1_raw`。

任一必需 component 不可计算：

```text
M1 = null
M1_status = INCOMPLETE_COMPONENTS
```

最终在当日 Capacity Pool 内，对 `m1_raw` 再做一次 normalized ranking：

```text
M1
= normalized_rank_score(m1_raw)
```

最终 M1 语义：

> 该股票在当日所有有效容量池成员中的综合相对强度位置。

---

# 7. M2 算法 —— 高度空间强度

## 7.1 Eligibility Universe

```text
当日 Height Pool 中 membership_state = IN_POOL 的全部 ticker
```

未入池：

```text
M2 = null
M2_status = NOT_ELIGIBLE
```

## 7.2 核心字段扩展

当前高度池已有：

```text
height_start_date
height_admitted_date
height_type
n
m
i
current_break_days
```

Task06 要求将以下字段提升为高度池核心事实，而不是在评分层临时重算：

```text
height_start_base_close
height_since_start_return
```

### height_start_base_close

定义为：

```text
height_start_date 前一个实际交易日的前复权收盘价
```

### height_since_start_return

定义：

```text
height_since_start_return(t)
= close(t) / height_start_base_close - 1
```

因此：

> `height_start_date` 当天的涨幅必须计入高度结构累计涨幅。

不得使用：

```text
close(t) / close(height_start_date) - 1
```

因为该公式会把 `height_start_date` 当天涨幅排除在外。

## 7.3 M2 Score

M2 v0.1 只使用单一 component：

```text
height_since_start_return
```

在当日 Height Pool 内：

```text
M2
= normalized_rank_score(height_since_start_return)
```

当前 `H(n)` / `H(n,m,i)` / `current_break_days` 等继续作为高度结构事实和 evidence 保存，但 v0.1 不给这些字段自行设置经验权重。

最终 M2 语义：

> 该股票在当前有效高度结构中，从高度结构真实起点（含起点当日涨幅）累计形成的相对价格空间强度。

---

# 8. M3 算法 —— 辨识度强度

## 8.1 Eligibility Universe

```text
当日 Recognition Pool 中 membership_state = IN_POOL 的全部 ticker
```

未入池：

```text
M3 = null
M3_status = NOT_ELIGIBLE
```

Recognition Pool 的入池和粘性保留语义继续由 Task03 / 当前 pool evaluator 负责。

Task06 不重新定义 Recognition Pool。

## 8.2 Price Components

在当日 Recognition Pool 内分别计算：

```text
m3_episode_rank_score
= normalized_rank_score(episode_return)
```

```text
m3_return5_rank_score
= normalized_rank_score(return5)
```

```text
m3_return10_rank_score
= normalized_rank_score(return10)
```

## 8.3 Popularity Component

ticker 级人气数据来自现有：

```text
dc_hot
ths_hot
```

两个平台均保留：

```text
trade_date
ticker
rank_position
snapshot_seq
source_rank_time
```

Task06 人气分采用两级聚合：

```text
snapshot rank
→ snapshot normalized score
→ platform daily score
→ cross-platform hot score
```

### 8.3.1 单次 snapshot score

对某平台某次成功采集的 snapshot：

```text
N = 该 snapshot 榜单有效排名位置数量
```

某 ticker 上榜时：

```text
snapshot_hot_score
= 100 × (N - rank_position) / (N - 1)
```

当：

```text
N = 1
```

则：

```text
snapshot_hot_score = null
```

### 8.3.2 成功采集但 ticker 未上榜

若某平台某次 snapshot 已可靠采集成功，但 ticker 未出现在该榜单：

```text
snapshot_hot_score = 0
```

这里：

```text
0
= 平台正常观察到市场，但该 ticker 未进入榜单
```

不同于：

```text
null
= 该平台该日 / 该次 snapshot 没有可靠可用数据
```

### 8.3.3 单平台日级分

同一平台一天可能存在多次有效快照。

先分别标准化每次 snapshot，再按日取均值：

```text
dc_hot_score
= AVG(all valid dc snapshot_hot_score)
```

```text
ths_hot_score
= AVG(all valid ths snapshot_hot_score)
```

禁止：

```text
先 AVG(rank_position) 再做 normalized ranking
```

因为不同 snapshot 榜单长度可能不同。

日内 AVG 的业务语义：

```text
排名高度
+
日内持续时间
→ 平台日级关注度
```

v0.1 不使用单日 MAX，也不只使用收盘最后一次 snapshot。

### 8.3.4 双平台合成

已冻结：

```text
hot_rank_score
= AVG(dc_hot_score, ths_hot_score)
```

即：

```text
DC weight  = 0.5
THS weight = 0.5
```

不使用 MAX。

## 8.4 外部热榜缺失的生产降级语义

这一条属于 Task06 v0.1 硬性生产语义。

外部平台当天整体不可用属于：

```text
业务数据缺失 / expected degradation
```

不是：

```text
系统计算异常
```

建议 source 状态至少区分：

```text
AVAILABLE
UNAVAILABLE
```

三类 ticker 级语义：

```text
平台当天正常 + ticker 上榜
→ platform_hot_score = 0~100
```

```text
平台当天正常 + ticker 未上榜
→ platform_hot_score = 0
```

```text
平台当天整体不可用
→ platform_hot_score = null
```

若：

```text
dc_hot_score = null
OR
ths_hot_score = null
```

则：

```text
hot_rank_score = null
M3 = null
M3_status = INCOMPLETE_COMPONENTS
```

不得动态将另一个平台权重从 0.5 提升为 1.0。

最重要的是：

```text
M3 = null
≠ Task06 stage failed
```

生产任务必须继续：

```text
M1 正常产出
M2 正常产出
M3 null + diagnostic
Task06 stage 正常完成
后续 stage 可继续
```

建议 diagnostics 明确记录：

```text
DC_HOT_SOURCE_UNAVAILABLE
THS_HOT_SOURCE_UNAVAILABLE
```

不得因为单一热榜源整日不可用直接 raise 业务异常导致 daily production job 崩溃。

### 8.4.1 真正应失败的情况

以下仍属于系统 / 契约错误，应 fail-fast：

```text
rank_position <= 0
identity 非法或重复
schema / contract 不一致
同一 snapshot 出现违反主键语义的数据冲突
数据库读取失败
计算产生不可能状态
```

原则：

```text
数据源缺失
→ 显式降级

系统无法相信输入或执行结果
→ job failed
```

## 8.5 M3 合成

已冻结权重：

```text
Episode return = 0.2
5D return       = 0.2
10D return      = 0.2
Popularity      = 0.4
```

中间值：

```text
m3_raw
= 0.2 × m3_episode_rank_score
+ 0.2 × m3_return5_rank_score
+ 0.2 × m3_return10_rank_score
+ 0.4 × hot_rank_score
```

四个 component 均有效时才形成 `m3_raw`。

否则：

```text
M3 = null
M3_status = INCOMPLETE_COMPONENTS
```

最后在当日 Recognition Pool 内对 `m3_raw` 再做一次 normalized ranking：

```text
M3
= normalized_rank_score(m3_raw)
```

最终 M3 语义：

> 该股票在当日有效辨识度池成员中，综合当前行情表现、短中期价格强度与市场关注度后的相对辨识度位置。

---

# 9. Asset Rank Snapshot 产出契约

Task06-A canonical logical result 建议至少包含：

```text
trade_date
ticker

m1_score
m1_rank
m1_status
m1_universe_size

m2_score
m2_rank
m2_status
m2_universe_size

m3_score
m3_rank
m3_status
m3_universe_size

calculation_version
diagnostics
evidence
```

同一 ticker 可出现：

```text
M1 = 96
M2 = null
M3 = 87
```

这不是异常，而是三维 eligibility 独立的自然结果。

建议逻辑名：

```text
system_b_asset_rank_snapshot
```

物理表名、repository 与 pipeline 落点必须先检查现有 QRP common infrastructure，再决定是否需要最小扩展。

不得为了 Task06 单独复制一套 Registry / Runtime / Persistence。

---

# 10. Component Audit Snapshot

为了确保算法可解释、可回放、未来可替换，建议形成通用 component 明细：

```text
trade_date
ticker
dimension
component

raw_value
direction
raw_rank
normalized_rank_score
universe_size
tie_count
status
calculation_version
```

M3 popularity component 还应保留：

```text
dc_source_status
ths_source_status
dc_hot_score
ths_hot_score
hot_rank_score
valid_snapshot_count
```

Raw Fact 必须可追溯到原始 pool / episode / hot snapshot 数据。

---

# 11. Score Status

至少定义：

```text
OK
NOT_ELIGIBLE
MISSING_INPUT
INSUFFICIENT_UNIVERSE
NO_VARIATION
INCOMPLETE_COMPONENTS
```

### OK

完整产生最终 score。

### NOT_ELIGIBLE

未进入对应三池，因此该维度不评价。

### MISSING_INPUT

理论上属于评价域，但必需的非外部容错事实缺失。

### INSUFFICIENT_UNIVERSE

有效比较对象少于 2。

### NO_VARIATION

有效对象不少于 2，但横截面不存在差异。

### INCOMPLETE_COMPONENTS

综合维度所需 component 未全部形成。

外部热榜整日不可用属于 M3 `INCOMPLETE_COMPONENTS`，不得默认升级为 stage failure。

---

# 12. Calculation Version

Task06 所有结果必须绑定 calculation version。

初版建议：

```text
system_b_asset_rank@0.1.0
```

下列变化必须升级版本：

```text
normalized_rank_score 公式变化
rank tie 语义变化
评价 universe 定义变化
M1 / M2 / M3 component 变化
component 权重变化
M3 日内热榜聚合规则变化
DC / THS 合成规则变化
缺失值 / 降级规则变化
```

禁止修改算法后重算历史却继续沿用旧 version。

---

# 13. Task06-B Theme Rank 当前边界

Task06 必须最终支持：

```text
trade_date
theme_id
→ theme relative score / rank
```

但当前 Theme Rank 具体算法尚未封板。

已明确：

```text
Theme Rank 是独立上层体系
```

```text
Task06-A M1/M2/M3 可作为 Theme Rank 的成员结构输入
```

```text
M4 / M5 可作为题材级输入
```

```text
M6 暂不进入评分系统
```

原因：M6 属于全市场公共环境，且 Task05 Judgment Layer 已承担部分市场环境职责；当前避免在 Ranking 与 Judgment 中重复计权。

当前不得实现：

```text
M4/M5 默认 50/50
任意未经业务确认的 Theme composite score
任意“主线题材”阈值
```

本地 Agent 如认为现有架构要求先定义 Theme Rank 接口，应提出最小接口方案，但不得自行补业务公式。

---

# 14. 与 Task05 / Task07 的边界

```text
Task05
→ 能不能新增仓？
```

```text
Task06-A
→ 哪些股票在 M1/M2/M3 上相对更强？
```

```text
Task06-B
→ 哪些题材相对更强？
```

```text
Task07
→ 最终选什么、买多少、形成什么 target portfolio？
```

Task06 不读取 Task05 authorization 才能计算。

即使：

```text
new_position_authorized = false
```

Task06 仍应形成完整的当日 Ranking Snapshot。

---

# 15. 明确不做

Task06 v0.1 当前不实现：

```text
M1/M2/M3 离散身份标签
龙头 boolean 标签
主线 boolean 标签
A/B/C 自动市场阶段判断
Task05 authorization
M6 score
账户与持仓约束
仓位计算
ENTER / HOLD / EXIT
target portfolio
未来收益预测
ML / Logistic scoring
PCA
TOPSIS
AHP
熵权
Elo
Wilson
Min-Max raw normalization
普通 Z-score
Robust Z-score production use
```

“龙头”若未来需要，应优先作为 M1/M2/M3 多维高分和领先程度的派生解释层，而不是反向定义基础分数。

---

# 16. 测试与退出条件

Task06-A 至少覆盖：

1. `normalized_rank_score = 100 × (N-rank)/(N-1)` 端点正确；
2. tie 获得相同业务 rank / score；
3. 输入顺序变化不改变业务结果；
4. N=1 → score=null + `INSUFFICIENT_UNIVERSE`；
5. N>=2 但所有 raw value 相同 → `NO_VARIATION`；
6. 未入 Capacity Pool → M1=null；
7. 未入 Height Pool → M2=null；
8. 未入 Recognition Pool → M3=null；
9. M1 使用 `episode_return` 而不是 capacity admission return；
10. M1 0.5 / 0.5 权重正确；
11. M2 的 `height_since_start_return` 包含 `height_start_date` 当天涨幅；
12. `height_start_base_close` 取 `height_start_date` 前一个实际交易日收盘；
13. M2 单 component rank 正确；
14. M3 0.2 / 0.2 / 0.2 / 0.4 权重正确；
15. 单 snapshot 榜单 rank 线性映射正确；
16. 成功 snapshot 中 ticker 未上榜 → score=0；
17. 不同 snapshot 榜单长度先各自标准化再 AVG；
18. 单平台日级 score 使用日内 AVG，不用 MAX / last snapshot；
19. `hot_rank_score = AVG(dc_hot_score, ths_hot_score)`；
20. 任一平台整日不可用 → M3=null + `INCOMPLETE_COMPONENTS`；
21. 任一平台整日不可用不会使 Task06 production stage 抛异常终止；
22. 热榜 source 缺失不会影响 M1 / M2；
23. 非法 rank / schema / identity 冲突仍 fail-fast；
24. M6 输入变化不得改变 Task06-A 结果；
25. theme membership 变化不得改变同一 `(trade_date,ticker)` 的 M1/M2/M3；
26. 相同 input snapshot + calculation_version 结果完全确定；
27. 现有三池、strategy、pipeline 全量回归零破坏。

Task06-A 完成定义：

```text
(date, ticker)
→ M1 / M2 / M3
```

已经形成稳定、确定、版本化的横截面相对比较语言：

```text
三池决定 eligibility
+
Raw Facts 决定 component
+
normalized_rank_score 表达相对位置
+
M1/M2/M3 形成独立股票级指标
+
外部热榜缺失只降级 M3，不拖垮 production job
```

---

# 17. 本地 Agent 设计审计重点

本轮先做设计审计，不直接实施代码。

Agent 需要结合当前 `develop/v1.1` 代码逐项回答：

1. 现有三池 feature / membership 输出是否已经提供 Task06-A 所需全部 raw facts？缺什么？
2. `height_start_base_close` / `height_since_start_return` 最合理的事实层落点在哪里？是否应随 Height Pool 一起计算和持久化？
3. 现有 `dc_hot` / `ths_hot` snapshot 数据是否足以区分“成功采集但 ticker 未上榜”与“平台整日不可用”？若不足，缺失哪个 source-level 状态事实？
4. 当前 pipeline / contract / repository 是否已有可复用的 rank snapshot / observation 模式？
5. 是否需要 QRP Core 最小扩展？若需要，只提出最小通用扩展，不为 System B 复制基础设施。
6. 如何保证外部热榜 source 缺失时 M3 fail-soft、而 schema / identity / storage 错误仍 fail-fast？
7. 当前 `_rank_by_date()` 使用 asset_id 作为 deterministic tie breaker，这与 Task06 要求的“业务 tie 不得被 ticker 打破”是否冲突？需要怎样最小调整或新增独立 ranking helper？
8. 是否存在 PIT / future leakage 风险，尤其是 Episode、Height start、热榜 snapshot 与 daily close 的时间边界？
9. 哪些现有测试可以复用，哪些必须新增？
10. Theme Rank 尚未封板的前提下，Task06-A 能否独立开发、测试、合并而不阻塞后续 Task06-B？

Agent 输出应区分：

```text
FACT
RISK
REQUIRED CHANGE
OPTIONAL IMPROVEMENT
OPEN BUSINESS DECISION
```

禁止在审计阶段自行修改本设计书中标记为“尚未封板”的业务规则。
