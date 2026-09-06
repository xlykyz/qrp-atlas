# Task06-A System B 股票横截面相对评分设计书 v0.1

## 1. 产品身份与本轮范围

```text
semantic_owner = SYSTEM_B
delivery_mode = BUILTIN
capability_type = STRATEGY_COMPONENT
runtime = QRP_COMMON
persistence = QRP_COMMON
```

Task06-A 是 System B 的股票级横截面相对比较组件。

它回答：

> 指定交易日，一只股票在 M1 / M2 / M3 三个评价维度上，相对各自有效评价域中的其他股票处于什么位置？

本轮只实现：

```text
Task06-A — Asset Relative Ranking
```

明确不实现、也不建立占位接口：

```text
Task06-B Theme Rank
M4 / M5 题材合成
M6 评分
主线题材判断
龙头 boolean 标签
Task05 authorization
账户 / 持仓 / target portfolio
ENTER / HOLD / EXIT
```

Theme Rank 待后续业务规则单独封板后再设计和实现，不得在 Task06-A 中提前搭建公式、结果占位或 `NOT_CONFIGURED` 骨架。

Task06-A 不定义 QRP Core 自身的股票评价标准，也不参与 Task05 的新增仓授权。

---

## 2. 核心语义

M1 / M2 / M3 不再是三类股票身份，而是某一交易日任意 ticker 独立的三个连续评价维度。

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

题材归属不会改变股票本身的 M1 / M2 / M3。

Task06-A 的稳定计算链路：

```text
PIT / canonical facts
→ pool eligibility
→ raw component
→ normalized_rank_score
→ dimension_raw
→ final dimension rank / score
→ asset rank snapshot
```

---

## 3. 输入与依赖边界

Task06-A 允许读取：

```text
历史行情 canonical series
System B 状态机 / Episode 事实
Capacity / Height / Recognition Pool membership
Ticker 级 dc_hot / ths_hot 人气事实（仅 M3）
目标日 canonical A股有效股票域
```

明确不依赖：

```text
theme_id
PIT Theme Membership
M4
M5
M6
market_phase
V_triggered
new_position_authorized
账户状态
已有持仓
未来收益
```

### 3.1 Canonical market series 工程约束

Task06-A 所有跨日价格/成交量派生必须来自同一条可复用的 canonical market series，并满足：

```text
1. 同一计算窗口内复权口径一致；
2. 只使用 actual trading observations；
3. 严格截断于 target_date，不读取未来数据；
4. 历史初始化与逐日日更在相同输入下产生一致结果。
```

实现阶段必须定位并验证现有具体输入路径；不得仅以“canonical market series”名称假定能力已经存在。

若除权场景能够复现现有价格序列不一致，应在事实输入层做最小修复，不得在 Task06 评分公式中补偿。

### 3.2 Raw feature 的生产边界

Task06-A 不要求把全部 raw feature 预先扩建到 pool membership，也不把三池改造成通用 feature store。

```text
episode_return
→ 复用现有 Episode observation

avg5_amount / return5 / return10
→ 可从 canonical market series 确定性计算

height_start_base_close / height_since_start_return
→ 作为 Height Pool 核心事实补齐
```

Task06 component audit 必须保留实际使用的 raw value 和 provenance。

---

## 4. Eligibility 与结果股票域

三池只决定对应维度是否进入评价：

```text
ticker ∉ Capacity Pool     → M1 = null / NOT_ELIGIBLE
ticker ∉ Height Pool       → M2 = null / NOT_ELIGIBLE
ticker ∉ Recognition Pool  → M3 = null / NOT_ELIGIBLE
```

因此：

```text
(M1=null, M2=null, M3=null)
```

是合法、需要正式物化的股票日级结果。

### 4.1 Asset output universe

Task06-A 的最终输出域不是三池并集，也不得用“目标日行情表恰好有行的 ticker”隐式替代。

正式输出域：

```text
目标 trade_date 的 canonical A股有效股票域
```

实现阶段必须复用项目现有权威 PIT 股票域解析入口，并验证上市、退市、停牌、目标日行情缺失等边界。

停牌或目标日缺少行情，不得仅因此把仍属于 canonical A股有效域的 ticker 从结果中静默删除；具体维度是否可计算由 eligibility 和 component status 决定。

---

## 5. 统一相对评分语言

### 5.1 Canonical 名称

统一命名：

```text
normalized_rank_score
```

不使用可能存在多种统计定义的 `percentile` 作为 canonical 算法名称。

### 5.2 Business rank 与 tie

对同一 trade_date、同一评价 universe 内的有效对象，默认方向：

```text
HIGHER_IS_BETTER
```

完全相同的 raw value 必须获得相同业务 rank。

v0.1 固定：

```text
tie method = average rank
```

稳定序列化可以额外按 ticker / identity key 排序，但不得用 ticker 打破业务 tie。

因此 rank 允许为小数，例如两个并列第一可得到：

```text
rank = 1.5
```

### 5.3 normalized_rank_score

设：

```text
N = 当前 component 的有效比较对象数量
rank = average business rank，1 为最强
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

无 tie 且极值唯一时：

```text
第一名 = 100
最后一名 = 0
```

存在极值 tie 时，并列对象共享同一 average rank / score，因此不强制仍命中 100 或 0。

### 5.4 特殊情况

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

```text
N >= 2
且所有有效 raw value 完全相同
→ normalized_rank_score = null
→ status = NO_VARIATION
```

禁止把“没有横截面信息”解释为 100 分。

---

## 6. Raw / Rank / Score 审计链路

Task06-A 必须保留：

```text
raw_value
→ raw_rank
→ normalized_rank_score
→ dimension_raw
→ final_dimension_rank
→ final_dimension_score
```

Rank / Score 不得覆盖或替代原始事实。

同一个 component 的 `universe_size` 必须是该 component 实际参与比较的有效对象数量，而不是简单使用三池 membership 总数。

---

# 7. M1 —— 容量核心强度

## 7.1 Eligibility Universe

```text
当日 Capacity Pool 中 membership_state = IN_POOL 的 ticker
```

未入池：

```text
M1 = null
M1_status = NOT_ELIGIBLE
```

## 7.2 Component 1：Episode 涨幅

使用现有：

```text
episode_return
```

M1 明确不使用“容量池入池以来涨幅”。

在当日 Capacity Pool 内：

```text
m1_episode_rank_score
= normalized_rank_score(episode_return)
```

## 7.3 Component 2：5 日平均成交额

```text
avg5_amount
```

定义沿用当前 System B 的实际交易观察序列：最近 5 个 actual trading observations 的 `amount` 均值，窗口不足则该 component 缺失。

在当日 Capacity Pool 内：

```text
m1_avg5_amount_rank_score
= normalized_rank_score(avg5_amount)
```

## 7.4 M1 合成

冻结权重：

```text
Episode return = 0.5
avg5_amount    = 0.5
```

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

最后在当日有效 `m1_raw` 集合内再次 normalized ranking：

```text
M1
= normalized_rank_score(m1_raw)
```

最终 M1 表达：

> 当前行情轮次表现与资金容量承载综合后的横截面相对强度。

---

# 8. M2 —— 高度空间强度

## 8.1 Eligibility Universe

```text
当日 Height Pool 中 membership_state = IN_POOL 的 ticker
```

未入池：

```text
M2 = null
M2_status = NOT_ELIGIBLE
```

## 8.2 Height 核心事实扩展

当前 Height Pool 已有：

```text
height_start_date
height_admitted_date
height_type
n
m
i
current_break_days
```

Task06-A 要求补齐并作为 Height Pool 核心事实持久化：

```text
height_start_base_close
height_since_start_return
```

### height_start_base_close

定义：

```text
height_start_date 前一个 actual trading observation 的 canonical adjusted close
```

如果不存在前一个 actual trading observation：

```text
height_start_base_close = null
height_since_start_return = null
```

不得改用 `height_start_date` 当日 close 代替。

### height_since_start_return

```text
height_since_start_return(t)
= canonical_adjusted_close(t) / height_start_base_close - 1
```

因此：

```text
height_start_date 当天涨幅必须计入累计涨幅
```

不得使用：

```text
close(t) / close(height_start_date) - 1
```

## 8.3 M2 Score

M2 v0.1 只有一个 component：

```text
M2
= normalized_rank_score(height_since_start_return)
```

`H(n)` / `H(n,m,i)` / `current_break_days` 等继续作为高度结构事实和 evidence，不进入 v0.1 权重。

最终 M2 表达：

> 当前有效高度结构从真实起点（含起点当日涨幅）形成的横截面相对价格空间强度。

---

# 9. M3 —— 辨识度强度

## 9.1 Eligibility Universe

```text
当日 Recognition Pool 中 membership_state = IN_POOL 的 ticker
```

未入池：

```text
M3 = null
M3_status = NOT_ELIGIBLE
```

Recognition Pool 的入池与 Episode 粘性保留继续由现有 pool evaluator 决定；Task06-A 不重新执行 Top30 入池筛选。

## 9.2 Price Components

v0.1 明确继承现有 Recognition Pool 的窗口定义：

```text
return5
= close(t) / close.shift(4) - 1

return10
= close(t) / close.shift(9) - 1
```

这里的 `close` 必须来自第 3.1 节定义的 canonical adjusted actual-trading sequence。

Task06-A 不自行把上述口径改为 shift(5) / shift(10)。

在当日 Recognition Pool 内分别计算：

```text
m3_episode_rank_score
= normalized_rank_score(episode_return)

m3_return5_rank_score
= normalized_rank_score(return5)

m3_return10_rank_score
= normalized_rank_score(return10)
```

## 9.3 Popularity Component

Ticker 级人气来源：

```text
dc_hot
ths_hot
```

### 9.3.1 v0.1 Snapshot contract

生产侧 v0.1 沿用当前完整 Top100 snapshot contract：

```text
N = 100
rank_position = 1..100
```

99 行、87 行等残缺响应不得因为 normalized rank 算子支持通用 N 就自动解释为合法变长榜单。

纯计算 helper 可以支持任意合法 `N >= 1`，但这不改变 v0.1 生产快照完整性规则。

### 9.3.2 单 Snapshot Score

对可靠、完整的 snapshot：

```text
snapshot_hot_score
= 100 × (N - rank_position) / (N - 1)
```

当平台 snapshot 正常，而目标 ticker 未上榜：

```text
snapshot_hot_score = 0
```

语义严格区分：

```text
0
= 平台正常观察市场，但 ticker 未上榜

null
= 对应平台 / 日期没有可信可用输入
```

### 9.3.3 单平台日级 Score

对本次 Task06-A run 启动时已经确认有效、且属于目标 `trade_date` 的全部 snapshots：

```text
dc_hot_score
= AVG(all valid DC snapshot_hot_score)

ths_hot_score
= AVG(all valid THS snapshot_hot_score)
```

必须先按 snapshot 标准化，再做日内 AVG。

禁止：

```text
AVG(rank_position) 后再标准化
单日 MAX
只取最后一次 snapshot
```

日内 AVG 同时表达排名高度与日内持续性。

### 9.3.4 双平台 Score

冻结：

```text
hot_rank_score
= AVG(dc_hot_score, ths_hot_score)
```

即：

```text
DC  = 0.5
THS = 0.5
```

不得动态重分配权重。

---

## 9.4 Popularity Availability 与生产降级

Task06-A 必须能够区分至少两种 source/date 状态：

```text
AVAILABLE
UNAVAILABLE
```

该状态必须是可回放的可信业务事实，不能只根据热榜表“有没有行”临时猜测。

### AVAILABLE

```text
平台当天存在可信完整 snapshots
```

Ticker 未上榜时仍为正常业务结果：

```text
platform_hot_score = 0
```

### UNAVAILABLE

```text
平台当日没有可信可用输入，且已被上游明确记录为 expected source unavailability
```

此时：

```text
platform_hot_score = null
```

如果任一平台：

```text
platform_hot_score = null
```

则：

```text
hot_rank_score = null
M3 = null
M3_status = INCOMPLETE_COMPONENTS
```

不得把另一个平台权重从 0.5 提升到 1.0。

### 9.4.1 Production 语义

Expected popularity source unavailability：

```text
M3 null + diagnostic
M1/M2 正常产出
Task06-A stage 正常完成
后续 stage 可继续
```

也就是：

```text
M3 = null
≠ production stage failed
```

实现必须让 expected `UNAVAILABLE` 真正穿过现有生产依赖边界，而不是只在 M3 函数里 catch exception。

但不得通过修改通用 scheduler 去吞掉所有上游失败。

以下仍应 fail-fast：

```text
schema / contract 不一致
identity 非法或重复
rank_position 非法
snapshot 违反完整性 / 主键语义
数据库或 storage error
计算产生不可能状态
```

建议 diagnostics：

```text
DC_HOT_SOURCE_UNAVAILABLE
THS_HOT_SOURCE_UNAVAILABLE
```

---

## 9.5 Popularity Input Version 与迟到数据

Task06-A 必须记录本次 run 实际消费的 popularity input provenance，至少能够识别：

```text
trade_date
source
source status
实际使用的有效 snapshot 集合 / 可唯一定位这些 snapshots 的版本信息
```

本次 run 启动后到达的迟到热榜数据：

```text
不得自动修改已经发布的历史 Task06-A 结果
```

如需纳入迟到数据：

```text
显式 rerun(target_date)
```

显式 rerun 产生新的、可审计的 calculation/input provenance。

v0.1 不要求为此新增一套通用版本存储框架，但现有按日期替换的热榜写入方式不能导致 Task06-A 无法说明自己实际消费了哪个输入版本。

---

## 9.6 M3 合成

冻结权重：

```text
Episode return = 0.2
5D return       = 0.2
10D return      = 0.2
Popularity      = 0.4
```

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

最后在当日有效 `m3_raw` 集合内再次 normalized ranking：

```text
M3
= normalized_rank_score(m3_raw)
```

最终 M3 表达：

> 当前行情表现、短中期价格强度与跨平台市场关注度综合后的横截面相对辨识度。

---

# 10. Asset Rank Snapshot

Task06-A 最终对第 4.1 节定义的每个目标 ticker 物化一行。

逻辑结果至少包含：

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
input_provenance
diagnostics
evidence
```

`m1_rank / m2_rank / m3_rank` 必须允许小数，以支持 average tie rank。

推荐逻辑名：

```text
system_b_asset_rank_snapshot
```

物理落点必须复用现有 QRP common infrastructure；不得为 Task06-A 新建第二套 Registry / Runtime / Persistence，也不得新增独立数据库路径或独立常驻服务，除非现有基础设施存在经验证的不可兼容约束。

---

# 11. Component Audit

Task06-A 必须保留 component 级审计链路。可独立物化，也可在现有结构化 evidence 中保存，但内容必须受契约约束。

至少包括：

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
source_provenance
```

M3 popularity 额外保留：

```text
dc_source_status
ths_source_status
dc_hot_score
ths_hot_score
hot_rank_score
dc_valid_snapshot_count
ths_valid_snapshot_count
popularity_input_version
```

Raw Fact 必须能够追溯到实际消费的 Episode / market series / pool membership / popularity snapshot。

---

# 12. Status Contract

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

Ticker 不属于该维度对应三池，当日不评价该维度。

### MISSING_INPUT

Ticker 理论上属于评价域，但必需的非 expected-degradation 事实缺失。

### INSUFFICIENT_UNIVERSE

有效比较对象数量不足 2。

### NO_VARIATION

有效对象不少于 2，但该比较层全部 raw value 相同。

### INCOMPLETE_COMPONENTS

综合维度要求的 component 未全部形成。

Expected popularity source `UNAVAILABLE` 导致的 M3 缺失属于：

```text
INCOMPLETE_COMPONENTS
```

不是 stage failure。

---

# 13. Calculation Version

初版：

```text
system_b_asset_rank@0.1.0
```

以下变化必须升级版本：

```text
normalized_rank_score 公式
average tie 语义
评价 universe
M1 / M2 / M3 component
component 权重
return5 / return10 窗口定义
热榜 snapshot 完整性语义
日内 AVG 规则
DC / THS 合成规则
availability / missing 规则
目标股票域定义
```

禁止修改算法后重算历史却继续沿用旧 calculation version。

---

# 14. 与 Task05 / Task07 的边界

```text
Task05
→ 能不能新增仓？

Task06-A
→ 股票在 M1/M2/M3 上相对有多强？

Task07
→ 最终选什么、买多少、形成什么 target portfolio？
```

Task06-A 不读取 Task05 authorization 才能计算。

即使：

```text
new_position_authorized = false
```

Task06-A 仍应形成完整当日 Asset Rank Snapshot。

---

# 15. 明确不做

Task06-A v0.1 不实现：

```text
Task06-B Theme Rank
M1/M2/M3 离散身份标签
龙头 boolean 标签
主线 boolean 标签
M4/M5/M6 scoring
A/B/C 自动市场阶段判断
Task05 authorization
账户与持仓约束
仓位计算
ENTER / HOLD / EXIT
target portfolio
未来收益预测
ML / Logistic scoring
PCA / TOPSIS / AHP / 熵权
Elo / Wilson
Min-Max raw normalization
Z-score / Robust Z production use
通用因子平台重构
pool feature store 扩建
通用热榜变长 snapshot 改造
```

“龙头”未来如需形成，应优先作为 M1/M2/M3 多维高分及领先程度的派生解释层，而不是反向定义基础评分。

---

# 16. 工程实现原则

实现阶段遵守：

```text
先验证现有可复用路径
→ 有真实缺口时做最小正确修改
→ 再完成纯计算
→ 再接入 persistence / production
```

重点工程验证项：

```text
1. 除权场景：历史初始化 vs 逐日日更是否消费一致的跨日 adjusted price；
2. canonical market series 的具体可复用函数 / repository / query；
3. expected popularity UNAVAILABLE 如何形成可信状态并穿过生产依赖；
4. run 启动时如何锁定并回放实际消费的 popularity snapshots；
5. canonical A股目标日有效股票域的权威解析入口。
```

这些是工程接入验证，不重新打开已冻结业务公式。

如果验证发现现有实现存在事实层 bug：

```text
先测试复现
→ 在事实/输入层最小修复
→ 不在 scoring layer 隐式补偿
```

---

# 17. 测试与完成定义

Task06-A 至少覆盖：

1. normalized rank 公式正确；
2. average tie 获得相同业务 rank / score，ticker 不打破 tie；
3. 输入顺序变化不改变业务结果；
4. N=1 → null + `INSUFFICIENT_UNIVERSE`；
5. N>=2 且无差异 → `NO_VARIATION`；
6. 三池未入池分别得到对应 `NOT_ELIGIBLE`；
7. 三维均未入池的 canonical A股 ticker 仍物化一行；
8. M1 使用 `episode_return` + `avg5_amount`，0.5/0.5；
9. M1 任一 component 缺失 → `INCOMPLETE_COMPONENTS`；
10. `avg5_amount` 使用 5 个 actual trading observations；
11. M2 `height_start_base_close` 取起点前一 actual trading observation；
12. M2 包含 `height_start_date` 当天涨幅；
13. 前一 actual trading observation 不存在时 M2 不伪造基准价；
14. M3 `return5 = shift(4)`、`return10 = shift(9)`；
15. M3 0.2/0.2/0.2/0.4 权重；
16. 完整 Top100 snapshot 评分正确；
17. 正常 snapshot 中 ticker 未上榜 → 0；
18. 残缺 Top100 response 不被当作合法变长 snapshot；
19. 单平台日级 score 使用有效 snapshot 的日内 AVG；
20. `hot_rank_score = AVG(dc_hot_score, ths_hot_score)`；
21. 任一平台可信 `UNAVAILABLE` → M3 null + `INCOMPLETE_COMPONENTS`；
22. expected popularity `UNAVAILABLE` 不导致 Task06-A production stage 失败；
23. popularity 缺失不影响 M1/M2；
24. schema / identity / rank / storage 错误仍 fail-fast；
25. popularity 迟到数据不自动修改历史结果，显式 rerun 可重算；
26. run provenance 可识别实际消费的 popularity input；
27. 除权场景下历史初始化与逐日日更结果一致；
28. target_date 截断无 future leakage；
29. 停牌/无当日行情但仍属于 canonical A股有效域的 ticker 不被静默删除；
30. M6 / theme membership / Task05 authorization 变化不改变 Task06-A 结果；
31. 相同 input provenance + calculation_version 结果完全确定；
32. 现有三池、Episode、strategy、pipeline targeted regression 通过；
33. full regression 通过。

Task06-A 完成定义：

```text
目标 trade_date canonical A股有效域
→ 每 ticker 一行
→ M1 / M2 / M3 独立、可空、可解释
→ raw/component/final 链路可审计
→ 输入版本可追溯
→ expected 外部热榜缺失只降级 M3
→ 结果 PIT-safe、deterministic、versioned、replayable
```
