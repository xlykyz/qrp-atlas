# M4 Effective Member Rule v1.0.0

> Rule ID：`theme_m4_effective_member@1.0.0`  
> 状态：**FROZEN / Task04-A 正式语义基线**  
> 时区：`Asia/Shanghai`（UTC+8）  
> 适用范围：Task04-A / THEME StockCollection / M4 Effective Member / Theme Custom Index Production

---

## 1. 文档目的

本规则用于正式回答两个问题：

1. **某只股票在哪一个交易日可以合法进入某个 Theme 的 M4 计算候选集合？**
2. **进入候选集合后，该股票在该交易日是否最终具备 M4 计算资格？**

本规则同时定义 Task04-A 的历史稳定性边界：

> **已经 finalized 的生产历史是 ledger，不是 cache。后续 membership 新增、删除、回溯修订或源事实纠错，不得改写已经 finalized 的历史生产结果。**

`calculation_version` 必须引用本规则 ID。规则升级必须发布新的规则版本，不得静默修改本版本语义。

---

## 2. 核心分层

M4 必须严格区分三层事实：

```text
Theme Membership
“业务上它是否属于这个 Theme”
        ↓
Membership Admission
“它是否被允许进入交易日 D 的 M4 候选成员集合”
        ↓
M4 Effective Member
“它在 D 日是否最终参与 M4 / 自定义指数计算”
```

正式 invariant：

```text
Theme Membership
≠ Membership Admission
≠ M4 Calculation Eligibility
```

因此：

- 股票可以属于 Theme，但尚未到合法纳入日；
- 股票可以已合法进入当日候选集合，但因新股、停牌等原因不参与当日 M4；
- M4 资格变化不得反向修改 Theme Membership 身份事实。

---

## 3. 时间定义

### 3.1 合法交易日 D

`D` 必须来自正式交易日历：

```text
trading_calendar.is_open = true
```

非交易日不存在独立的当日 M4 admission cutoff。

### 3.2 固定 Admission Cutoff

对每个合法交易日 `D`：

```text
AdmissionCutoff(D) = D 09:00:00 Asia/Shanghai (UTC+8)
```

边界采用左闭右开式业务约束：

```text
t <  D 09:00:00  → 允许影响 D
t >= D 09:00:00  → 不允许影响 D
```

**09:00:00 整点发生的变更归入下一合法交易日。**

### 3.3 Membership 变更时刻 t

`t` 表示：

> membership 新增、删除或修订已经正式提交到 canonical membership source，并成为系统可消费事实的时刻。

`t` 不是：

- 人开始研究或编辑的时间；
- 文件创建时间；
- M4 任务开始或结束时间；
- 人工声称的“本来属于昨天”的主观时间。

实现层必须为 `t` 提供**可持久化、不可倒填、可审计、至少秒级**的 authoritative timestamp。

当前 `available_trade_date` 只有 DATE 粒度，不能单独承担 08:59 / 09:00 / 09:01 的 admission 判定。实现可复用语义满足要求的 `ingested_at`，或新增专用 timestamp 字段；字段选择属于实现任务，但不得改变本规则的时间语义。

### 3.4 实际计算时刻 T

M4 实际任务执行时刻记为 `T`。

正式规则：

> **T 不参与 membership admission 判定。**

禁止使用：

```text
t < T  → 今天参与
t > T  → 明天参与
```

任务早跑、晚跑、重试或故障恢复不得改变同一交易日的合法成员集合。

---

## 4. Membership Admission Rule

### 4.1 新增成员

对于交易日 `D`，某股票只有同时满足以下条件，才允许进入 D 日 Theme 候选集合：

1. membership 的业务有效区间覆盖 `D`；
2. membership 变更已在 `AdmissionCutoff(D)` 前正式提交，即 `t < D 09:00:00`。

若在 `t >= D 09:00:00` 才新增：

- 不得影响 D；
- 最早从下一合法交易日开始参与 admission；
- 即使 `effective_from` 被回溯到 D 或更早，也不得反向纳入 D。

### 4.2 删除成员

删除与新增完全对称：

- `t < D 09:00:00` 删除：D 日即不再进入候选集合；
- `t >= D 09:00:00` 删除：D 日仍按已锁定集合处理，下一合法交易日起移除。

### 4.3 Membership 修订

对 `effective_from`、`effective_to` 或其他会改变历史 membership 区间的修订，同样受 admission cutoff 和 finalized history 约束。

后续获知：

```text
“某股票其实从更早日期就属于 Theme”
```

只能修正 membership 业务事实，不得因此重新纳入已经锁定或 finalized 的历史 M4 生产集合。

### 4.4 跨午夜与盘前工作

自然日午夜不是业务边界。

例如：

```text
D-1 收盘后未完成维护
D 00:30 继续工作
D 08:40 正式提交 membership
D 09:00 admission cutoff
```

由于：

```text
08:40 < 09:00
```

且 membership 业务有效区间覆盖 D，因此该成员可以合法进入 D 日候选集合。

无需人为标记“这属于 D-1 的工作”。

### 4.5 周末与节假日

非交易日没有独立 cutoff。

在周末、节假日或休市日完成的 membership 变更，在下一合法交易日 `D` 的 09:00 前已经正式提交且业务有效区间覆盖 D，则可进入 D 日候选集合。

---

## 5. M4 Effective Member Rule

股票通过 Membership Admission 后，才进入 M4 资格判断。

对 `(collection_id, asset_id, trade_date=D)`：

### Rule 1｜必须是 D 日合法纳入的 Theme Member

只有已进入 D 日 Theme 候选成员集合的股票才进入后续 M4 资格判断。

正式生产不应为全市场非成员股票生成 `is_theme_member=false` 行。

### Rule 2｜上市实际交易日数必须可确认

必须存在可证明的：

```text
confirmed_listing_trading_day_count
```

如果上市交易日事实缺失、不可解析，或明确标记为：

```text
UNRESOLVED_MISSING
```

则 fail-closed：

```text
is_m4_effective_member = false
exclusion_reason = UNCONFIRMED_LISTING_DAYS
```

### Rule 3｜上市实际交易日数必须严格大于 5

```text
confirmed_listing_trading_day_count > 5
```

即：

- 第 1—5 个实际交易日：不具备 M4 资格；
- 从第 6 个实际交易日起：满足该项资格。

不满足时：

```text
is_m4_effective_member = false
exclusion_reason = NEW_LISTING_LE_5
```

### Rule 4｜D 日不得停牌

若 D 日：

```text
is_suspended = true
```

则：

```text
is_m4_effective_member = false
exclusion_reason = SUSPENDED
```

停牌只影响当日 M4 资格，不修改 Theme Membership。

### Rule 5｜EXPLICIT_NON_TRADING 等价于不可交易

若市场事实明确为：

```text
market_fact_status = EXPLICIT_NON_TRADING
```

M4 资格层按不可交易处理，不参与 D 日计算。

### Rule 6｜最终资格公式

概念公式：

```text
is_m4_effective_member =
    admitted_theme_member(D)
    AND listing_days_confirmed(D)
    AND confirmed_listing_trading_day_count(D) > 5
    AND tradable(D)
```

其中：

```text
tradable(D) = NOT suspended / NOT explicit non-trading
```

### Rule 7｜排除原因优先级

当多个排除条件同时出现时，v1.0.0 使用固定优先级：

```text
UNCONFIRMED_LISTING_DAYS
    > NEW_LISTING_LE_5
    > SUSPENDED
```

该优先级属于规则语义，修改必须升级规则版本。

---

## 6. 正式生产事实：theme_effective_member_daily

`is_m4_effective_member` 不再只作为运行时 DataFrame 中间变量存在。

Task04-A 正式生产必须建立逐日、逐 Theme、逐股票事实：

```text
theme_effective_member_daily
```

建议唯一键：

```text
(collection_id, trade_date, asset_id)
```

至少应保留：

```text
collection_id
theme_id
asset_id
trade_date
is_theme_member
confirmed_listing_trading_day_count
is_suspended
is_m4_effective_member
exclusion_reason
calculation_version
input_snapshot_id
production_run_id
created_at
finalized_at
```

其中：

```text
calculation_version = theme_m4_effective_member@1.0.0
```

`is_theme_member` 在该表中主要用于审计表达；正常生产只写入 D 日候选 Theme 成员，因此其业务预期为 `true`。

---

## 7. finalized 与不可变生产历史

### 7.1 Ledger invariant

一旦交易日 D 的 `theme_effective_member_daily` finalized：

> **后续任何 membership 新增、删除、effective date 回溯修订、源 metadata 修正或晚到事实，都不得修改或 invalidate D 日已 finalized 的成员资格事实。**

同样不得因此改写其下游已 finalized 生产事实，包括：

```text
theme_custom_index_daily
theme_custom_index_state
theme_custom_index_episode
theme_m4_observation
```

### 7.2 历史 membership 与历史生产事实允许不同

系统后续可能知道更准确的历史 membership。

因此允许同时存在：

```text
Membership History
= 当前已知、可修订的业务身份事实

Production History
= 当时按照合法 admission + 当时资格规则实际生产并 finalized 的历史资产
```

二者不要求因后续知识修订而重新一致。

### 7.3 禁止普通生产触发历史重算

普通 DAILY / CORRECTION membership 变更不得触发对 finalized 历史日期的：

- effective-member 重算；
- custom-index 重算；
- state / episode 重算；
- M4 observation 重算。

生产路径必须以 finalized 历史为不可变 anchor 向前推进。

### 7.4 Replay 的边界

若研究或审计需要回答：

> “按照今天已经知道的完整历史事实，当时理论上会得到什么结果？”

可以提供显式 PIT / research replay，但必须：

- read-only；
- non-materializing；
- 不覆盖 canonical production history；
- 与正式生产结果明确区分。

---

## 8. Custom Index 的消费边界

Custom Index 不得拥有第二套成员资格规则。

正式生产链应为：

```text
Theme Membership
        ↓
09:00 Admission
        ↓
M4 Eligibility
        ↓
theme_effective_member_daily (finalized fact)
        ↓
Custom Index
```

Custom Index 只消费：

```text
is_m4_effective_member = true
```

并负责：

```text
成员日收益
→ 等权平均
→ theme_daily_return
→ index_level
```

`total_member_count` 与 `effective_member_count` 应以正式 effective-member facts 为来源，不在指数层重新解释 membership eligibility。

---

## 9. 规则版本锚点

历史记录中的：

```text
calculation_version
```

必须明确引用本规则：

```text
theme_m4_effective_member@1.0.0
```

推荐同时保存：

```text
rule_spec_hash
```

用于证明历史计算时引用的规则文档内容未被静默修改。

规则、实现与测试必须形成稳定链路：

```text
calculation_version
        ↓
Rule Spec
        ↓
Implementation
        ↓
Regression Tests
```

规则内容发生业务语义变化时，必须发布新的版本，例如：

```text
theme_m4_effective_member@1.1.0
```

不得直接修改 v1.0.0 的既有语义后继续沿用同一 `calculation_version`。

---

## 10. 边界案例（Normative Examples）

假设 D 为合法交易日，cutoff 为 D 09:00 UTC+8：

| 场景 | D 日处理 |
|---|---|
| D 08:59:59 新增，业务有效区间覆盖 D | 可以进入 D 日候选集合 |
| D 09:00:00 新增 | D 不进入，最早下一合法交易日 |
| D 09:01 新增 | D 不进入，最早下一合法交易日 |
| D 08:59 删除 | D 日即移除 |
| D 09:01 删除 | D 日仍保留，下一合法交易日起移除 |
| D 08:40 才完成 D-1 盘后维护 | 若正式提交且业务有效区间覆盖 D，可进入 D |
| 周末新增 | 下一合法交易日 09:00 前已提交则可进入该交易日 |
| D 10:00 才发现 effective_from 应为 D-10 | 不重算 D-10 至 D；按下一合法交易日向前生效 |
| finalized 后修改 membership 历史 | finalized effective-member 及下游生产事实不变 |
| M4 实际任务延迟到深夜 | 不改变 D 日 admission 结果 |

---

## 11. Task04-A 完成条件中的地位

本规则不是局部补丁约定，而是 Task04-A 的正式生产语义基线。

Task04-A 只有在以下语义全部落地后才视为真正完成：

```text
09:00 Membership Admission
+ theme_effective_member_daily 正式事实化
+ calculation_version 明确锚定规则规范
+ finalized immutable production history
+ Custom Index 消费正式成员事实
+ 普通生产不再因 membership 修订重算历史
```

核心不变量：

> **Past production history is a ledger, not a cache.**

> **Membership changes can affect only the first legal, not-yet-finalized trading-day production set permitted by the 09:00 admission rule.**
