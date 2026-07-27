# System B 行情轮次规格与运行

## 边界

行情轮次消费已封版的 `system_b_state_observation`，不修改基础状态机规则。存续的唯一表达是 `episode_end_date IS NULL`，不设置 `episode_status`。不包含高度池、容量池、辨识度池、M 身份、评分、筛选或交易逻辑。

## 输入语义

- 仅处理 `market_fact_status='ACTUAL_TRADING'` 且 `trend_state` 有效的实际状态观察。
- 无有效状态观察的日期不推进也不打断状态、低于 MA10 连续性或交易日计数。
- MA5 直接消费状态机正式输出；MA10 通过正式参数化指标 `sma(window=10)` 计算，行情轮次计算器只消费结果，不实现均线算法。
- 价格口径继承状态机的 `FORWARD_ADJUSTED`。

## 轮次规则

- 无未结束轮次时，实际观察序列发生 `CANDIDATE -> ACTIVE`，创建轮次；开始日和基准收盘为前一实际观察日，确认日为当日，重入次数为 0。
- 已有未结束轮次时再次发生该转换，只在当日将 `ma5_reentry_count` 加一。
- 当前状态非 `ACTIVE`，且当前与前一实际观察日均有 `close < MA10` 时，当日结束；结束日仍输出观察。
- 编号按资产独立递增：`{asset_id}_EP_{episode_no:04d}`。
- 允许下一轮开始日等于前轮结束日；该共享日只在前轮生成每日观察，下一轮从确认日开始观察。
- 禁止下一轮开始日早于前轮结束日，禁止下一轮确认日不晚于前轮结束日。

## 每日观察与公式

每日观察从确认日开始输出，不向候选日回填次日才可知的 `episode_id`。因此确认日 `days_since_start=1`、`days_since_confirmed=0`。

- `episode_return = close / start_close - 1`
- `peak_return = max(close_so_far) / start_close - 1`
- `drawdown_from_peak = close / max(close_so_far) - 1`

## 存储

- 主表：`system_b_episode`
- 每日表：`system_b_episode_observation`
- 正式入口：`qrp-atlas-system-b-episode --state-input-database <state-db> --output-database <episode-db> --end-date YYYY-MM-DD --report <json>`
- 全量重建按规则版本原子删除并重算；输入乱序由资产、日期稳定排序规范化。
- 状态输入必须为已存在的绝对路径并以只读方式打开；输入验证先于输出建表和事务写入。
- 正式验收起点之前必须存在状态历史上下文，禁止从验收起点空状态启动。
