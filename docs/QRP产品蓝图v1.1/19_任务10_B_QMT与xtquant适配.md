# 任务 10-B：QMT 与 xtquant 适配

## 一、目标

在任务 10-A 的 broker-neutral 核心之上，实现国金 QMT / xtquant 适配器，完成账户查询、下单、撤单、回调、重连、恢复和对账，并通过纸面、仿真和受限实盘三级验收。

## 二、前置

- 任务 10-A 已验收；
- 用户已在仓库外确认 QMT/xtquant 基础链路可连通；
- 正式开发机、QMT 客户端、账户权限和环境配置可用；
- 真实账户自动交易默认关闭。

## 三、适配器边界

建议结构：

```text
trading/qmt/
├── adapter.py
├── client.py
├── callbacks.py
├── mapping.py
├── config.py
├── diagnostics.py
└── AGENTS.md
```

适配器职责：

- 把 QMT/xtquant 请求和响应转换为 trading 标准领域对象；
- 维护 QRP client order id 与券商 order id 映射；
- 归一状态码、错误码和回调；
- 不实现策略、评分、目标组合和账户风控政策。

## 四、环境与配置

配置至少包括：

```text
qmt_path
session_id
account_type
account_id reference
mode
connect_timeout
query_timeout
submit_timeout
reconnect_policy
enabled
```

要求：

- 敏感信息不提交 Git；
- account_id 在日志/API 中脱敏；
- 纸面、仿真和实盘配置物理/逻辑隔离；
- `LIVE` 模式需要显式双重启用；
- 启动时验证 QMT 进程、路径、账户和权限。

## 五、连接生命周期

支持：

```text
DISCONNECTED
CONNECTING
CONNECTED
DEGRADED
RECONNECTING
FAILED
STOPPED
```

要求：

- session id 唯一；
- 重复 connect 幂等；
- 连接失败明确诊断；
- 断线触发 degraded/recovery，不直接重下订单；
- 重连后先查询和对账；
- 回调线程异常不导致静默停止；
- health 输出连接、回调、查询和最后事件时间。

## 六、查询能力

实现并标准化：

- 账户资产；
- 可用资金；
- 持仓和可用数量；
- 当日委托；
- 当日成交；
- 单订单查询（若 SDK 支持）；
- 账户状态；
- 交易连接状态。

查询结果必须：

- 转为标准模型；
- 保留 broker raw reference/diagnostic；
- 处理空结果和 SDK 异常；
- 时间和编码规范化；
- 不因单项失败伪造完整快照。

## 七、下单

支持 v1.1 生产所需的 A 股 long-only 买卖：

```text
submit_order(StandardOrderCommand)
```

要求：

- 仅接收任务 10-A 风险门已批准的 command；
- 映射账户、标的、方向、数量、价格类型和策略备注；
- 返回同步接收结果不等同最终成交；
- 立即保存提交尝试和返回值；
- 超时进入 UNKNOWN 并查询恢复；
- client_order_id 进入可用的策略/备注字段或本地映射；
- 不在适配器自动改变数量和方向；
- SDK 拒绝转为稳定 reason code。

## 八、撤单

- 只撤销可撤订单；
- 撤单请求幂等；
- 同步返回不等同最终取消；
- 通过回调/查询确认终态；
- 已成交或终态订单返回稳定原因；
- kill switch 的撤单策略遵循任务 00 和 10-A，不在适配器自行决定。

## 九、回调

归一：

- 连接状态；
- 委托状态；
- 成交；
- 资产/持仓变动（SDK 支持时）；
- 错误；
- 异步下单响应。

要求：

- 回调快速入队/落盘，避免阻塞 SDK；
- 乱序、重复和延迟回调幂等；
- 无映射订单进入 unmatched queue；
- 原始字段保留审计引用；
- 回调异常有监控和告警；
- 不在回调中直接调用策略。

## 十、状态映射

建立明确表：

```text
QMT order status
→ trading LiveOrderStatus
→ terminal/non-terminal
→ allowed transitions
```

必须覆盖：

- 已报；
- 未报/待报；
- 部成；
- 已成；
- 已撤；
- 废单/拒单；
- 撤单中；
- 未知/新增 SDK 状态。

未知状态进入 `UNKNOWN`，不得猜测为终态。

## 十一、恢复和对账

重连/重启后：

```text
query account
→ query positions
→ query orders
→ query fills
→ rebuild mappings
→ feed missing events to OMS
→ reconcile
→ unlock new submissions
```

要求：

- 本地 SUBMITTING 订单能与券商记录匹配；
- 无法匹配时保持 UNKNOWN；
- 外部人工订单和成交标记；
- 持仓、资金和成交差异进入正式对账；
- 不因本地状态丢失重复提交。

## 十二、模式

### PAPER

由任务 10-A paper adapter 提供，不启动 QMT。

### SIMULATION

连接 QMT 仿真账户或明确模拟环境：

- 使用同一 QMT adapter；
- 验证真实 SDK 查询、下单、撤单和回调；
- 账户与正式账户隔离。

### LIVE

- 默认 disabled；
- 需要配置、控制状态和人工授权；
- 只执行冻结计划；
- 风险门和恢复未通过时禁止下单；
- 小资金和严格仓位限制由任务 11 验收配置控制。

## 十三、API 与前端

API 仍使用 trading 标准模型，增加：

- broker health；
- connection/recovery status；
- masked account；
- broker diagnostics；
- unmatched events；
- QRP/broker mapping；
- simulation/live mode。

前端不暴露 SDK 参数和任意原始下单入口。

## 十四、测试

### 单元

使用 fake xtquant client 覆盖：

- 字段和状态映射；
- 查询；
- submit/cancel；
- SDK 异常；
- 超时；
- 回调乱序/重复；
- unmatched；
- 重连；
- 日志脱敏。

### 集成

在可控 QMT 环境覆盖：

- connect/disconnect；
- 账户/持仓/订单/成交查询；
- 仿真下单；
- 部分成交或可控等价场景；
- 撤单；
- 断线重连；
- 进程重启恢复；
- 日终对账。

真实账户测试不得作为普通 CI 自动执行。

## 十五、安全要求

- 真实下单默认关闭；
- 无冻结计划不得下单；
- 无风险门批准不得下单；
- 未完成 recovery/reconciliation 不得下单；
- account/mode 不匹配 fail closed；
- 网络远程访问不暴露 QMT 端口到公网；
- 所有 live 操作有审计；
- 凭证、路径敏感信息和完整账户号不进入结果包。

## 十六、禁止范围

- 不修改策略和目标；
- 不做盘口、Level-2 或高频；
- 不使用 QMT 自带策略替代 QRP；
- 不在 adapter 内重试未知订单；
- 不直接从前端发送任意订单；
- 不支持多券商或多账户作为 v1.1 门禁；
- 不把仿真通过等同正式实盘授权。

## 十七、验收

- QMT adapter 完整实现 BrokerProtocol；
- 查询、下单、撤单、回调、重连和对账可用；
- OMS 幂等和状态语义不被 SDK 破坏；
- 仿真端到端和故障恢复通过；
- LIVE 默认关闭并受双重门禁；
- 专项、全量、fake SDK 和 QMT 集成测试通过；
- PR 等待独立验收。