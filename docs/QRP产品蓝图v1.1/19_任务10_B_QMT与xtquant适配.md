# 任务 10-B：MiniQMT 网关协议与执行适配

## 一、目标

在任务 10-A 的 `qrp-atlas/execution` 核心之上，完成独立 `qrp-trading` MiniQMT 网关的生产协议封板，并在 qrp-atlas 中实现对应的远程网关适配器。

本任务不是在 qrp-atlas 中重新实现 xtquant，也不是把 qrp-trading 合并进 qrp-atlas。

最终链路：

```text
qrp-atlas/execution
→ versioned REST / WebSocket
→ independent qrp-trading on Windows
→ MiniQMT / xtquant
→ broker account
```

## 二、当前现实基线

用户已在本地独立仓库 `E:\projects\qrp-trading` 完成并启动 MiniQMT 局域网网关。

已确认能力：

- 服务监听 `0.0.0.0:8000`，局域网地址 `192.168.0.104:8000`；
- MiniQMT 连接和账户订阅成功；
- 自动发现账户别名 `account_1`；
- 资产、持仓、当日委托、当日成交查询；
- 最新行情、合约、板块和交易日历；
- 下单、撤单和行情订阅；
- `XtQuantTrader`、`xtdata`、信用账户和其他公开能力的通用 RPC；
- `/api/v1/capabilities` 能力与签名发现；
- WebSocket 连接、账户、订单、成交、错误和行情事件；
- READONLY / TRADE / ADMIN API Key；
- IP 白名单、账户脱敏、SQLite 审计、运行日志和防火墙脚本；
- 生命周期方法 `start/stop/connect/register_callback/run_forever` 禁止远程调用；
- `TRADE_ENABLED=false` 时正确拒绝下单和撤单；
- Linux 客户端可通过局域网访问；
- 离线自动化测试 11 passed；
- 本机真实 MiniQMT 查询、RPC、异步回调和行情订阅已验证。

因此本任务从“已有可运行网关”出发，不按“QMT能力尚未实现”重新建设。

## 三、双仓工作包边界

### 3.1 qrp-trading 子交付

负责：

- 生产窄协议；
- 网关协议版本；
- request/client order 幂等；
- 稳定错误码；
- WebSocket 事件序列和恢复游标；
- MiniQMT 原始状态映射；
- 权限、IP、交易总开关和本地审计；
- 网关生命周期、健康、重连和能力发现；
- Linux SDK/客户端契约。

### 3.2 qrp-atlas 子交付

建议结构：

```text
src/qrp_atlas/execution/adapters/miniqmt_gateway/
├── __init__.py
├── client.py
├── adapter.py
├── models.py
├── mapping.py
├── events.py
├── recovery.py
├── config.py
├── diagnostics.py
└── AGENTS.md
```

负责：

- 实现 `ExecutionGatewayProtocol`；
- 将 QRP 标准命令转换为网关窄协议；
- 将网关响应和事件转换为 execution 标准领域对象；
- 管理 QRP `client_order_id`、网关 request id 和 broker order id 映射；
- WebSocket 断线后的主动查询、恢复和对账；
- 不 import xtquant；
- 不调用网关生命周期方法；
- 不使用通用 RPC 绕过生产窄协议。

### 3.3 不共享的内容

- 两个仓库不共享数据库；
- qrp-atlas 不读取 qrp-trading 的 `audit.db` 作为常规运行依赖；
- qrp-trading 不直接写 QRP 数据库；
- API Key 只通过环境或安全存储分发，不提交 Git；
- qrp-trading 的原始审计与 QRP 业务审计分别保留，通过 ID 对账。

## 四、生产窄协议

通用 RPC继续保留为 ADMIN级诊断和能力探索接口，但正式生产执行必须使用稳定窄协议。

至少封板：

```text
GET  /api/v1/health
GET  /api/v1/capabilities
GET  /api/v1/accounts
GET  /api/v1/accounts/{alias}/asset
GET  /api/v1/accounts/{alias}/positions
GET  /api/v1/accounts/{alias}/orders
GET  /api/v1/accounts/{alias}/trades
POST /api/v1/orders
POST /api/v1/orders/{client_order_id}/cancel
GET  /api/v1/orders/by-client-id/{client_order_id}
WS   /api/v1/ws/events
```

具体路径可保持现有实现兼容，但语义必须版本化和文档化。

禁止生产OMS通过：

```text
POST /api/v1/rpc/trader/{method_name}
POST /api/v1/rpc/xtdata/{method_name}
```

直接构造任意交易方法调用。

## 五、协议版本和能力协商

健康和能力接口至少返回：

```text
service_name
gateway_version
protocol_version
build_commit
server_time
timezone
trade_enabled
qmt_connected
account_subscribed
supported_accounts
supported_order_types
supported_price_types
supported_event_types
capability_hash
```

要求：

- qrp-atlas 声明支持的协议范围；
- 不兼容版本 fail closed；
- capability hash 变化进入告警和审计；
- 新增字段向后兼容；
- 删除或改变语义必须升级协议版本；
- QMT当前安装版本的动态能力与生产窄协议分开表示。

## 六、认证和网络安全

继续使用分级密钥：

```text
READONLY
TRADE
ADMIN
```

要求：

- 查询和WebSocket使用最小权限；
- 下单/撤单必须使用TRADE或明确受控的ADMIN；
- 通用RPC默认只允许ADMIN；
- qrp-atlas execution使用独立服务身份和专用交易Key；
- API Key不进入日志、结果包和PR；
- 支持密钥轮换与旧密钥失效；
- IP白名单默认限制局域网或指定Linux主机；
- 服务不得直接暴露公网；
- 外出访问只能通过加密虚拟局域网或认证隧道；
- 认证失败、来源IP非法和权限不足均有稳定错误码和审计。

## 七、双重交易门禁

真实下单必须同时满足：

```text
QRP execution control enabled
AND frozen order plan valid
AND QRP business risk passed
AND gateway TRADE_ENABLED=true
AND caller has TRADE permission
AND QMT connected/subscribed
AND account alias matches
```

任一失败均 fail closed。

`TRADE_ENABLED` 是网关机械安全门，不替代 QRP 业务风险；QRP 的 enable 也不能绕过网关关闭状态。

## 八、标准下单请求

生产下单请求至少包含：

```text
request_id
client_order_id
account_alias
asset_id
side
quantity
price_type
limit_price (optional)
strategy_tag
plan_id
plan_item_id
submitted_at
protocol_version
```

要求：

- 网关不得自动改变方向、数量和账户；
- 非法证券代码、数量、价格类型和时间拒绝；
- 账户对象只通过别名解析；
- xtconstant由网关内部映射，不要求QRP传SDK对象；
- 策略备注不包含敏感信息；
- 同步返回只表示接收/拒绝，不等价成交；
- 返回稳定 `gateway_request_id` 和可用的 broker order reference；
- 原始 QMT 返回保存在网关审计，外部API只返回脱敏标准字段。

## 九、幂等契约

网关必须把 `request_id` 和 `client_order_id` 作为正式幂等字段。

要求：

- 同一 `request_id` 重复请求返回第一次的确定结果或当前状态；
- 同一 `client_order_id` 不允许创建第二笔不同订单；
- 同一 `client_order_id` 且payload不同必须冲突拒绝；
- submit超时后支持按client_order_id查询；
- 网关重启后幂等记录仍可恢复；
- 未知提交不得由客户端盲目重发；
- 新attempt由QRP生成新的generation/client_order_id；
- cancel也具有独立request_id和幂等语义。

## 十、响应和错误码

统一响应信封至少包含：

```text
request_id
status
code
message
server_time
data
diagnostics_id
```

错误分类至少包括：

```text
AUTHENTICATION_FAILED
PERMISSION_DENIED
SOURCE_IP_DENIED
TRADE_DISABLED
QMT_DISCONNECTED
ACCOUNT_NOT_FOUND
ACCOUNT_NOT_SUBSCRIBED
INVALID_REQUEST
INVALID_ASSET
INVALID_QUANTITY
INVALID_PRICE_TYPE
IDEMPOTENCY_CONFLICT
ORDER_NOT_FOUND
ORDER_NOT_CANCELABLE
BROKER_REJECTED
BROKER_TIMEOUT
GATEWAY_BUSY
CAPABILITY_UNAVAILABLE
INTERNAL_ERROR
UNKNOWN_RESULT
```

不得把所有异常压成HTTP 500或自由文本。

## 十一、订单和成交事实

标准查询返回：

```text
client_order_id
gateway_request_id
broker_order_id
account_alias
asset_id
side
quantity
filled_quantity
average_price
order_status
submitted_at
updated_at
broker_status_code
error_code
```

要求：

- 账户号和内部敏感字段继续脱敏；
- broker原始状态保留审计引用；
- 空列表与查询失败分离；
- 时间戳timezone-aware；
- 数量、价格和金额类型稳定；
- 按client_order_id和broker_order_id均可查询；
- 网关不生成QRP策略reason code。

## 十二、WebSocket事件契约

支持并版本化：

```text
qmt_connected
qmt_disconnected
account_status
stock_asset
stock_position
stock_order
stock_trade
order_error
cancel_error
order_stock_async_response
cancel_order_stock_async_response
market_data
gateway_ready
heartbeat
```

每个事件至少包含：

```text
event_id
sequence
session_id
event_type
occurred_at
received_at
account_alias
client_order_id (when known)
broker_order_id (when known)
payload
protocol_version
```

要求：

- sequence在网关会话内单调；
- event_id全局可去重；
- 重复、乱序和延迟事件允许出现，QRP端必须幂等；
- WebSocket断线后不能假设没有事件；
- 重连后QRP先主动查询订单、成交、持仓，再恢复事件流；
- 心跳和最后事件时间进入健康状态；
- 无法匹配client_order_id的事件进入unmatched队列。

## 十三、MiniQMT生命周期

保持现有设计：

- qrp-trading内部统一调用start/connect/subscribe/register_callback/run_forever；
- 远程调用方不得直接触发生命周期方法；
- 重复启动和重连幂等；
- QMT断线时网关进入degraded状态；
- 重连后恢复账户订阅和行情订阅；
- 有活动订单或未知状态时发出高优先级事件；
- 生命周期异常不得静默吞掉。

qrp-atlas只消费health、查询和事件，不管理Windows进程。

## 十四、qrp-atlas适配器

`MiniQmtGatewayAdapter` 至少实现：

- 配置加载和密钥注入；
- health/capability negotiation；
- 账户别名选择；
- 资产、持仓、订单、成交查询；
- 标准submit/cancel；
- WebSocket事件消费；
- REST与事件映射；
- 超时、重试和熔断；
- client/gateway/broker ID映射；
- 日志脱敏；
- recovery/reconciliation触发。

重试边界：

- GET/只读查询可按策略重试；
- submit/cancel只允许使用相同request_id的幂等重试；
- 不确定结果先查询，不创建新订单；
- 协议不兼容、鉴权失败和交易关闭不自动重试。

## 十五、运行模式

### PAPER

使用任务10-A的本地paper adapter，不访问qrp-trading。

### GATEWAY_READONLY

连接真实网关但只查询和消费事件：

- `TRADE_ENABLED=false`；
- READONLY Key；
- 用于账户状态、持仓、行情和事件联调。

### GATEWAY_SIMULATION

连接MiniQMT仿真或明确受控账户：

- 使用生产窄协议；
- 验证下单、撤单、回调、断线和恢复；
- 与正式账户配置隔离。

### LIVE

- 默认disabled；
- QRP和网关双重启用；
- 只执行冻结计划；
- recovery/reconciliation未通过时禁止下单；
- 受限资金和严格仓位配置由任务11验收；
- 任一关键异常暂停新增订单。

## 十六、审计与三方对账

三层事实：

```text
qrp-atlas DB      业务订单、目标、原因和状态
qrp-trading audit 原始请求、响应、回调和调用身份
broker account    最终委托、成交、持仓和资金
```

必须支持通过以下字段关联：

```text
request_id
client_order_id
gateway_request_id
broker_order_id
account_alias
```

要求：

- 网关审计保留期和轮转明确；
- QRP无需常规直接读取SQLite文件；
- 事故调查可导出脱敏审计包；
- 三方不一致进入正式reconciliation difference；
- 不以网关审计替代券商最终查询。

## 十七、测试

### qrp-trading离线测试

至少覆盖：

- auth与权限；
- IP白名单；
- TRADE_ENABLED；
- 请求校验；
- request/client_order幂等；
- payload冲突；
- 标准错误码；
- 序列化与脱敏；
- QMT状态映射；
- WebSocket序列、重复和断线；
- 生命周期方法禁止远程调用；
- 审计与重启恢复。

### qrp-atlas单元测试

使用fake gateway覆盖：

- 协议和能力协商；
- 查询映射；
- submit/cancel；
- 超时和UNKNOWN；
- 错误码映射；
- 事件乱序/重复；
- unmatched；
- 断线；
- 日志脱敏；
- 三方ID映射。

### 局域网集成测试

在Windows网关与Linux QRP环境覆盖：

- health/capabilities；
- READONLY查询；
- WebSocket事件；
- 交易关闭时拒绝；
- 仿真下单和撤单；
- 部分成交或可控等价场景；
- QMT断线重连；
- 网关/QRP进程重启；
- UNKNOWN恢复；
- 日终三方对账。

真实账户交易不得作为普通CI自动副作用。

## 十八、安全要求

- 真实交易默认关闭；
- 无冻结计划、无业务风险批准不得提交；
- 通用RPC不得成为生产下单路径；
- account/mode不匹配fail closed；
- 不直接暴露网关到公网；
- API Key、完整账户号和敏感路径不进入Git和结果包；
- 服务端与客户端日志均脱敏；
- 所有交易类调用可审计；
- kill switch在QRP和网关两层均可阻塞新增订单。

## 十九、禁止范围

- 不把qrp-trading并入qrp-atlas；
- 不在qrp-atlas import xtquant；
- 不在网关实现策略、评分、目标组合和业务风险；
- 不让前端直接调用网关或通用RPC；
- 不通过RPC远程启动/停止QMT生命周期；
- 不在未知订单状态盲目重下；
- 不把现有真实连接验证等同完整生产验收；
- 不以单次成功下单跳过连续仿真和恢复测试。

## 二十、验收

- qrp-trading生产窄协议和版本语义封板；
- request/client_order幂等和稳定错误码完整；
- WebSocket事件可去重、可恢复、可对账；
- qrp-atlas `MiniQmtGatewayAdapter` 完整实现 `ExecutionGatewayProtocol`；
- qrp-atlas不依赖xtquant和Windows环境；
- 查询、下单、撤单、事件、断线、重启和三方对账可用；
- READONLY和仿真端到端通过；
- LIVE默认关闭并受双重门禁；
- 两个仓库各自测试和跨服务集成测试通过；
- qrp-atlas工作分支PR目标为`develop/v1.1`；
- 本地qrp-trading交付有独立提交、版本和验收记录。
