# QRP 临时只读远程访问工具

这是一个**开发辅助工具**，独立于 QRP 正式 FastAPI 服务运行。它只读取 contracts 明确允许的 DuckDB 市场研究数据；不会注册或修改 `src/qrp_atlas/api/` 路由，也不会写数据库、执行任意 SQL、运行 pipeline/backtest 或读取任意本地文件。

工具提供两种访问方式：

1. Bearer Token API：适合脚本、curl 和能够设置请求头的客户端；
2. URL Capability Session：适合浏览器或不能设置 Authorization 请求头的临时只读访问场景。

两种方式共用同一个只读网关和表/字段白名单，但鉴权方式、接口形态和单次返回上限不同。

## 启动与关闭网关

在仓库根目录执行：

```bash
tools/remote_access/start.sh
tools/remote_access/status.sh
tools/remote_access/stop.sh
```

`start.sh` 会：

- 启动仅监听 `127.0.0.1:8765` 的独立 Uvicorn 进程；
- 生成 Bearer Token；
- 运行 `cloudflared tunnel --url http://127.0.0.1:8765`；
- 在未安装 `cloudflared` 时，将官方二进制下载到未提交的 `.runtime/remote_access/bin/`；
- 输出临时 `trycloudflare.com` 公网地址。

不需要自有域名、Cloudflare 账户或登录。Quick Tunnel 地址和 Token 每次启动都可能变化。

运行状态、PID、日志、Token、Capability Session 和临时公网地址位于：

```text
.runtime/remote_access/
```

该目录已被 Git 忽略。停止脚本只终止 PID 文件指向且命令行匹配本工具的 API/tunnel 进程，并清理运行状态。

## 方式一：Bearer Token API

除 `GET /health` 外，Bearer API 请求都需要：

```text
Authorization: Bearer <token>
```

接口：

| Method | Path | 说明 |
| --- | --- | --- |
| GET | `/health` | 最小健康/只读连通性检查，无鉴权 |
| GET | `/v1/meta` | 用途、限制与接口元信息 |
| GET | `/v1/tables` | 已开放表列表 |
| GET | `/v1/tables/{table}/schema` | contracts 表结构 |
| GET | `/v1/tables/{table}/overview` | 行数、日期范围、更新时间/资产数 |
| POST | `/v1/tables/{table}/query` | 受控分页数据查询 |

示例：

```bash
BASE_URL='https://example.trycloudflare.com'
TOKEN='replace-with-start-output'
AUTH="Authorization: Bearer $TOKEN"

curl -sS -H "$AUTH" "$BASE_URL/v1/meta"
curl -sS -H "$AUTH" "$BASE_URL/v1/tables/daily_market_snapshot/schema"
curl -sS -X POST -H "$AUTH" -H 'Content-Type: application/json' \
  "$BASE_URL/v1/tables/daily_market_snapshot/query" \
  --data '{
    "fields": ["trade_date", "ticker", "name", "close", "pct_change"],
    "filters": [{"field": "ticker", "operator": "eq", "value": "000001.SZ"}],
    "date_from": "2024-01-01",
    "date_to": "2024-01-31",
    "order_by": "trade_date",
    "order_direction": "desc",
    "limit": 10
  }'
```

Bearer 查询单次最多返回 200 行，最大 offset 为 100000，最多 20 个过滤器，`in` 过滤器最多 50 个值。

## 方式二：URL Capability Session

Capability Session 将临时凭证放在不可猜测的 URL 路径中，适用于不能方便设置 Bearer 请求头的只读客户端。

网关启动后执行：

```bash
tools/remote_access/share_start.sh
tools/remote_access/share_status.sh
tools/remote_access/share_stop.sh
```

默认会话持续 30 分钟。可以在创建前设置：

```bash
QRP_SHARE_DURATION_MINUTES=60 tools/remote_access/share_start.sh
```

允许范围为 1～1440 分钟。`share_start.sh` 会生成新的随机 session ID，保存 owner-only 会话文件，并输出类似入口：

```text
https://example.trycloudflare.com/share/<session_id>
```

常用完整地址：

| Method | Path | 说明 |
| --- | --- | --- |
| GET | `/share/{session_id}/meta` | 会话与能力元信息 |
| GET | `/share/{session_id}/tables` | 已开放表列表 |
| GET | `/share/{session_id}/tables/{table}/schema` | 表结构 |
| GET | `/share/{session_id}/tables/{table}/overview` | 表概览 |
| GET | `/share/{session_id}/tables/{table}/query` | URL 查询参数形式的数据查询 |

Capability 查询单次最多返回 50 行。字段列表使用逗号分隔；复杂过滤器使用 URL 编码后的 JSON。

示例：

```text
/share/<session_id>/tables/daily_market_snapshot/query
  ?fields=trade_date,ticker,close
  &date_from=2024-01-01
  &date_to=2024-01-31
  &order_by=trade_date
  &order_direction=desc
  &limit=10
```

会话安全语义：

- session ID 是临时凭证，不应公开传播；
- 到期、撤销或重新生成后，旧会话失效；
- `share_stop.sh` 撤销当前 Capability Session，不替代 `stop.sh` 关闭整个网关；
- Capability Session 不改变 Bearer API 的行为；
- 即使 URL 泄露，能力仍受只读、白名单、行数和会话期限约束。

## 安全模型

- DuckDB 以 `read_only=True` 打开；
- 应用没有写入、DDL、任意 SQL、上传、Shell、文件读取或任务执行接口；
- 表、字段、排序字段、过滤字段和操作符均基于 contracts 白名单校验；
- 所有查询值使用 DuckDB 参数绑定；
- Bearer Token、session ID、运行状态和公网地址不会提交 Git；
- 请求存在行数、offset、过滤器和超时限制；
- 不开放 `trade_execution`、认证凭证、本地配置、日志、文件索引或全文研究资料表；
- 本工具是临时开发入口，不替代正式认证、权限、部署和审计系统。

## 已开放表

当前白名单包括：

```text
stock_info
trading_calendar
daily_market_snapshot
index_daily
adj_factor_changes
daily_basic
zt_pool
dt_pool
suspend_d
market_phase
```

表结构直接由 `qrp_atlas.contracts.TABLE_BY_NAME` 生成；中央白名单位于 `tools/remote_access/config.py` 的 `REMOTE_TABLES`。

## 查询规则

Bearer POST 和 Capability GET 最终都经过同一受控查询逻辑：

- `filters.operator` 只允许 `eq`、`ne`、`gt`、`gte`、`lt`、`lte`、`in`；
- 未开放表或字段会被拒绝；
- 排序字段必须属于表白名单；
- `date_from` 不得晚于 `date_to`；
- limit 超过对应模式上限会被拒绝；
- 请求不能提供任意 SQL，也不能通过字段、排序或过滤器注入 SQL。

## 使用边界

- 该工具适合临时检查本地市场数据、验证 contracts 和向受信客户端提供短时只读能力；
- 不用于长期稳定公网服务；
- 不用于远程执行 pipeline、回测、写库、文件管理或实盘交易；
- Quick Tunnel 本身不保证固定域名或长期稳定性；需要长期服务时应使用正式部署方案。