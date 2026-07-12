# QRP 临时只读远程访问工具

这是一个**开发辅助工具**，独立于 QRP 正式 FastAPI 服务运行。它只读取 contracts 明确允许的 DuckDB 市场研究数据；不会注册或修改 `src/qrp_atlas/api/` 路由，也不会写数据库、执行 SQL、运行 pipeline/backtest 或读取本地文件。

## 启动与关闭

在仓库根目录执行：

```bash
tools/remote_access/start.sh
tools/remote_access/status.sh
tools/remote_access/stop.sh
```

`start.sh` 生成随机 token、启动仅监听 `127.0.0.1:8765` 的独立 Uvicorn 进程，并运行 `cloudflared tunnel --url http://127.0.0.1:8765`。如果系统没有 `cloudflared`，脚本会把官方 Cloudflare 发布二进制下载到未提交的 `.runtime/remote_access/bin/`。不需要域名、Cloudflare 账户或登录；每次启动会生成新的 `trycloudflare.com` 地址和 token。

运行状态（PID、日志、token、临时公网地址）全部位于 `.runtime/remote_access/`，已被 Git 忽略。停止脚本只终止 PID 文件所指向且命令行匹配本工具的 API/tunnel 进程，并清理这些状态文件。

## 安全模型

- DuckDB 以 `read_only=True` 打开；应用没有写入、DDL、任意 SQL、上传、Shell 或任务执行接口。
- 除无敏感内容的 `GET /health` 外，所有接口都要求 `Authorization: Bearer <token>`。
- token 每次 `start.sh` 随机生成，权限为 owner-only；不会写进 Git、URL 或日志。停止后 token 文件被删除。
- 表、字段、排序字段、过滤字段和操作符均基于 contracts 白名单校验；所有值使用 DuckDB 参数绑定。
- 每次查询最多返回 200 行，最大 offset 为 100000，最多 20 个过滤器，`in` 过滤器最多 50 个值；请求超时为 15 秒。
- 不开放 `trade_execution`、本地配置、日志、凭证、文件索引或全文研究资料表。

## 已开放表

`stock_info`、`trading_calendar`、`daily_market_snapshot`、`index_daily`、`adj_factor_changes`、`daily_basic`、`zt_pool`、`dt_pool`、`suspend_d`、`market_phase`。

表结构直接由 `qrp_atlas.contracts.TABLE_BY_NAME` 生成；中央白名单在 `tools/remote_access/config.py` 的 `REMOTE_TABLES`。

## 接口与示例

| Method | Path | 说明 |
| --- | --- | --- |
| GET | `/health` | 最小健康/只读连通性检查，无鉴权 |
| GET | `/v1/meta` | 用途、限制与接口元信息 |
| GET | `/v1/tables` | 已开放表列表 |
| GET | `/v1/tables/{table}/schema` | contracts 表结构 |
| GET | `/v1/tables/{table}/overview` | 行数、日期范围、更新时间/资产数（适用时） |
| POST | `/v1/tables/{table}/query` | 受控分页数据查询 |

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

`filters.operator` 只允许 `eq`、`ne`、`gt`、`gte`、`lt`、`lte`、`in`。请求体中包含 `sql` 等未定义字段会被拒绝；未开放表、字段或大于 200 的 `limit` 也会被拒绝。
