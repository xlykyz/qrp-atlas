# KI-013 处理文档

> 关联 Known Issue：`docs/engineering/known-issues/KI-013-open.yaml`
> 主题：历史重放相关表的时间语义不统一，`stock_info` 等表存在 PIT 泄漏风险。
> 处理方向：将依赖 `stock_info` 的消费点逐个解耦（目标：全部解耦）。

本目录 `docs/engineering/issue-resolutions/` 用于存放已登记 Known Issue 的**处理/修复过程文档**，
与只做问题登记的 `known-issues/` 分开。每个 KI 一个处理文档，记录逐项改动的背景、方案、验证与状态。

---

## 背景

KI-013 确认 `stock_info` 是 current snapshot 表（主键 `(ticker,)`，仅 `updated_at`，无
`available_trade_date`/`revision_id`/`ingested_at`），且其数据源 Tushare `stock_basic` 接口**无
`trade_date` 参数、不提供历史**，因此对该表做 revision-PIT 重构缺乏历史数据来源。

经确认，改为**逐点解耦**：把运行时读取 `stock_info` 的消费点一个一个拆掉，使系统不再把
`stock_info` 当作历史/canonical 事实源。

## 消费点清单（共 10 处）

| # | 位置 | 依赖字段 | 用途 | 状态 |
| --- | --- | --- | --- | --- |
| 1 | `api/routes/stock.py:39-44` | ticker,name,exchange,market,list_date,delist_date,is_active | 股票列表 API | 待处理 |
| 2 | `api/routes/stock.py:57-61` | 同上 + updated_at | 股票详情 API | 待处理 |
| 3 | `api/routes/system_b.py:306-307` | ticker,name | System B 活跃 episode 取显示名 | 待处理 |
| 4 | `stock_collections/repository.py:179-183` | ticker | 校验资产为有效 EQUITY | **已解耦** |
| 5 | `pipeline/market_facts.py:60-77` | ticker,list_date,delist_date | 市场事实域 | 待处理 |
| 6 | `pipeline/market_m6/service.py:107-117` | ticker,market,exchange | M6 子市场映射 | 待处理 |
| 7 | `pipeline/market_m6/query.py:127-130` | ticker,market,exchange | M6 查询 | 待处理 |
| 8 | `pipeline/system_b_asset_rank/service.py:177-188` | ticker,list_date,delist_date(+exchange,market,list_status 可选) | 目标日 canonical A 股域 | 待处理 |
| 9 | `pipeline/system_b/repository.py:225-230` | ticker,list_date,delist_date | System B 域 | 待处理 |
| 10 | `pipeline/daily_update/enrich.py:59-74` | ticker,name | 补全缺失股票名 | 待处理 |

---

## 改动 1：消费点 #4 —— `check_is_equity` 解耦

- **日期**：2026-09-14
- **分支**：`refactor/v1.1-decouple-stock-info`（基于 `origin/develop/v1.1`）
- **提交**：`28d0cd2` refactor(stock-collections): decouple check_is_equity from stock_info

### 位置

`src/qrp_atlas/stock_collections/repository.py` —— `StockCollectionRepository.check_is_equity`

### 原行为

```python
def check_is_equity(self, asset_id: str) -> bool:
    """Verify asset is a valid EQUITY in stock_info (if stock_info exists)."""
    tables = [...information_schema.tables...]
    if "stock_info" in tables:
        row = self.con.execute(
            "SELECT COUNT(*) FROM stock_info WHERE ticker = ?", [asset_id]
        ).fetchone()
        return bool(row and row[0] > 0)
    # Fallback check on asset_id format
    return isinstance(asset_id, str) and len(asset_id) >= 6 and (
        asset_id.endswith(".SH") or asset_id.endswith(".SZ") or asset_id.endswith(".BJ")
    )
```

当 `stock_info` 存在时，校验"该 ticker 是否真实存在于 `stock_info`"；不存在时退化为格式校验。

### 改后行为

删除 `stock_info` 查询分支，方法固定为纯**交易代码格式校验**（`.SH/.SZ/.BJ` 后缀）：

```python
def check_is_equity(self, asset_id: str) -> bool:
    """Verify asset_id has a valid EQUITY trading-code format. ..."""
    return isinstance(asset_id, str) and len(asset_id) >= 6 and (
        asset_id.endswith(".SH") or asset_id.endswith(".SZ") or asset_id.endswith(".BJ")
    )
```

### 行为变化

| 输入 | 改前（`stock_info` 存在时） | 改后 |
| --- | --- | --- |
| `INVALID_BOND_001` | `False` | `False`（格式不符） |
| `999999.SZ` | `False`（不在表） | `True`（格式合法） |
| `000001.SZ` | `True` | `True` |

净效果：校验强度从「真实股票集合」退化为「格式合法性」，失去对"格式合法但不存在"的野代码拦截。
调用点（`service.py:197` `add_member`、`service.py:434` `add_members_batch`）无需改动，
`NON_EQUITY_ASSET` 错误路径保持。

### 验证

| 命令 | 结果 |
| --- | --- |
| `.venv/bin/python -m pytest tests/stock_collections/ -q` | 15 passed |
| `.venv/bin/python -m pytest tests/stock_collections/ tests/pipeline/theme/ tests/pipeline/test_theme_contracts.py -q` | 75 passed |

该文件已无 `stock_info` 引用；`git diff` `+6/-14`，仅 1 个文件。

---

## 进度

- 已解耦：**1 / 10**
- 下一个：消费点 #1 `api/routes/stock.py:39-44`（股票列表 API）