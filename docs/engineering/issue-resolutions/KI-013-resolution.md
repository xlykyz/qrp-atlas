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
| 1 | `api/routes/stock.py:39-44` | ticker,name,exchange,market,list_date,delist_date,is_active | 股票列表 API | **低优先/展示层**（见改动 4，本次不处理） |
| 2 | `api/routes/stock.py:57-61` | 同上 + updated_at | 股票详情 API | **低优先/展示层**（见改动 4，本次不处理） |
| 3 | `api/routes/system_b.py:306-307` | ticker,name | System B 活跃 episode 取显示名 | **低优先/展示层**（见改动 4，本次不处理） |
| 4 | `stock_collections/repository.py:179-183` | ticker | 校验资产为有效 EQUITY | **已解耦** |
| 5 | `pipeline/market_facts.py:60-77` | ticker,list_date,delist_date | 市场事实域 | **豁免不改**（见改动 2） |
| 6 | `pipeline/market_m6/service.py:107-117` | ticker,market,exchange | M6 子市场映射 | **已解耦**（见改动 3） |
| 7 | `pipeline/market_m6/query.py:127-130` | ticker,market,exchange | M6 查询 | **已解耦**（见改动 3） |
| 8 | `pipeline/system_b_asset_rank/service.py:177-188` | ticker,list_date,delist_date(+exchange,market,list_status 可选) | 目标日 canonical A 股域 | **豁免不改**（见改动 6） |
| 9 | `pipeline/system_b/repository.py:225-230` | ticker,list_date,delist_date | System B 域 | 待处理 |
| 10 | `pipeline/daily_update/enrich.py:59-74` | ticker,name | 补全缺失股票名 | **低优先/展示层**（见改动 4，本次不处理） |

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

## 改动 2：消费点 #5 —— `market_facts.py` 豁免不改（判定记录）

- **日期**：2026-09-14
- **结论**：**不改代码**（豁免）
- **提交**：无（仅本判定记录）

### 位置

`src/qrp_atlas/pipeline/market_facts.py:60-77` —— `query_confirmed_listing_facts`
（`domain` CTE：`FROM stock_info AS stock`，用 `list_date`/`delist_date` × `trading_calendar` 生成预期域）。

### 判定依据（代码核实）

该消费点仅读取 `ticker`、`list_date`、`delist_date` 三个字段（grep 确认**不读** `list_status`/`is_active`）：

- `ticker`：身份键，不随快照变化；
- `list_date`：上市事件日期，事件发生后固定；
- `delist_date`：退市事件日期，仅在退市事件时由 NULL 变为固定日期，此后固定。

三者均为**不可变事件/身份事实**，非 current-state 字段。读"当前快照值"与读"历史 as-of 值"语义等价，
故**不构成 PIT 泄漏**。

对照：真正有 PIT 风险的是 `list_status`/`is_active` 这类随每日快照变化的 current-state 字段，
但它们**不在本消费点**，而在 `api/routes/stock.py:40,59`（`is_active`）与
`pipeline/system_b_asset_rank/service.py:185`（`list_status` 可选列）等消费点。

### 关于"上市状态会造成风险"的界定

- `list_status`/`is_active` 属 current-state，**确实有 PIT 风险** ✅；
- 但该风险**不属于本消费点**（本点不读这两个字段）；
- "目前没有实质性 PIT 错误产生" —— 这是**运行期事实**，代码无法证实或证伪。
  代码只能证明：消费路径存在、字段随快照变化；**是否真的产生过错误结果**取决于运行期是否有人以历史
  日期调用这些 API / 依赖 as-of 语义。因此准确表述为**"未观察到实质性 PIT 错误"**，而非"确认没有"。

### 残留风险（非字段值，而是行集）

真正可能随每日覆盖变化的是**行集**：若 provider 某次不再返回某只已退市股票，`domain` 会缺其历史。
缓解证据：`stock_basic_contracts.py:76` 拉取范围 `STOCK_BASIC_LIST_STATUSES = ("L","D","P","G")`
**包含 D（退市）**，设计上即为保留退市股票，故行集风险在实践中较低。

### 补充议题（本次不处理）

- 概念上 `ts_code` 才是 provider 原始身份，`ticker` 是"标准交易代码兼容字段"；但本表中
  `ticker ≡ ts_code`（`stock_basic_contracts.py:213` 赋值、`:394-395` 强制相等校验），
  故主键选哪个在数据上等价。是否改为 `ts_code` 属**独立的主键语义重构**，与解耦分开处理，本次不动。

---

## 改动 3：消费点 #6 / #7 —— M6 子市场映射解耦

- **日期**：2026-09-14
- **分支**：`refactor/v1.1-decouple-stock-info`
- **提交**：`refactor(market-m6): derive market scope from ticker prefix`

### 位置

- `src/qrp_atlas/pipeline/market_m6/service.py:107-117`（`MarketM6PipelineService.run_m6_daily`）
- `src/qrp_atlas/pipeline/market_m6/query.py:127-130`（`MarketM6QueryService.audit_m6_observation`）
- 函数 `resolve_canonical_market_scope`（`market_m6/service.py`）

### 原行为

两处均从 `stock_info` 全量读取 `(ticker, market, exchange)`，对每行调
`resolve_canonical_market_scope(market, exchange)` 建 `ticker_to_scope` 映射，再按快照 ticker 查。
`resolve_canonical_market_scope` 基于 `market`/`exchange` 字段值判断，其 docstring 明写
**"Never infer from ticker prefix."**。

### 改后行为

`resolve_canonical_market_scope` 入参改为 **`ticker`**，内部用 `conventions.get_board()`（纯前缀函数）
推导板块，再经 `_BOARD_TO_MARKET_SCOPE` 映射为 4 个 scope：

| get_board(ticker) | MARKET_SCOPE |
| --- | --- |
| 上证主板 / 深证主板（60 / 00） | `MAIN_BOARD` |
| 创业板（30） | `CHINEXT` |
| 科创板（688 / 689，**含 CDR**） | `STAR_MARKET` |
| 北交所（43/83/87/88/92） | `BSE` |
| 其他 | `None`（unresolved） |

两处消费点删除 `stock_info` 查询，改为在 `daily_market_snapshot` 的 ticker 上直接
`resolve_canonical_market_scope(ticker)`；`service.py` 中 `input_snapshot_id` 的 payload 移除
`stock_info_count` 项；`service.py` 移除未再使用的 `STOCK_INFO` 导入。

### 决策记录

- **CDR 归科创板**：provider `market` 可取 `CDR`（如 `689009.SH`），前缀落在 `689`，`get_board`
  判为科创板；经确认，CDR 直接归入 `STAR_MARKET`，不再单独区分。这是本次唯一的语义取舍点。
- **"Never infer from ticker prefix" 被推翻**：该约束是设计约定而非技术限制——前缀规则
  （`conventions.py`）对 M6 所需 4 个板块与 `market` 字段口径一一对应，唯一差异即 CDR。

### 行为变化

| 场景 | 改前 | 改后 |
| --- | --- | --- |
| `market` 字段缺失/NULL 的在市股票 | `resolve` 返回 `None` → `M6_CANONICAL_MARKET_UNRESOLVED` | 按 ticker 前缀正常映射 |
| `999999.ZZ`（非法代码） | `None`（market='未知板块'） | `None`（前缀 `99` 不匹配任何规则） |
| CDR `689009.SH` | `None`（market='CDR' 无匹配） | `STAR_MARKET` |
| 依赖 `stock_info` 行集 | 是 | 否（完全解耦） |

### 验证

| 命令 | 结果 |
| --- | --- |
| `.venv/bin/python -m pytest tests/pipeline/test_market_m6_contracts.py -q` | 7 passed |
| `.venv/bin/python -m pytest tests/pipeline/test_market_m6_contracts.py tests/pipeline/test_market_data_contracts.py tests/indicators/test_m6_observations.py tests/contracts/test_schema_contracts.py -q` | 77 passed |

测试 `test_market_scope_resolver` 的断言由 `(market, exchange)` 入参改为 ticker 入参。
`market_m6/` 已无 `stock_info` 引用；`git diff` 3 文件 `+43/-55`。

---

## 改动 4：name 相关消费点（#1 / #2 / #3 / #10）判定为低优先/展示层，本次不处理

- **日期**：2026-09-14
- **结论**：**本次不处理**，降级为"低优先/展示层"
- **提交**：无（仅本判定记录）

### 涉及消费点

| # | 位置 | name 用法 |
| --- | --- | --- |
| 1 | `api/routes/stock.py:39-44` | 列表 API 返回 `name`，并支持 `name LIKE` 模糊搜索 |
| 2 | `api/routes/stock.py:57-61` | 详情 API 返回 `name` |
| 3 | `api/routes/system_b.py:306-307` | `si.name AS name` 取显示名 |
| 10 | `pipeline/daily_update/enrich.py:59-74` | 用 `stock_info.name` 回填 `daily_market_snapshot` 缺失名称 |

### 判定依据（代码核实）

- **`name` 是 current-state 字段**：`stock_info` 每日被 `stock_basic` 全量覆盖，只保留最新名称；
  项目调研报告亦明确警告"不要假设 `stock_basic.name` 可回溯历史"（`Tushare_Pro数据调研报告_QRP_v1.0.md:349`），
  并列出 `namechange`（doc_id=100）为历史曾用名来源。
- **但 `name` 不参与任何关联/键**：全库检索确认，股票 `name` 只用于①展示返回、②`LIKE` 模糊搜索；
  **无任何 JOIN / 精确匹配 / 分组键**依赖股票名称，所有关联一律走 `ticker`（代码）。
- **主键无问题**：`stock_info.primary_key = (ticker,)`（`schema.py:437`），主键已是稳定代码，非名称。

### 风险定性

- **危害限于展示层**：历史查询会显示"当前名称"而非"当时名称"（如查历史记录却显示改名后的新名），
  属**观感/标注错误**，不造成数据关联错误、不破坏计算。
- **`enrich.py` 的特殊性**：它把 current `name` **写入** `daily_market_snapshot` 历史行，
  使历史行的展示名固化为当前名；但同样只影响展示，不影响任何计算/关联。
- 因无键依赖，"改名导致找不到数据"的场景**在当前代码中不存在**（仅当未来新增以名称为键的消费点才会出现）。

### 与 KI-013 的关系

- `name` 确属 current-state 字段，其"历史版本不可查"是 KI-013 所指"表不保留 revision"的表现之一；
- 但因不影响计算/关联，**不构成实质性 PIT 风险**，故本次不纳入解耦范围。

---

## 改动 5：M6 去掉无意义的 fail-closed raise（修复 KI-010 计算阻塞）

- **日期**：2026-09-14
- **分支**：`refactor/v1.1-decouple-stock-info`
- **提交**：`fix(market-m6): skip unmappable tickers instead of failing the whole day`
- **关联**：KI-010（M6 长期必现失败）、KI-011（标的集合不一致）

### 位置

`src/qrp_atlas/pipeline/market_m6/service.py:168-172`（`MarketM6PipelineService.run_m6_daily`）

### 原行为

循环内对无法映射 scope 的 ticker 已 `continue` 跳过（不进 `market_rows`），但循环后无条件：

```python
if unresolved_tickers:
    raise ContractError("M6_CANONICAL_MARKET_UNRESOLVED", ...)
```

只要当日存在任一不可映射标的（KI-011 的 `600849.SH`/`810011.BJ`/`810013.BJ` 占位行），
即拒绝整日、`rows_written=0`，导致 M6 每个交易日必现失败（KI-010，已 `enabled=false` 下线）。

### 改后行为

删除该 `raise`，改为 `logger.warning` 记录被排除的标的，计算照常继续：

```python
if unresolved_tickers:
    logger.warning(
        "M6: %d tickers cannot be mapped to a canonical market scope and were "
        "excluded from today's calculation: %s",
        len(unresolved_tickers),
        unresolved_tickers[:20],
    )
```

### 依据（实测）

`calculate_market_m6_observations` 对缺失板块自动填 0，实测只喂 2 个板块的行仍输出完整 5 行
（`ALL_MARKET`/`MAIN_BOARD`/`CHINEXT`/`STAR_MARKET`/`BSE`，无标的板块计数为 0）。即"丢弃 None 后
已映射板块可正常计算"——原 `raise` 是纯策略选择，非计算依赖。

### 附带修复

`today_market = pd.DataFrame(market_rows)` 在 `market_rows` 为空（全部标的不可映射）时无列，
后续访问 `IS_LIMIT_UP` 会 `KeyError`。改为显式指定列：

```python
today_market = pd.DataFrame(
    market_rows,
    columns=[TICKER, MARKET_SCOPE, IS_LIMIT_UP, IS_LIMIT_DOWN, CLOSE, "is_trading"],
)
```

### 行为变化

| 场景 | 改前 | 改后 |
| --- | --- | --- |
| 当日有 1 个不可映射标的 | 拒绝整日（`rows_written=0`） | 跳过该标的 + `warning`，正常出 5 行 |
| 当日全部标的不可映射 | 拒绝整日 | 出 5 行（各 scope 计数为 0） |
| 已映射标的的计算 | 不受影响 | 不受影响 |

### 验证

| 命令 | 结果 |
| --- | --- |
| `.venv/bin/python -m pytest tests/pipeline/test_market_m6_contracts.py -q` | 8 passed |
| `.venv/bin/python -m pytest tests/pipeline/test_market_m6_contracts.py tests/pipeline/test_market_data_contracts.py tests/indicators/test_m6_observations.py tests/contracts/test_schema_contracts.py -q` | 78 passed |

测试改造：原 `test_m6_production_fail_closed_on_unresolved_market_scope`（断言抛
`M6_CANONICAL_MARKET_UNRESOLVED`）改为两个新测试——`test_m6_production_skips_unresolved_market_scope`
（混合标的：跳过 + 正常出数）与 `test_m6_production_all_unresolved_still_emits_zeroed_scopes`
（全部不可映射：仍出 5 行、计数为 0）。全库已无 `M6_CANONICAL_MARKET_UNRESOLVED` 残留。

### 残留（未处理）

`810011.BJ`/`810013.BJ` 前缀 `81` 不在 `conventions.py` 的 `BJ_TICKER_PREFIXES=("43","83","87","88","92")`
中，`resolve_canonical_market_scope` 仍返回 `None`——但改后不再阻塞整日，仅被跳过并记录。
是否补前缀（若 `81` 是真实北交所代码段）属独立议题，本次不动。

---

## 改动 6：消费点 #8 —— `build_canonical_a_share_universe` 豁免不改（判定记录）

- **日期**：2026-09-14
- **结论**：**不改代码**（豁免）
- **提交**：无（仅本判定记录）

### 位置

`src/qrp_atlas/pipeline/system_b_asset_rank/service.py:165-218` —— `build_canonical_a_share_universe`
（`FROM stock_info`，用 `list_date`/`delist_date` 界定目标日 A 股域，再用 `_looks_like_a_share` 排除非 A 股）。

### 读取字段与逐字段判定

| 字段 | 会变？ | 影响本消费点结果？ | PIT 泄漏？ |
| --- | --- | --- | --- |
| `ticker` | 否 | — | 否（身份键） |
| `list_date` / `delist_date` | 否 | — | 否（不可变事件日期，界定域） |
| `exchange` / `market` | 是 | **否** | **否** |
| `list_status` | 是 | 否（死读取） | 否 |

### 判定依据（代码 + 实测核实）

- **`ticker`/`list_date`/`delist_date`**：与消费点 #5 同类，均为不可变事件/身份事实，非 current-state。
- **`exchange`/`market`**：虽属 current-state（provider 理论可改），但**其变化不改变本消费点的结果**：
  `_looks_like_a_share` 对标准 A 股代码（`\d{6}.SZ` 等）走 ticker 正则（第 3/4 条）直接返回 `True`，
  `exchange`/`market` 只在 `_NON_A_MARKERS` 排除分支参与。实测：
    - `000001.SZ` 的 `market` 由 `中小板`→`主板`、`exchange` 由 `SZSE`→`SZ`→`None`，结果恒为 `True`；
    - 标准 A 股的 `market`/`exchange` 值（`主板`/`创业板`/`SZSE`/`SSE` 等）均不含 `_NON_A_MARKERS`
      （`HK`/`美股`/`ETF`/`基金`/`债`…），故永不触发排除。
  即：`exchange`/`market` 变化**不影响 A 股判定**；它们真正起作用的是排除**非 A 股**（港股/美股/ETF 等），
  而"非 A 股"属性本身**稳定**（港股永远带 `HK`），故判定结果 PIT-稳定。
- **`list_status`**：死读取——全文件仅 `service.py:185` 一处出现（加入 `select`），L191-218 全程零消费。
  测试亦未构造该列（`if optional in columns` 本就跳过），删除零影响。

### 结论

本消费点所读字段**全部不构成 PIT 泄漏**（`exchange`/`market` 虽为 current-state，但变化不影响结果），
故**豁免不改**。

### 可选清理（未执行）

`service.py:185` 的 `list_status` 死读取可安全移除（改动量 1 行、零行为影响、无需改测试），
属代码卫生而非 PIT 修复；本次按"豁免不改"处理，未执行。

### 备注（判定修正过程）

本消费点的判定经两次修正：初判"含 current-state 字段 → 有 PIT 面"（未验证是否影响结果）→
实测确认 `exchange`/`market` 变化不改变结果，且非 A 股标记稳定 → 最终判定**不构成 PIT 泄漏**。

---

## 进度

- 已解耦：**3 / 10**（#4、#6、#7）
- 已豁免：**2 / 10**（#5 见改动 2、#8 见改动 6）
- 降级（低优先/展示层，本次不处理）：**4 / 10**（#1、#2、#3、#10，见改动 4）
- 附带修复：M6 fail-closed `raise` 移除（改动 5，修复 KI-010）
- 下一个：消费点 #9 `pipeline/system_b/repository.py:225-230`（System B 域）