# 任务 03-B：Tushare 数据底座（部分完成）

状态：**任务 03 数据底座部分完成**（本轮不标记任务 03 整体完成）。  
分支：`feature/pit-data-pipeline`  
范围：contracts + pipeline + 本地 DuckDB 小范围真实入库；不包含历史查询服务、因子、策略、API/前端。

## 1. Tushare 接口真实审计结果

数据源：项目现有 Tushare Pro 代理配置（`get_tushare_pro`，限速约 0.6s/次）。  
审计方式：小范围真实请求，不依赖文档臆测。

### 1.1 财务四表 VIP

| 接口 | 调用方式 | 审计样本 | 关键观察 |
|---|---|---|---|
| `income_vip` | `period=YYYYMMDD` | `20231231` 全市场约 6733 行；样本 `000001/600519/300750` | 含 `ann_date/f_ann_date/end_date/report_type/comp_type/end_type/update_flag` 与利润表科目；金额单位为元 |
| `balancesheet_vip` | 同上 | `20240630` 约 5882 行 / 152 列 | 元数据同上；核心科目 `total_assets/total_liab/...` |
| `cashflow_vip` | 同上 | `20240630` 约 6400 行 / 97 列 | 元数据同上；核心科目 `n_cashflow_act/...` |
| `fina_indicator_vip` | 同上 | `20240630` 约 7254 行 / 109 列 | **无** `f_ann_date`、**无** `report_type`；有 `ann_date/end_date/update_flag` 与 ROE/EPS 等 |

补充观察：

- VIP 按 `period` 返回当期全市场（或近似全市场）行，适合后续按报告期回填。
- 同票同报告期可出现 `update_flag=0/1` 两行；`000001.SZ` 历史单票查询可见多期 `update_flag` 双版本。
- 本轮小样本 `income_vip(period=20231231/20240630)` 过滤 4 个 ticker 后入库时，最终保留版本以 `update_flag=1` 为主；未发现需要伪造的“内容不同但仅靠 ann 变化”的修订样本。
- 金额/指标均为数值型；财务三表金额为人民币元，指标为比率/每股值（按 Tushare 口径）。

### 1.2 行业历史

| 接口 | 调用方式 | 观察 |
|---|---|---|
| `index_member_all` | `ts_code` / `l1_code` / `l2_code` / `l3_code`，可选 `is_new` | 返回三级申万：`l1/l2/l3_code+name`，`in_date/out_date/is_new` |

补充：

- 按 ticker 拉取当前成员时，常见 `out_date=None, is_new=Y`。
- 按行业码 + `is_new=N` 可拿到退出记录；真实入库中 `industry_membership_history.effective_to is not null` 共 168 行（如 `002028.SZ` 等）。
- 单次按 `l1_code=801730.SI, is_new=N` 约 56 行原始成员记录，展开三级后 168 行。

### 1.3 指数成分权重

| 接口 | 调用方式 | 观察 |
|---|---|---|
| `index_weight` | `index_code + start_date + end_date` | 月度/相邻快照；`000300.SH` 在 2024-01~03 返回 6 个 `trade_date`，各 300 成分，权重和≈100 |

快照日期样例：`20240102/20240131/20240201/20240229/20240301/20240329`。

### 1.4 交易日历

本地 `trading_calendar` 当前仅存储开市日（`is_open=True` 全表 8797 行）。  
`available_trade_date` 计算采用“严格晚于事件日的下一个开市日”。

若事件日等于或晚于交易日历最后一天，导致找不到后续开市日，则 **明确抛错**：

```text
No open trade date found after YYYY-MM-DD
```

不会静默回退到自然日 `+1`，也不会生成周末/节假日。需要先更新 `trading_calendar` 再重跑。

## 2. 六张表与主键

| 表 | 主键 | 说明 |
|---|---|---|
| `income_statement` | `revision_id` | 利润表修订追加 |
| `balance_sheet` | `revision_id` | 资产负债表修订追加 |
| `cashflow_statement` | `revision_id` | 现金流量表修订追加 |
| `financial_indicator` | `revision_id` | 财务指标修订追加 |
| `industry_membership_history` | `revision_id` | 申万三级展开后的成员历史 |
| `index_component_history` | `revision_id` | 指数成分权重快照 |

统一元数据字段：`source / source_record_id / revision_id / ingested_at`。  
财务表另含：`ticker, report_period, announcement_date, published_at, available_trade_date, report_type, update_flag`（指标表无 `report_type/f_ann_date`）。

## 3. 字段映射与单位

映射定义于 `src/qrp_atlas/contracts/mappings.py`：

- `TUSHARE_INCOME` / `TUSHARE_BALANCESHEET` / `TUSHARE_CASHFLOW` / `TUSHARE_FINA_INDICATOR`
- `TUSHARE_INDEX_MEMBER_ALL` / `TUSHARE_INDEX_WEIGHT`

关键映射：

```text
ts_code -> ticker / asset_id
end_date -> report_period
ann_date -> announcement_date
f_ann_date -> f_ann_date
trade_date(index_weight) -> snapshot_date
con_code -> asset_id
in_date/out_date -> effective_from/effective_to
```

单位约定：

- 财务三表金额：元
- 财务指标：按 Tushare 原始口径（ROE/ROA 为百分比数值，EPS/BPS 为元）
- 指数权重：百分比权重（成分权重和约 100）

## 4. 版本规则

- 不以 `ticker + report_period` 作为可覆盖唯一键。
- `source_record_id`：业务键稳定哈希（表名 + ticker + period + report_type/ann/update_flag 等）。
- `revision_id`：业务键 + 核心内容字段的稳定内容哈希。
- 相同内容重复抓取：同一 `revision_id`，入库 0 行新增。
- 内容变化：新 `revision_id`，**追加**旧版本保留。
- 不使用随机 UUID。

## 5. `available_trade_date` 规则

在没有可信公告时分秒时：

```text
published_at = NULL
announcement_date = f_ann_date 优先，否则 ann_date
available_trade_date = announcement_date 之后的首个开市日
```

因此：

- 公告当天不可用；
- 周末/节假日公告映射到后续首个开市日；
- 行业成员 `available_trade_date` 基于 `effective_from` 的下一开市日；
- 指数权重快照 `available_trade_date` 基于 `snapshot_date` 的下一开市日（保守、无日内时点）。

真实样本：`000001.SZ` 2023 年报 `announcement_date=2024-03-15` → `available_trade_date=2024-03-18`。

## 6. 行业与指数区间语义

### 行业

- classification_system 固定 `sw2021`（申万层级来自 `index_member_all`）。
- 每条原始成员记录展开为 level=1/2/3 三行。
- `effective_from=in_date`，`effective_to=out_date`（空表示仍有效）。

### 指数（快照模型，非精确成员生效区间）

- `snapshot_date` 保留 Tushare `trade_date`（月度权重快照）。
- `effective_from = snapshot_date`，仅作统一时间字段兼容。
- `effective_to` **不由单次 Pipeline 构造**，统一保持为空。
- 不同 `index_code` 完全隔离；禁止跨指数拼接“下一快照”。
- 分批回填：各批次独立按 `revision_id` 幂等追加快照，不依赖前后批次衔接。
- 注意：`index_weight` 是月度权重快照近似，**不是**精确的历史成员生效区间。

后续任务 03-C 查询某个 `as_of_date` 时，按：

```text
同一 index_code
snapshot_date <= as_of_date
available_trade_date <= as_of_date
选择最新 snapshot_date
```

取得当时可用的完整成分快照。

## 6.1 验收修正（PR 跟进）

1. 交易日历越界默认 `raise`，禁止自然日回退。
2. 指数采用**快照模型**：不在单批 fetch 内固化相邻 `effective_to`。
3. 多指数隔离与分批回填由测试覆盖。
4. 运行入口在仅传入 `db_path` 时，使用同一 `db_path` 读取交易日历。

## 7. Pipeline 入口

### 财务

```bash
# 小范围验证 / 指定报告期
python -m qrp_atlas.pipeline.fundamentals.run \
  --tables all \
  --periods 20231231,20240630 \
  --tickers 000001.SZ,600519.SH,300750.SZ,000002.SZ

# 按 ticker 补采
python -m qrp_atlas.pipeline.fundamentals.run \
  --mode ticker \
  --tables income_statement \
  --tickers 000001.SZ \
  --periods 20231231,20240630
```

### 行业

```bash
python -m qrp_atlas.pipeline.industry_membership.run \
  --tickers 000001.SZ,600519.SH,300750.SZ

# 后续按行业补历史（手动，不进默认任务）
python -m qrp_atlas.pipeline.industry_membership.run --l1-code 801730.SI --is-new N
```

### 指数

```bash
python -m qrp_atlas.pipeline.index_component.run \
  --index-code 000300.SH \
  --start 20240101 \
  --end 20240331
```

### 迁移

```bash
python scripts/migrate_pit_tables.py --db-path data/db/quant.db
```

迁移幂等：只 `CREATE TABLE IF NOT EXISTS` 六张新表；默认先备份数据库。

## 8. 数据库迁移与备份

- 正式库路径：`data/db/quant.db`（`qrp_atlas.config.paths.DB_PATH`）
- 本轮备份：`data/db/quant.backup_pit_20260713_234807.db`
- 备份与数据库文件均不提交 git

## 9. 真实入库样本（小范围）

财务：

- tickers：`000001.SZ, 600519.SH, 300750.SZ, 000002.SZ`
- periods：`20231231, 20240630`
- 行数：income 8 / balance 11 / cashflow 10 / fina 9
- 四表均成功入库；`report_type=1`、`update_flag` 保留
- 二次执行 inserted=0（幂等）

行业：

- 指定股票 6 只 → 18 行（三级展开）
- 额外小范围 `is_new=N` 行业查询补充退出样本
- 总行数 186；其中 `effective_to is not null` = 168

指数：

- `000300.SH`，2024-01-01~2024-03-31
- 6 个快照 × 300 = 1800 行
- 快照模型：`effective_from=snapshot_date`，`effective_to` 为空
- 二次执行 inserted=0

## 10. 幂等验证

| 对象 | 首次 inserted | 二次 inserted | 最终 count |
|---|---:|---:|---:|
| income_statement | 8 | 0 | 8 |
| balance_sheet | 11 | 0 | 11 |
| cashflow_statement | 10 | 0 | 10 |
| financial_indicator | 9 | 0 | 9 |
| industry_membership_history（指定股票） | 18 | 0 | — |
| index_component_history | 1800 | 0 | 1800 |

DuckDB 端到端测试额外覆盖：内容变化时新 `revision_id` 追加，旧行保留。

## 11. 测试结果

```bash
python -m pytest tests/pipeline/test_pit_data_pipeline.py tests/contracts/test_schema_contracts.py -q
```

相关测试通过（六表 contracts、mapping、稳定哈希、下一交易日/周末公告、四表 fake pipeline、行业、指数区间、DuckDB 幂等与追加）。

交付前另运行：

```bash
python -m pytest
python -m compileall -q src
git diff --check
```

## 12. 已知限制

1. 本轮**不做**全市场/全历史正式回填，仅验证链路与小样本。
2. 未开发 point-in-time 查询服务（任务 03 后续）。
3. `published_at` 因无可信时分秒，统一为 `NULL`。
4. 本地 `trading_calendar` 仅含开市日；若日历缺口会导致 `available_trade_date` 回退启发式。
5. `index_weight` 是月频近似，不是日频精确成分权重。
6. 财务真实样本中，小范围未稳定遇到“同内容键不同科目”的现场修订对；版本追加语义由测试覆盖。检索范围：VIP 两期 + `000001.SZ` 2020-2024 单票 income。
7. `index_member_all` 网关偶发 SSL/空结果；fetch 层有重试，但不保证第三方代理 100% 稳定。
8. 任务 03 仅数据底座部分完成，不能标记整体完成。

## 13. 回填入口（供后续手动执行，不默认启动）

```text
按报告期回填财务：fundamentals.run --mode period --periods ...
按 ticker 补采财务：fundamentals.run --mode ticker --tickers ...
按股票/行业补行业历史：industry_membership.run --tickers ... / --l1-code ...
按指数+日期区间回填成分：index_component.run --index-code ... --start ... --end ...
失败后断点续跑：重复执行同一命令（按 revision_id 幂等）
```

不要把上述回填并入默认 daily pipeline。
