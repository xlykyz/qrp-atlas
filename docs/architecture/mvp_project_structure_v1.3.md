# qrp-atlas --- MVP Project Structure (SSOT Blueprint)

> 本文档定义 qrp-atlas 在 MVP 阶段的唯一项目结构标准。
> 基于原始版本，仅对齐最新讨论的 **Dual‑SSOT 与 scripts/src
> 职责边界**，不改变原有的结构表达方式。

------------------------------------------------------------------------

# 一、核心原则（MVP 必须遵守）

Dual SSOT 定义：

Runtime SSOT：`data/db/quant.db`\
Recovery SSOT：`data/canonical/**`

说明：

-   quant.db 是日常运行唯一事实源。
-   canonical 作为数据库重建备份层，长期维护。
-   raw/daily_snapshot 是唯一允许的增量入口。
-   raw 数据只写不改。
-   所有路径只能来自 `config/paths.py`。
-   所有字段结构只能来自 `contracts/schema`。
-   scripts 仅允许临时性脚本，不作为长期入口。
-   所有需要反复执行的能力必须写入 src。

------------------------------------------------------------------------

# 二、项目文件骨架（最终定型版）

``` text
qrp-atlas/
├─ README.md
├─ pyproject.toml
├─ .gitignore
├─ .env.example
├─ docs/
│  └─ architecture/
│     └─ mvp_project_structure.md
├─ data/
│  ├─ raw/
│  │  └─ daily_snapshot/
│  ├─ canonical/                # ⭐ Recovery SSOT（数据库重建来源）
│  └─ db/
│     └─ quant.db               # ⭐ Runtime SSOT
├─ scripts/                     # 临时脚本，仅一次性使用
└─ src/
   └─ qrp_atlas/
      ├─ __init__.py
      ├─ config/
      │  ├─ __init__.py
      │  ├─ paths.py
      │  ├─ settings.py
      │  └─ tushare.py
      ├─ contracts/
      │  ├─ __init__.py
      │  ├─ fields.py
      │  ├─ schema.py
      │  ├─ mappings.py
      │  ├─ validate.py
      │  └─ conventions.py
      ├─ sources/                     # 数据源适配器（待扩展）
      │  ├─ __init__.py
      │  └─ eastmoney.py
      └─ pipeline/
         ├─ __init__.py
         ├─ duckdb_store.py
         ├─ raw_store.py              # TODO
         ├─ canonical_store.py        # TODO
         ├─ canonicalize_snapshot.py  # TODO
         ├─ canonicalize_daily_bar.py # TODO
         └─ daily_update/             # ⭐ 每日更新工作流（已实现）
            ├─ __init__.py
            ├─ run.py
            ├─ fetch.py
            ├─ clean.py
            ├─ enrich.py
            └─ load_duckdb.py
```

------------------------------------------------------------------------

# 三、数据流（Runtime Flow）

``` text
daily_update/run.py (src)
        ↓  fetch()
data/raw/daily_snapshot/{date}_{source}.csv
        ↓  clean()
data/canonical/daily_market_snapshot/{date}.csv
        ↓  enrich() + load_duckdb()
data/db/quant.db   ← Runtime SSOT
        ↓
web/app.py  →  Streamlit 交互可视化
```

------------------------------------------------------------------------

# 四、Recovery 数据流（数据库重建）

``` text
data/canonical/**
        ↓
duckdb_store
        ↓
rebuild quant.db
```

------------------------------------------------------------------------

# 五、SSOT 设计说明

## 1）路径 SSOT

文件：

src/qrp_atlas/config/paths.py

规则：

-   禁止任何模块自行拼路径。
-   quant.db、raw、canonical 必须通过 paths.py 获取。

------------------------------------------------------------------------

## 2）字段 SSOT（Data Contract）

文件：

src/qrp_atlas/contracts/

职责：

-   定义 canonical schema。
-   定义数据库字段结构。
-   定义 ticker 与日期规范。

要求：

canonical schema 变更必须保持向后兼容，以确保数据库可重建。

------------------------------------------------------------------------

# 六、scripts 与 src 边界

scripts：

-   一次性迁移脚本
-   临时调试脚本
-   历史遗留工具

src：

-   所有长期能力
-   每日更新流程
-   数据处理与入库逻辑

------------------------------------------------------------------------

# 七、未来扩展（暂不实现）

-   tests/                          # 单元测试与集成测试
-   features/                       # 策略/因子模块
-   多数据源 adapter 完善            # sources/ 层适配器正式化
-   Multi-Agent 协同层               # Sentinel / Narrative / Integrity Agent
-   微服务 / 容器化

---

> **已落地**：Streamlit 可视化层已实现在 `web/`，多源数据采集（tushare/新浪/东方财富）已在 `daily_update/` 中通过 akshare 直接实现，`sources/` 层为后续 adapter 抽象预留。

------------------------------------------------------------------------

# 版本

Architecture Version: MVP-1.3 (Dual SSOT Aligned) Status: Active
