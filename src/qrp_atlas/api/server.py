"""QRP Atlas API 服务入口"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from qrp_atlas.api.db import (
    detach_database_if_attached,
    get_db,
    get_db_path,
    require_irm_qa_db,
)
from qrp_atlas.config.settings import get_settings
from qrp_atlas.contracts import IRM_INTERACTION_QA
from qrp_atlas.api.routes import (
    declarative_strategies,
    adj_factor,
    auth,
    backtest,
    backtest_tasks,
    catalog,
    daily,
    dev,
    index,
    phase,
    research,
    stock,
    system_b,
    system_b_pools,
    tables,
    trades,
    zt_pool,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期：启动时初始化状态，关闭时清理"""
    settings = get_settings()
    app.state.settings = settings
    app.state.db_path = settings.paths.duckdb_path
    yield


# ── FastAPI 应用 ──────────────────────────────


_SETTINGS = get_settings()


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'

app = FastAPI(
    title="QRP Atlas API",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=list(_SETTINGS.api.cors_origins),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── 注册路由 ──────────────────────────────────

app.include_router(auth.router)
app.include_router(daily.router)
app.include_router(phase.router)
app.include_router(stock.router)
app.include_router(tables.router)
app.include_router(trades.router)
app.include_router(dev.router)
app.include_router(index.router)
app.include_router(zt_pool.router)
app.include_router(adj_factor.router)
app.include_router(research.router)
app.include_router(backtest.router)
app.include_router(backtest_tasks.router)
app.include_router(catalog.router)
app.include_router(declarative_strategies.router)
app.include_router(system_b.router)
app.include_router(system_b_pools.router)


# ── 系统端点 ──────────────────────────────────


@app.get("/api/health")
def health():
    """健康检查"""
    con = get_db()
    try:
        tables = con.execute("SHOW TABLES").fetchall()
        table_list = [t[0] for t in tables]
        return {"status": "ok", "tables": table_list}
    except Exception as e:
        return {"status": "error", "detail": str(e)}
    finally:
        con.close()


@app.get("/api/stats")
def stats():
    """数据库概况"""
    con = get_db()
    attached_alias: str | None = None
    try:
        result = {}
        tables = con.execute("SHOW TABLES").fetchall()
        for (tname,) in tables:
            table_ref = _quote_identifier(tname)
            if tname == IRM_INTERACTION_QA.name:
                if attached_alias is None:
                    attached_alias = require_irm_qa_db(con)
                table_ref = f"{attached_alias}.{table_ref}"
                date_columns_sql = (
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_catalog = ? AND table_name = ? "
                    "AND column_name LIKE '%date%'"
                )
                date_column_params = [attached_alias, tname]
            else:
                date_columns_sql = (
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = ? AND column_name LIKE '%date%'"
                )
                date_column_params = [tname]
            count = con.execute(
                f"SELECT COUNT(*) FROM {table_ref}"
            ).fetchone()[0]
            # 每张表可能有不同的日期字段名
            date_cols = con.execute(
                date_columns_sql,
                date_column_params,
            ).fetchall()
            date_col = date_cols[0][0] if date_cols else None
            earliest = latest = None
            if date_col:
                row = con.execute(
                    f"SELECT MIN({_quote_identifier(date_col)}), "
                    f"MAX({_quote_identifier(date_col)}) FROM {table_ref}"
                ).fetchone()
                earliest = str(row[0]) if row and row[0] else None
                latest = str(row[1]) if row and row[1] else None
            result[tname] = {
                "rows": count,
                "earliest_date": earliest,
                "latest_date": latest,
            }
        db_path = get_db_path()
        return {
            "database": str(db_path),
            "size_bytes": db_path.stat().st_size if db_path.exists() else 0,
            "tables": result,
        }
    finally:
        if attached_alias is not None:
            detach_database_if_attached(con, attached_alias)
        con.close()
