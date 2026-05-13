"""QRP Atlas API 服务入口"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from qrp_atlas.api.db import get_db, get_db_path
from qrp_atlas.api.routes import daily, phase, trades


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期：启动时初始化状态，关闭时清理"""
    app.state.db_path = get_db_path()
    yield


# ── FastAPI 应用 ──────────────────────────────


app = FastAPI(
    title="QRP Atlas API",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── 注册路由 ──────────────────────────────────

app.include_router(daily.router)
app.include_router(phase.router)
app.include_router(trades.router)


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
    try:
        result = {}
        tables = con.execute("SHOW TABLES").fetchall()
        for (tname,) in tables:
            count = con.execute(
                f'SELECT COUNT(*) FROM "{tname}"'
            ).fetchone()[0]
            # 每张表可能有不同的日期字段名
            date_cols = con.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = ? AND column_name LIKE '%date%'",
                [tname],
            ).fetchall()
            date_col = date_cols[0][0] if date_cols else None
            earliest = latest = None
            if date_col:
                row = con.execute(
                    f"SELECT MIN({date_col}), MAX({date_col}) FROM {tname}"
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
        con.close()
