"""专项测试：irm_interaction_qa 从主库迁移到独立 IRM 库的幂等与 fail-closed 行为。"""

from __future__ import annotations

import importlib.util
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from qrp_atlas.config.settings import AppSettings
from qrp_atlas.contracts import init_irm_database


def _load_migration_module():
    script = Path(__file__).resolve().parents[2] / "scripts" / "migrate_irm_qa_to_dedicated_db.py"
    spec = importlib.util.spec_from_file_location("migrate_irm_qa_to_dedicated_db", script)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


mod = _load_migration_module()


def _sample_rows() -> list[dict]:
    return [
        {
            "pid": "PID001",
            "ticker": "001205.SZ",
            "company_code": "001205",
            "company_shortname": "盛航股份",
            "question_content": "问题 1",
            "reply_content": "回复 1",
            "question_time": "2026-07-10 22:00:00",
            "reply_time": "2026-07-10 23:43:01",
            "reply_date": "2026-07-10",
            "nickname": "投资者",
            "keywords": None,
            "source": "p5w",
            "created_at": "2026-07-10 23:43:02",
        },
        {
            "pid": "PID002",
            "ticker": "001205.SZ",
            "company_code": "001205",
            "company_shortname": "盛航股份",
            "question_content": "问题 2",
            "reply_content": "回复 2",
            "question_time": "2026-07-10 22:05:00",
            "reply_time": "2026-07-10 23:45:00",
            "reply_date": "2026-07-10",
            "nickname": "投资者",
            "keywords": None,
            "source": "p5w",
            "created_at": "2026-07-10 23:45:01",
        },
    ]


def _build_source(db_path: Path, rows: list[dict] | None = None) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    try:
        init_irm_database(con)
        rows = rows if rows is not None else _sample_rows()
        for row in rows:
            columns = list(row)
            placeholders = ", ".join("?" for _ in columns)
            con.execute(
                f"INSERT INTO irm_interaction_qa ({', '.join(columns)}) VALUES ({placeholders})",
                [row[column] for column in columns],
            )
    finally:
        con.close()


def _settings(tmp_path: Path, *, source: Path, target: Path) -> AppSettings:
    return AppSettings.load(
        overrides={
            "QRP_HOME": str(tmp_path / "home"),
            "QRP_DATA_DIR": str(tmp_path / "data"),
            "QRP_DUCKDB_PATH": str(source),
            "QRP_IRM_QA_DUCKDB_PATH": str(target),
        },
        project_root=tmp_path / "repo",
    )


def _target_metrics(target: Path) -> dict:
    con = duckdb.connect(str(target), read_only=True)
    try:
        row = con.execute(
            "SELECT COUNT(*), COUNT(DISTINCT pid), MIN(reply_time), MAX(reply_time) "
            "FROM irm_interaction_qa"
        ).fetchone()
        dup = con.execute(
            "SELECT COUNT(*) FROM (SELECT pid FROM irm_interaction_qa "
            "GROUP BY pid HAVING COUNT(*) > 1)"
        ).fetchone()[0]
    finally:
        con.close()
    return {
        "total_rows": int(row[0]),
        "distinct_pid": int(row[1]),
        "min_reply_time": str(row[2]),
        "max_reply_time": str(row[3]),
        "duplicate_pid": int(dup),
    }


def test_migrate_first_run_creates_and_copies(tmp_path: Path) -> None:
    source = tmp_path / "quant.db"
    target = tmp_path / "irm_qa.duckdb"
    _build_source(source)
    settings = _settings(tmp_path, source=source, target=target)

    result = mod.migrate(settings)

    assert result["action"] == "created"
    assert result["schema_ok"] is True
    assert result["source_metrics"]["total_rows"] == 2
    assert result["target_metrics"]["total_rows"] == 2
    assert result["target_metrics"]["distinct_pid"] == 2
    assert result["target_metrics"]["duplicate_pid"] == 0
    assert result["target_metrics"]["min_reply_time"] == result["source_metrics"]["min_reply_time"]
    assert result["target_metrics"]["max_reply_time"] == result["source_metrics"]["max_reply_time"]
    metrics = _target_metrics(target)
    assert metrics["total_rows"] == 2
    assert metrics["duplicate_pid"] == 0


def test_migrate_second_run_is_noop(tmp_path: Path) -> None:
    source = tmp_path / "quant.db"
    target = tmp_path / "irm_qa.duckdb"
    _build_source(source)
    settings = _settings(tmp_path, source=source, target=target)

    first = mod.migrate(settings)
    assert first["action"] == "created"
    second = mod.migrate(settings)

    assert second["action"] == "noop"
    assert second["target_metrics"]["total_rows"] == 2
    assert _target_metrics(target)["total_rows"] == 2


def test_migrate_into_empty_target_copies(tmp_path: Path) -> None:
    source = tmp_path / "quant.db"
    target = tmp_path / "irm_qa.duckdb"
    _build_source(source)
    # 预建空目标库（表存在但 0 行）
    target.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(target))
    try:
        init_irm_database(con)
    finally:
        con.close()
    settings = _settings(tmp_path, source=source, target=target)

    result = mod.migrate(settings)

    assert result["action"] == "copied_into_empty_target"
    assert _target_metrics(target)["total_rows"] == 2


def test_migrate_fails_closed_on_diverged_target(tmp_path: Path) -> None:
    source = tmp_path / "quant.db"
    target = tmp_path / "irm_qa.duckdb"
    _build_source(source)
    # 目标库已有不同内容（少一行）
    target.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(target))
    try:
        init_irm_database(con)
        row = _sample_rows()[0]
        columns = list(row)
        placeholders = ", ".join("?" for _ in columns)
        con.execute(
            f"INSERT INTO irm_interaction_qa ({', '.join(columns)}) VALUES ({placeholders})",
            [row[column] for column in columns],
        )
    finally:
        con.close()
    settings = _settings(tmp_path, source=source, target=target)

    with pytest.raises(RuntimeError, match="content diverges"):
        mod.migrate(settings)

    # 目标未被覆盖/清空
    assert _target_metrics(target)["total_rows"] == 1


def test_migrate_does_not_modify_source(tmp_path: Path) -> None:
    source = tmp_path / "quant.db"
    target = tmp_path / "irm_qa.duckdb"
    _build_source(source)
    before = _target_metrics(source)
    settings = _settings(tmp_path, source=source, target=target)

    mod.migrate(settings)

    after = _target_metrics(source)
    assert before == after


def test_migrate_fails_closed_on_business_field_tampering(tmp_path: Path) -> None:
    """行数/pid/reply_time 均相同，仅业务字段（reply_content）被篡改时必须 fail-closed。"""
    source = tmp_path / "quant.db"
    target = tmp_path / "irm_qa.duckdb"
    _build_source(source)
    settings = _settings(tmp_path, source=source, target=target)

    first = mod.migrate(settings)
    assert first["action"] == "created"

    # 篡改目标库 reply_content（pid 与 reply_time 保持不变）
    con = duckdb.connect(str(target))
    try:
        con.execute(
            "UPDATE irm_interaction_qa SET reply_content = '损坏或错误内容' "
            "WHERE pid = 'PID001'"
        )
    finally:
        con.close()

    with pytest.raises(RuntimeError, match="content diverges"):
        mod.migrate(settings)

    # 目标库未被覆盖或清空
    assert _target_metrics(target)["total_rows"] == 2
