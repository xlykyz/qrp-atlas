"""全景网互动问答 pipeline 测试。"""

from __future__ import annotations

import duckdb
import pandas as pd
import pytest

from qrp_atlas.contracts import (
    IRM_INTERACTION_QA,
    INTERACTION_PID,
    get_table,
    init_database,
)
from qrp_atlas.pipeline.irm_qa.clean import clean_interaction_qa, clean_record
from qrp_atlas.pipeline.irm_qa.fetch import fetch_interaction_qa
from qrp_atlas.pipeline.irm_qa.load import upsert_interaction_qa


RAW_SAMPLE = {
    "companyShortname": "盛航股份",
    "companyCode": "001205",
    "nickname": "186****5202",
    "content": "请问公司是否考虑注入其他产业链资产？",
    "replyContent": "尊敬的投资者您好！感谢关注。(来自：深交所互动易)",
    "replyerTimeStr": "2026-07-10 23:43:47",
    "questionerTimeStr": "2026-07-10 22:00:00",
    "pid": "0001EE520E9BBE2C4EB7B2E74B073E478FBC",
}


def test_irm_interaction_qa_schema():
    table = get_table("irm_interaction_qa")
    assert table is IRM_INTERACTION_QA
    assert table.primary_key == (INTERACTION_PID,)
    names = table.column_names()
    for required in (
        "pid",
        "ticker",
        "company_code",
        "question_content",
        "reply_content",
        "reply_time",
        "reply_date",
        "source",
    ):
        assert required in names


def test_clean_record_maps_and_sanitizes():
    cleaned = clean_record(RAW_SAMPLE, keywords="回购")
    assert cleaned is not None
    assert cleaned["pid"] == RAW_SAMPLE["pid"]
    assert cleaned["company_code"] == "001205"
    assert cleaned["ticker"] == "001205.SZ"
    assert cleaned["nickname"] == "投资者"
    assert cleaned["reply_date"] == "2026-07-10"
    assert cleaned["keywords"] == "回购"
    assert cleaned["source"] == "p5w"
    assert cleaned["question_content"].startswith("请问公司")


def test_clean_drops_invalid_and_dedupes():
    bad = dict(RAW_SAMPLE)
    bad["pid"] = ""
    early = dict(RAW_SAMPLE)
    early["pid"] = "EARLY1"
    early["replyerTimeStr"] = "2026-07-09 10:00:00"

    cleaned = clean_interaction_qa(
        [RAW_SAMPLE, RAW_SAMPLE, bad, early],
        since_date="2026-07-10",
    )
    assert len(cleaned) == 1
    assert cleaned[0]["pid"] == RAW_SAMPLE["pid"]


def test_upsert_interaction_qa(tmp_path):
    db = tmp_path / "irm.duckdb"
    con = duckdb.connect(str(db))
    try:
        init_database(con)
        records = clean_interaction_qa([RAW_SAMPLE])

        # 默认增量：首次插入
        n1 = upsert_interaction_qa(con, records, incremental=True)
        assert n1 == 1
        # 再次增量：同 pid 保留原内容
        records[0]["reply_content"] = "should-not-overwrite"
        n2 = upsert_interaction_qa(con, records, incremental=True)
        assert n2 == 1
        count = con.execute(
            "SELECT COUNT(*) FROM irm_interaction_qa"
        ).fetchone()[0]
        assert count == 1
        reply = con.execute(
            "SELECT reply_content FROM irm_interaction_qa WHERE pid = ?",
            [RAW_SAMPLE["pid"]],
        ).fetchone()[0]
        assert "should-not-overwrite" not in reply

        # replace 模式可覆盖
        records[0]["reply_content"] = "updated"
        upsert_interaction_qa(con, records, incremental=False)
        reply = con.execute(
            "SELECT reply_content FROM irm_interaction_qa WHERE pid = ?",
            [RAW_SAMPLE["pid"]],
        ).fetchone()[0]
        assert reply == "updated"
    finally:
        con.close()


def test_fetch_stops_on_pid_overlap(monkeypatch):
    page1 = {
        "success": True,
        "rows": [
            {
                **RAW_SAMPLE,
                "pid": f"PID{i}",
                "replyerTimeStr": f"2026-07-10 23:4{i}:00",
            }
            for i in range(10)
        ],
    }
    # page2 fully repeats page1 (server wrap-around)
    page2 = page1

    calls = {"n": 0}

    def fake_post(page, **kwargs):
        calls["n"] += 1
        return page1 if page == 1 else page2

    monkeypatch.setattr(
        "qrp_atlas.pipeline.irm_qa.fetch._post_page",
        fake_post,
    )
    monkeypatch.setattr(
        "qrp_atlas.pipeline.irm_qa.fetch.p5w_sleep_interval",
        lambda: 0,
    )

    records = fetch_interaction_qa(max_pages=5)
    assert len(records) == 10
    assert calls["n"] == 2  # second page detects full overlap and stops


def test_fetch_stops_by_since_date(monkeypatch):
    def fake_post(page, **kwargs):
        if page == 1:
            return {
                "success": True,
                "rows": [
                    {
                        **RAW_SAMPLE,
                        "pid": "NEW1",
                        "replyerTimeStr": "2026-07-10 12:00:00",
                    },
                    {
                        **RAW_SAMPLE,
                        "pid": "OLD1",
                        "replyerTimeStr": "2026-07-09 12:00:00",
                    },
                ],
            }
        return {"success": True, "rows": []}

    monkeypatch.setattr(
        "qrp_atlas.pipeline.irm_qa.fetch._post_page",
        fake_post,
    )
    monkeypatch.setattr(
        "qrp_atlas.pipeline.irm_qa.fetch.p5w_sleep_interval",
        lambda: 0,
    )

    records = fetch_interaction_qa(since_date="2026-07-10", max_pages=5)
    assert [r["pid"] for r in records] == ["NEW1"]
