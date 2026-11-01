from datetime import date
from types import SimpleNamespace

import numpy as np
import pytest

from tools import lake_benchmark as benchmark


def config(tmp_path, *, extra=""):
    env = tmp_path / "benchmark.env"
    root = tmp_path / "output"
    env.write_text("\n".join([
        "QRP_RUNTIME_ENV=test", f"QRP_LAKE_ROOT={root.as_posix()}",
        "QRP_LAKE_CORE_CATALOG=postgres:dbname=benchmark_core user=writer",
        "QRP_LAKE_CORE_READ_CATALOG=postgres:dbname=benchmark_core user=reader",
        "QRP_LAKE_EXTENSION_CATALOG=postgres:dbname=benchmark_extension user=writer",
        extra,
    ]), encoding="utf-8")
    args = SimpleNamespace(env_file=env, days=2, assets=10, history_days=20,
                           max_core_p95_seconds=None, max_slowdown_ratio=None)
    return args, root


def test_preflight_does_not_create_directories(tmp_path, monkeypatch):
    args, root = config(tmp_path)
    catalogs = []
    monkeypatch.setattr(benchmark, "empty_catalog", catalogs.append)
    item = benchmark.prepare(args)
    assert item.lake.root == root
    assert len(catalogs) == 2
    assert not root.exists()


def test_existing_output_is_rejected_before_connecting(tmp_path, monkeypatch):
    args, root = config(tmp_path)
    root.mkdir()
    (root / "keep").write_text("existing output", encoding="utf-8")
    monkeypatch.setattr(benchmark, "empty_catalog", lambda _: pytest.fail("must reject locally before connecting"))
    with pytest.raises(ValueError, match="completely empty"):
        benchmark.prepare(args)
    assert (root / "keep").read_text() == "existing output"


def test_production_and_default_env_are_rejected(tmp_path):
    args, _ = config(tmp_path, extra="QRP_RUNTIME_ENV=production")
    with pytest.raises(ValueError, match="RUNTIME_ENV=test"):
        benchmark.prepare(args)
    args.env_file = tmp_path / "root.env"
    with pytest.raises(ValueError, match="dedicated benchmark"):
        benchmark.prepare(args)


def test_different_credentials_do_not_make_one_database_two_catalogs(tmp_path, monkeypatch):
    args, _ = config(tmp_path, extra="QRP_LAKE_EXTENSION_CATALOG=postgres:dbname=benchmark_core user=extension")
    monkeypatch.setattr(benchmark, "empty_catalog", lambda _: None)
    with pytest.raises(ValueError, match="extension database must be separate"):
        benchmark.prepare(args)


def test_synthetic_rolling_has_independent_linear_series_expectation():
    history = benchmark.synthetic_history(10, 20)
    frame = benchmark.compute(history, date(2020, 1, 29), 20, 10, 1, "128MB")
    expected = 10.0 + np.arange(10) / 100 + 0.2
    np.testing.assert_allclose(frame.close, expected, rtol=0, atol=1e-10)
    np.testing.assert_allclose(frame.ma20, expected - 0.095, rtol=0, atol=1e-10)
