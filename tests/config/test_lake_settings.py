from pathlib import Path

import pytest

from qrp_atlas.config.lake import LAKE_ENV_VARS
from qrp_atlas.config.settings import AppSettings, SUPPORTED_ENV_VARS, redact_secrets


def load(tmp_path, **values):
    return AppSettings.load(environ=values, project_root=tmp_path / "unused-project")


def test_lake_is_opt_in_and_load_does_not_create_directories(tmp_path):
    item = load(tmp_path)
    assert item.lake.root is None
    assert item.lake.daily_source == "legacy"
    assert item.lake.core_catalog is None
    assert not (tmp_path / "unused-project").exists()


def test_lake_accepts_explicit_snapshot_and_separate_catalogs_without_side_effects(tmp_path):
    target = tmp_path / "lake"
    item = load(tmp_path, QRP_LAKE_ROOT=str(target), QRP_LAKE_DAILY_SOURCE="lake",
                QRP_LAKE_DAILY_SNAPSHOT="17", QRP_LAKE_THREADS="3", QRP_LAKE_MEMORY_LIMIT="512MB",
                QRP_LAKE_CORE_CATALOG="postgres:dbname=core", QRP_LAKE_EXTENSION_CATALOG="postgres:dbname=extensions")
    assert item.lake.daily_snapshot == 17
    assert item.lake.threads == 3
    assert item.lake.memory_limit == "512MB"
    assert item.lake.root == target
    assert not target.exists()


@pytest.mark.parametrize("values", [
    {"QRP_LAKE_ROOT": "relative-path"},
    {"QRP_LAKE_DAILY_SOURCE": "auto"},
    {"QRP_LAKE_DAILY_SOURCE": "lake"},
    {"QRP_LAKE_THREADS": "0"},
    {"QRP_LAKE_THREADS": "13"},
    {"QRP_LAKE_MEMORY_LIMIT": "unlimited"},
    {"QRP_LAKE_WORKER_TIMEOUT_SECONDS": "nan"},
    {"QRP_LAKE_WORKER_MEMORY_BYTES": "-1"},
    {"QRP_LAKE_WORKER_TEMP_BYTES": "0"},
    {"QRP_LAKE_PARTITION_START_DATE": "not-a-date"},
    {"QRP_LAKE_RELOCATED": "yes"},
    {"QRP_LAKE_ALLOW_OBSERVED": "yes"},
    {"QRP_LAKE_CORE_CATALOG": "postgres:dbname=same", "QRP_LAKE_EXTENSION_CATALOG": "postgres:dbname=same"},
])
def test_invalid_lake_configuration_fails_closed(tmp_path, values):
    with pytest.raises(ValueError):
        load(tmp_path, **values)


def test_lake_catalog_secrets_never_appear_in_diagnostics(tmp_path):
    values = {"QRP_LAKE_CORE_CATALOG": "postgres:dbname=core password=core-test-secret",
              "QRP_LAKE_EXTENSION_CATALOG": "postgres:dbname=extensions password=extension-test-secret",
              "QRP_LAKE_CORE_READ_CATALOG": "postgres:dbname=core user=reader password=reader-test-secret"}
    item = load(tmp_path, **values)
    for secret in values.values():
        assert secret not in repr(item)
        assert secret not in item.safe_json()
        assert secret not in redact_secrets("catalog attachment: " + secret, item)


def test_lake_environment_catalog_and_example_are_synchronized():
    example = (Path(__file__).resolve().parents[2] / ".env.example").read_text(encoding="utf-8")
    assert LAKE_ENV_VARS <= SUPPORTED_ENV_VARS
    assert all(name + "=" in example for name in LAKE_ENV_VARS)
