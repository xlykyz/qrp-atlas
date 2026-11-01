"""Lake configuration parsed only by AppSettings' unified value reader."""
from dataclasses import dataclass, field
from pathlib import Path


LAKE_ENV_VARS = frozenset({"QRP_LAKE_ROOT", "QRP_LAKE_CORE_CATALOG", "QRP_LAKE_EXTENSION_CATALOG",
    "QRP_LAKE_CORE_READ_CATALOG", "QRP_LAKE_THREADS", "QRP_LAKE_MEMORY_LIMIT",
    "QRP_LAKE_DAILY_SOURCE", "QRP_LAKE_DAILY_SNAPSHOT", "QRP_LAKE_WORKER_TIMEOUT_SECONDS",
    "QRP_LAKE_WORKER_MEMORY_BYTES", "QRP_LAKE_WORKER_TEMP_BYTES", "QRP_LAKE_PARTITION_START_DATE",
    "QRP_LAKE_PLUGINS_FILE", "QRP_LAKE_ALLOW_OBSERVED", "QRP_LAKE_RELOCATED"})


@dataclass(frozen=True)
class LakeSettings:
    root: Path | None = None
    core_catalog: str | None = field(default=None, repr=False)
    extension_catalog: str | None = field(default=None, repr=False)
    core_read_catalog: str | None = field(default=None, repr=False)
    threads: int = 2
    memory_limit: str = "4GB"
    daily_source: str = "legacy"
    daily_snapshot: int | None = None
    worker_timeout_seconds: float = 1800.0
    worker_memory_bytes: int = 8 * 1024**3
    worker_temp_bytes: int = 16 * 1024**3
    partition_start_date: str = "2026-01-01"
    plugins_file: Path | None = None
    allow_observed: bool = False
    relocated: bool = False

    @classmethod
    def parse(cls, reader):
        root = reader.optional("QRP_LAKE_ROOT")
        threads = int(reader.optional("QRP_LAKE_THREADS") or 2)
        memory = reader.optional("QRP_LAKE_MEMORY_LIMIT") or "4GB"
        import re
        if threads < 1 or threads > 12 or not re.fullmatch(r"[1-9][0-9]*(MB|GB)", memory):
            raise ValueError("invalid lake resource budget")
        source = reader.optional("QRP_LAKE_DAILY_SOURCE") or "legacy"
        snapshot = reader.optional("QRP_LAKE_DAILY_SNAPSHOT")
        if source not in {"legacy", "lake"}:
            raise ValueError("invalid daily source")
        if root and not Path(root).is_absolute():
            raise ValueError("QRP_LAKE_ROOT must be absolute")
        if source == "lake" and (not root or snapshot is None or int(snapshot) < 0):
            raise ValueError("lake daily source needs root and fixed snapshot")
        core = reader.optional("QRP_LAKE_CORE_CATALOG")
        ext = reader.optional("QRP_LAKE_EXTENSION_CATALOG")
        if core and ext and core == ext:
            raise ValueError("core and extension catalogs must differ")
        import math
        from datetime import date
        timeout = float(reader.optional("QRP_LAKE_WORKER_TIMEOUT_SECONDS") or 1800)
        worker_memory = int(reader.optional("QRP_LAKE_WORKER_MEMORY_BYTES") or 8 * 1024**3)
        worker_temp = int(reader.optional("QRP_LAKE_WORKER_TEMP_BYTES") or 16 * 1024**3)
        start_date = reader.optional("QRP_LAKE_PARTITION_START_DATE") or "2026-01-01"
        if not math.isfinite(timeout) or min(timeout, worker_memory, worker_temp) <= 0:
            raise ValueError("invalid lake worker budget")
        if date.fromisoformat(start_date).isoformat() != start_date:
            raise ValueError("invalid lake partition start date")
        plugins = reader.optional("QRP_LAKE_PLUGINS_FILE")
        if plugins and not Path(plugins).is_absolute():
            raise ValueError("QRP_LAKE_PLUGINS_FILE must be absolute")
        observed = (reader.optional("QRP_LAKE_ALLOW_OBSERVED") or "false").lower()
        if observed not in {"true", "false"}:
            raise ValueError("QRP_LAKE_ALLOW_OBSERVED must be true or false")
        relocated = (reader.optional("QRP_LAKE_RELOCATED") or "false").lower()
        if relocated not in {"true", "false"}:
            raise ValueError("QRP_LAKE_RELOCATED must be true or false")
        return cls(Path(root) if root else None, core, ext,
                   reader.optional("QRP_LAKE_CORE_READ_CATALOG"), threads, memory, source,
                   int(snapshot) if snapshot is not None else None,
                   timeout, worker_memory, worker_temp, start_date,
                   Path(plugins) if plugins else None, observed == "true",
                   relocated == "true")
