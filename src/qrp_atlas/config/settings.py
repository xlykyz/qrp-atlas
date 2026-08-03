"""Unified runtime configuration for QRP Atlas.

Configuration precedence is explicit overrides, process environment, the selected
.env file, then stable defaults. Relative paths are always resolved from the
repository root rather than the caller's current working directory.
"""

from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass, field
from enum import StrEnum
from functools import lru_cache
from pathlib import Path, PurePosixPath, PureWindowsPath
from types import MappingProxyType
from typing import Any, Mapping
from urllib.parse import urlsplit
from uuid import UUID

from dotenv import dotenv_values


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_LOCAL_USER_ID = UUID("f445c8c9-96d8-4ce7-9f8a-9e884dd038d8")


class ConfigError(ValueError):
    """Raised when runtime configuration is missing or invalid."""


class AuthMode(StrEnum):
    LOCAL = "local"
    DATABASE = "database"


class RuntimeEnvironment(StrEnum):
    DEVELOPMENT = "development"
    TEST = "test"
    PRODUCTION = "production"


class PathSyntax(StrEnum):
    RELATIVE = "relative"
    WINDOWS_ABSOLUTE = "windows_absolute"
    POSIX_ABSOLUTE = "posix_absolute"


def classify_path_syntax(value: str) -> PathSyntax:
    """Classify path text without depending on the host operating system."""

    if PureWindowsPath(value).is_absolute():
        return PathSyntax.WINDOWS_ABSOLUTE
    if PurePosixPath(value).is_absolute():
        return PathSyntax.POSIX_ABSOLUTE
    return PathSyntax.RELATIVE


def _parse_bool(name: str, raw: str) -> bool:
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ConfigError(
        f"{name} must be a boolean: true/false, yes/no, on/off, or 1/0"
    )


def _parse_int(
    name: str,
    raw: str,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    try:
        value = int(raw.strip())
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{name} must be an integer") from exc
    if minimum is not None and value < minimum:
        raise ConfigError(f"{name} must be >= {minimum}")
    if maximum is not None and value > maximum:
        raise ConfigError(f"{name} must be <= {maximum}")
    return value


def _parse_float(name: str, raw: str, *, minimum: float = 0.0) -> float:
    try:
        value = float(raw.strip())
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{name} must be a number") from exc
    if not math.isfinite(value) or value < minimum:
        raise ConfigError(f"{name} must be a finite number >= {minimum}")
    return value


def _parse_http_url(name: str, raw: str, *, required: bool = True) -> str | None:
    value = raw.strip()
    if not value:
        if required:
            raise ConfigError(f"{name} must not be empty")
        return None
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ConfigError(f"{name} must be an absolute http(s) URL")
    return value


def _parse_postgres_dsn(name: str, raw: str | None) -> str | None:
    if raw is None or not raw.strip():
        return None
    value = raw.strip()
    parsed = urlsplit(value)
    if parsed.scheme not in {"postgres", "postgresql"} or not parsed.hostname:
        raise ConfigError(f"{name} must be a PostgreSQL URL")
    return value


def _resolve_path(name: str, raw: str | Path, *, base: Path) -> Path:
    value = str(raw).strip()
    if not value:
        raise ConfigError(f"{name} must not be empty")
    if "\x00" in value:
        raise ConfigError(f"{name} contains a NUL byte")

    windows = PureWindowsPath(value)
    syntax = classify_path_syntax(value)
    if windows.drive and not windows.is_absolute():
        raise ConfigError(f"{name} uses an ambiguous drive-relative Windows path")
    if syntax is PathSyntax.WINDOWS_ABSOLUTE and os.name != "nt":
        raise ConfigError(f"{name} uses a Windows absolute path on a POSIX host")
    if syntax is PathSyntax.POSIX_ABSOLUTE and os.name == "nt":
        raise ConfigError(f"{name} uses a POSIX absolute path on a Windows host")

    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        candidate = base / candidate
    try:
        return candidate.resolve(strict=False)
    except OSError as exc:
        raise ConfigError(f"{name} could not be resolved") from exc


def _parse_csv(name: str, raw: str) -> tuple[str, ...]:
    values = tuple(item.strip() for item in raw.split(",") if item.strip())
    if not values:
        raise ConfigError(f"{name} must contain at least one value")
    return values


class _ValueReader:
    def __init__(
        self,
        *,
        overrides: Mapping[str, Any],
        environ: Mapping[str, str],
        dotenv: Mapping[str, str | None],
        dotenv_path: Path | None,
    ) -> None:
        self.overrides = overrides
        self.environ = environ
        self.dotenv = dotenv
        self.dotenv_path = dotenv_path
        self.sources: dict[str, str] = {}

    def get(
        self,
        name: str,
        default: str,
        *,
        aliases: tuple[str, ...] = (),
    ) -> str:
        keys = (name, *aliases)
        for key in keys:
            if key in self.overrides and self.overrides[key] is not None:
                self.sources[name] = "explicit" if key == name else f"explicit:{key}"
                return str(self.overrides[key])
        for key in keys:
            value = self.environ.get(key)
            if value is not None:
                self.sources[name] = "environment" if key == name else f"environment:{key}"
                return value
        for key in keys:
            value = self.dotenv.get(key)
            if value is not None:
                label = str(self.dotenv_path) if self.dotenv_path else ".env"
                self.sources[name] = f"dotenv:{label}" if key == name else f"dotenv:{label}:{key}"
                return value
        self.sources[name] = "default"
        return default

    def optional(
        self,
        name: str,
        *,
        aliases: tuple[str, ...] = (),
    ) -> str | None:
        marker = "__QRP_OPTIONAL_VALUE_NOT_SET__"
        value = self.get(name, marker, aliases=aliases)
        if value == marker:
            return None
        return value.strip() or None


@dataclass(frozen=True, slots=True)
class PathSettings:
    home: Path
    data_dir: Path
    raw_dir: Path
    canonical_dir: Path
    db_dir: Path
    duckdb_path: Path
    irm_qa_duckdb_path: Path
    state_dir: Path
    job_runtime_dir: Path
    backtest_runs_dir: Path
    backtest_tasks_dir: Path
    robustness_runs_dir: Path
    declarative_strategies_dir: Path
    log_dir: Path
    tmp_dir: Path
    research_pdfs_dir: Path
    remote_access_runtime_dir: Path
    web_dir: Path
    backtest_fixture_runs_dir: Path
    # Optional cross-database paths for episode/pool data (ATTACH'd read-only).
    # Actual paths must be configured on the Linux server via environment
    # variables QRP_EPISODE_DB_PATH / QRP_POOL_DB_PATH.
    episode_db_path: Path | None
    pool_db_path: Path | None

    def persistent_directories(self) -> tuple[Path, ...]:
        return (
            self.data_dir,
            self.raw_dir,
            self.canonical_dir,
            self.db_dir,
            self.state_dir,
            self.job_runtime_dir,
            self.backtest_runs_dir,
            self.backtest_tasks_dir,
            self.robustness_runs_dir,
            self.declarative_strategies_dir,
            self.research_pdfs_dir,
        )

    def runtime_directories(self) -> tuple[Path, ...]:
        return (self.home, self.log_dir, self.tmp_dir, self.remote_access_runtime_dir)


@dataclass(frozen=True, slots=True)
class DatabaseSettings:
    duckdb_path: Path
    read_only: bool = False


@dataclass(frozen=True, slots=True)
class ApiSettings:
    host: str = "127.0.0.1"
    port: int = 8000
    cors_origins: tuple[str, ...] = ("*",)


@dataclass(frozen=True, slots=True)
class AuthenticationSettings:
    mode: AuthMode = AuthMode.LOCAL
    local_user_id: UUID = DEFAULT_LOCAL_USER_ID
    local_username: str = "ryan"
    local_display_name: str = "Ryan"
    postgres_dsn: str | None = field(default=None, repr=False)
    session_ttl_seconds: int = 60 * 60 * 24 * 7


@dataclass(frozen=True, slots=True)
class ExternalServicesSettings:
    tushare_token: str | None = field(default=None, repr=False)
    tushare_api_url: str = "https://fastapic.stockai888.top"
    tushare_request_interval_seconds: float = 0.6
    tushare_max_retries: int = 0
    tushare_retry_backoff_seconds: float = 1.0
    http_proxy: str | None = field(default=None, repr=False)
    https_proxy: str | None = field(default=None, repr=False)
    no_proxy: str | None = None


@dataclass(frozen=True, slots=True)
class LoggingSettings:
    level: str = "INFO"


@dataclass(frozen=True, slots=True)
class RuntimeSettings:
    environment: RuntimeEnvironment = RuntimeEnvironment.DEVELOPMENT
    read_only: bool = False
    env_file: Path | None = None
    remote_access_token_file: Path | None = None
    remote_access_database_path: Path | None = None
    remote_access_port: int = 8765


@dataclass(frozen=True, slots=True)
class AppSettings:
    project_root: Path
    paths: PathSettings
    database: DatabaseSettings
    api: ApiSettings
    authentication: AuthenticationSettings
    external_services: ExternalServicesSettings
    logging: LoggingSettings
    runtime: RuntimeSettings
    sources: Mapping[str, str] = field(
        default_factory=dict,
        repr=False,
        compare=False,
    )

    @classmethod
    def load(
        cls,
        *,
        overrides: Mapping[str, Any] | None = None,
        environ: Mapping[str, str] | None = None,
        env_file: str | Path | None = None,
        project_root: str | Path | None = None,
    ) -> "AppSettings":
        explicit = dict(overrides or {})
        env = dict(os.environ if environ is None else environ)
        root = Path(project_root or PROJECT_ROOT).resolve(strict=False)

        env_file_value = env_file
        env_file_explicit = env_file is not None
        if env_file_value is None:
            env_file_value = explicit.get("QRP_ENV_FILE") or env.get("QRP_ENV_FILE")
            env_file_explicit = env_file_value is not None
        dotenv_path = (
            _resolve_path("QRP_ENV_FILE", env_file_value, base=root)
            if env_file_value
            else root / ".env"
        )
        if env_file_explicit and not dotenv_path.is_file():
            raise ConfigError("QRP_ENV_FILE does not point to a readable file")
        dotenv_data = dotenv_values(dotenv_path) if dotenv_path.is_file() else {}
        reader = _ValueReader(
            overrides=explicit,
            environ=env,
            dotenv=dotenv_data,
            dotenv_path=dotenv_path if dotenv_path.is_file() else None,
        )
        reader.sources["QRP_ENV_FILE"] = (
            "explicit" if env_file is not None or "QRP_ENV_FILE" in explicit
            else "environment" if "QRP_ENV_FILE" in env
            else "default"
        )

        home = _resolve_path("QRP_HOME", reader.get("QRP_HOME", str(root)), base=root)
        data_dir = _resolve_path(
            "QRP_DATA_DIR",
            reader.get("QRP_DATA_DIR", str(root / "data")),
            base=root,
        )
        raw_dir = data_dir / "raw"
        canonical_dir = data_dir / "canonical"
        db_dir = data_dir / "db"
        duckdb_path = _resolve_path(
            "QRP_DUCKDB_PATH",
            reader.get("QRP_DUCKDB_PATH", str(db_dir / "quant.db")),
            base=root,
        )
        irm_qa_duckdb_path = _resolve_path(
            "QRP_IRM_QA_DUCKDB_PATH",
            reader.get(
                "QRP_IRM_QA_DUCKDB_PATH", str(db_dir / "irm_qa.duckdb")
            ),
            base=root,
        )
        state_dir = _resolve_path(
            "QRP_STATE_DIR",
            reader.get("QRP_STATE_DIR", str(data_dir / "state")),
            base=root,
        )
        job_runtime_dir = _resolve_path(
            "QRP_JOB_RUNTIME_DIR",
            reader.get("QRP_JOB_RUNTIME_DIR", str(data_dir / "runtime" / "job")),
            base=root,
        )
        backtest_runs_dir = _resolve_path(
            "QRP_BACKTEST_RUNS_DIR",
            reader.get(
                "QRP_BACKTEST_RUNS_DIR",
                str(data_dir / "backtest_runs"),
                aliases=("QRP_ATLAS_BACKTEST_RUNS_DIR",),
            ),
            base=root,
        )
        backtest_tasks_dir = _resolve_path(
            "QRP_BACKTEST_TASKS_DIR",
            reader.get(
                "QRP_BACKTEST_TASKS_DIR",
                str(data_dir / "backtest_tasks"),
                aliases=("QRP_ATLAS_BACKTEST_TASKS_DIR",),
            ),
            base=root,
        )
        robustness_runs_dir = _resolve_path(
            "QRP_ROBUSTNESS_RUNS_DIR",
            reader.get(
                "QRP_ROBUSTNESS_RUNS_DIR",
                str(data_dir / "robustness_runs"),
                aliases=("QRP_ATLAS_ROBUSTNESS_RUNS_DIR",),
            ),
            base=root,
        )
        declarative_strategies_dir = _resolve_path(
            "QRP_DECLARATIVE_STRATEGIES_DIR",
            reader.get(
                "QRP_DECLARATIVE_STRATEGIES_DIR",
                str(data_dir / "declarative_strategies"),
                aliases=("QRP_ATLAS_DECLARATIVE_STRATEGIES_DIR",),
            ),
            base=root,
        )
        log_dir = _resolve_path(
            "QRP_LOG_DIR",
            reader.get("QRP_LOG_DIR", str(home / ".runtime" / "logs")),
            base=root,
        )
        tmp_dir = _resolve_path(
            "QRP_TMP_DIR",
            reader.get("QRP_TMP_DIR", str(home / ".runtime" / "tmp")),
            base=root,
        )
        research_pdfs_dir = data_dir / "pdfs"
        remote_access_runtime_dir = home / ".runtime" / "remote_access"

        # ── Episode / Pool database paths (optional, for cross-db ATTACH) ──
        # These are separate DuckDB files produced by the episode and pool
        # pipelines.  On the Linux server, configure via environment variables.
        # If not set or file missing, the API gracefully degrades (endpoints
        # return 503 with a clear message).
        episode_db_path_raw = reader.optional("QRP_EPISODE_DB_PATH")
        episode_db_path: Path | None = (
            _resolve_path("QRP_EPISODE_DB_PATH", episode_db_path_raw, base=root)
            if episode_db_path_raw
            else None
        )
        pool_db_path_raw = reader.optional("QRP_POOL_DB_PATH")
        pool_db_path: Path | None = (
            _resolve_path("QRP_POOL_DB_PATH", pool_db_path_raw, base=root)
            if pool_db_path_raw
            else None
        )
        remote_access_token_file = _resolve_path(
            "QRP_REMOTE_ACCESS_TOKEN_FILE",
            reader.get(
                "QRP_REMOTE_ACCESS_TOKEN_FILE",
                str(remote_access_runtime_dir / "token"),
            ),
            base=root,
        )
        remote_access_database_path = _resolve_path(
            "QRP_REMOTE_ACCESS_DB_PATH",
            reader.get("QRP_REMOTE_ACCESS_DB_PATH", str(duckdb_path)),
            base=root,
        )
        remote_access_port = _parse_int(
            "QRP_REMOTE_ACCESS_PORT",
            reader.get("QRP_REMOTE_ACCESS_PORT", "8765"),
            minimum=1,
            maximum=65535,
        )

        read_only = _parse_bool(
            "QRP_READ_ONLY",
            reader.get("QRP_READ_ONLY", "false", aliases=("QRP_DB_READ_ONLY",)),
        )

        api_host = reader.get("QRP_API_HOST", "127.0.0.1").strip()
        if not api_host or "://" in api_host or "/" in api_host:
            raise ConfigError("QRP_API_HOST must be a hostname or IP address")
        api_port = _parse_int(
            "QRP_API_PORT", reader.get("QRP_API_PORT", "8000"), minimum=1, maximum=65535
        )
        cors_origins = _parse_csv(
            "QRP_API_CORS_ORIGINS", reader.get("QRP_API_CORS_ORIGINS", "*")
        )
        for origin in cors_origins:
            if origin != "*":
                _parse_http_url("QRP_API_CORS_ORIGINS", origin)

        raw_auth_mode = reader.get("QRP_AUTH_MODE", AuthMode.LOCAL.value).strip().lower()
        try:
            auth_mode = AuthMode(raw_auth_mode)
        except ValueError as exc:
            raise ConfigError("QRP_AUTH_MODE must be one of: local, database") from exc
        raw_user_id = reader.get("QRP_LOCAL_USER_ID", str(DEFAULT_LOCAL_USER_ID)).strip()
        try:
            local_user_id = UUID(raw_user_id)
        except ValueError as exc:
            raise ConfigError("QRP_LOCAL_USER_ID must be a valid UUID") from exc
        local_username = reader.get("QRP_LOCAL_USERNAME", "ryan").strip()
        local_display_name = reader.get("QRP_LOCAL_DISPLAY_NAME", "Ryan").strip()
        if not local_username:
            raise ConfigError("QRP_LOCAL_USERNAME must not be empty")
        if not local_display_name:
            raise ConfigError("QRP_LOCAL_DISPLAY_NAME must not be empty")
        session_ttl = _parse_int(
            "QRP_AUTH_SESSION_TTL_SECONDS",
            reader.get("QRP_AUTH_SESSION_TTL_SECONDS", str(60 * 60 * 24 * 7)),
            minimum=1,
        )
        postgres_dsn = _parse_postgres_dsn(
            "QRP_AUTH_DATABASE_URL", reader.optional("QRP_AUTH_DATABASE_URL")
        )
        if auth_mode is AuthMode.DATABASE and not postgres_dsn:
            raise ConfigError(
                "QRP_AUTH_DATABASE_URL is required when QRP_AUTH_MODE=database"
            )

        tushare_url = _parse_http_url(
            "QRP_TUSHARE_API_URL",
            reader.get("QRP_TUSHARE_API_URL", "https://fastapic.stockai888.top"),
        )
        assert tushare_url is not None
        request_interval = _parse_float(
            "QRP_TUSHARE_REQUEST_INTERVAL_SECONDS",
            reader.get("QRP_TUSHARE_REQUEST_INTERVAL_SECONDS", "0.6"),
        )
        max_retries = _parse_int(
            "QRP_TUSHARE_MAX_RETRIES",
            reader.get("QRP_TUSHARE_MAX_RETRIES", "0"),
            minimum=0,
        )
        retry_backoff = _parse_float(
            "QRP_TUSHARE_RETRY_BACKOFF_SECONDS",
            reader.get("QRP_TUSHARE_RETRY_BACKOFF_SECONDS", "1.0"),
        )
        http_proxy = reader.optional("QRP_HTTP_PROXY")
        https_proxy = reader.optional("QRP_HTTPS_PROXY")
        if http_proxy:
            _parse_http_url("QRP_HTTP_PROXY", http_proxy)
        if https_proxy:
            _parse_http_url("QRP_HTTPS_PROXY", https_proxy)

        log_level = reader.get("QRP_LOG_LEVEL", "INFO").strip().upper()
        if log_level not in {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}:
            raise ConfigError(
                "QRP_LOG_LEVEL must be one of: CRITICAL, ERROR, WARNING, INFO, DEBUG"
            )
        raw_runtime = reader.get(
            "QRP_RUNTIME_ENV", RuntimeEnvironment.DEVELOPMENT.value
        ).strip().lower()
        try:
            runtime_environment = RuntimeEnvironment(raw_runtime)
        except ValueError as exc:
            raise ConfigError(
                "QRP_RUNTIME_ENV must be one of: development, test, production"
            ) from exc

        paths = PathSettings(
            home=home,
            data_dir=data_dir,
            raw_dir=raw_dir,
            canonical_dir=canonical_dir,
            db_dir=db_dir,
            duckdb_path=duckdb_path,
            irm_qa_duckdb_path=irm_qa_duckdb_path,
            state_dir=state_dir,
            job_runtime_dir=job_runtime_dir,
            backtest_runs_dir=backtest_runs_dir,
            backtest_tasks_dir=backtest_tasks_dir,
            robustness_runs_dir=robustness_runs_dir,
            declarative_strategies_dir=declarative_strategies_dir,
            log_dir=log_dir,
            tmp_dir=tmp_dir,
            research_pdfs_dir=research_pdfs_dir,
            remote_access_runtime_dir=remote_access_runtime_dir,
            web_dir=root / "web",
            backtest_fixture_runs_dir=root / "tests" / "fixtures" / "backtest_runs",
            episode_db_path=episode_db_path,
            pool_db_path=pool_db_path,
        )
        return cls(
            project_root=root,
            paths=paths,
            database=DatabaseSettings(duckdb_path=duckdb_path, read_only=read_only),
            api=ApiSettings(host=api_host, port=api_port, cors_origins=cors_origins),
            authentication=AuthenticationSettings(
                mode=auth_mode,
                local_user_id=local_user_id,
                local_username=local_username,
                local_display_name=local_display_name,
                postgres_dsn=postgres_dsn,
                session_ttl_seconds=session_ttl,
            ),
            external_services=ExternalServicesSettings(
                tushare_token=reader.optional("TUSHARE_TOKEN"),
                tushare_api_url=tushare_url,
                tushare_request_interval_seconds=request_interval,
                tushare_max_retries=max_retries,
                tushare_retry_backoff_seconds=retry_backoff,
                http_proxy=http_proxy,
                https_proxy=https_proxy,
                no_proxy=reader.optional("QRP_NO_PROXY"),
            ),
            logging=LoggingSettings(level=log_level),
            runtime=RuntimeSettings(
                environment=runtime_environment,
                read_only=read_only,
                env_file=dotenv_path if dotenv_path.is_file() else None,
                remote_access_token_file=remote_access_token_file,
                remote_access_database_path=remote_access_database_path,
                remote_access_port=remote_access_port,
            ),
            sources=MappingProxyType(dict(reader.sources)),
        )

    def safe_dict(self) -> dict[str, Any]:
        """Return serializable effective configuration without secret values."""

        secret_names = {
            "TUSHARE_TOKEN",
            "QRP_AUTH_DATABASE_URL",
            "QRP_HTTP_PROXY",
            "QRP_HTTPS_PROXY",
        }
        return {
            "paths": {
                "home": str(self.paths.home),
                "data_dir": str(self.paths.data_dir),
                "duckdb_path": str(self.paths.duckdb_path),
                "irm_qa_duckdb_path": str(self.paths.irm_qa_duckdb_path),
                "state_dir": str(self.paths.state_dir),
                "job_runtime_dir": str(self.paths.job_runtime_dir),
                "backtest_runs_dir": str(self.paths.backtest_runs_dir),
                "backtest_tasks_dir": str(self.paths.backtest_tasks_dir),
                "robustness_runs_dir": str(self.paths.robustness_runs_dir),
                "declarative_strategies_dir": str(
                    self.paths.declarative_strategies_dir
                ),
                "log_dir": str(self.paths.log_dir),
                "tmp_dir": str(self.paths.tmp_dir),
                "episode_db_path": str(self.paths.episode_db_path) if self.paths.episode_db_path else None,
                "pool_db_path": str(self.paths.pool_db_path) if self.paths.pool_db_path else None,
            },
            "database": {
                "backend": "duckdb",
                "read_only": self.database.read_only,
            },
            "api": {
                "host": self.api.host,
                "port": self.api.port,
                "cors_origins": list(self.api.cors_origins),
            },
            "authentication": {
                "mode": self.authentication.mode.value,
                "local_user_id": str(self.authentication.local_user_id),
                "local_username": self.authentication.local_username,
                "local_display_name": self.authentication.local_display_name,
                "postgres_dsn": (
                    "configured" if self.authentication.postgres_dsn else "not configured"
                ),
                "session_ttl_seconds": self.authentication.session_ttl_seconds,
            },
            "external_services": {
                "tushare_token": (
                    "configured"
                    if self.external_services.tushare_token
                    else "not configured"
                ),
                "tushare_api_url": self.external_services.tushare_api_url,
                "tushare_request_interval_seconds": (
                    self.external_services.tushare_request_interval_seconds
                ),
                "tushare_max_retries": self.external_services.tushare_max_retries,
                "tushare_retry_backoff_seconds": (
                    self.external_services.tushare_retry_backoff_seconds
                ),
                "http_proxy": (
                    "configured" if self.external_services.http_proxy else "not configured"
                ),
                "https_proxy": (
                    "configured" if self.external_services.https_proxy else "not configured"
                ),
                "no_proxy": (
                    "configured" if self.external_services.no_proxy else "not configured"
                ),
            },
            "logging": {"level": self.logging.level},
            "runtime": {
                "environment": self.runtime.environment.value,
                "read_only": self.runtime.read_only,
                "env_file": str(self.runtime.env_file) if self.runtime.env_file else None,
                "platform": os.name,
                "remote_access_token_file": str(self.runtime.remote_access_token_file),
                "remote_access_database_path": str(self.runtime.remote_access_database_path),
                "remote_access_port": self.runtime.remote_access_port,
            },
            "sources": {
                name: source
                for name, source in sorted(self.sources.items())
                if name not in secret_names
            }
            | {
                name: source
                for name, source in sorted(self.sources.items())
                if name in secret_names
            },
        }

    def safe_json(self, *, indent: int = 2) -> str:
        return json.dumps(self.safe_dict(), ensure_ascii=False, indent=indent)


def redact_secrets(text: str, settings: AppSettings | None = None) -> str:
    """Remove configured secret values from arbitrary diagnostic text."""

    effective = settings or AppSettings.load()
    redacted = str(text)
    secrets = (
        effective.external_services.tushare_token,
        effective.authentication.postgres_dsn,
        effective.external_services.http_proxy,
        effective.external_services.https_proxy,
    )
    for secret in secrets:
        if secret:
            redacted = redacted.replace(secret, "***")
    lowered = redacted.lower()
    if any(marker in lowered for marker in ("token=", "authorization:", "bearer ")):
        return redacted[:200] + " ...[redacted]"
    return redacted


def require_writable(
    settings: AppSettings | None = None,
    *,
    operation: str = "write operation",
) -> AppSettings:
    """Reject configured write operations when QRP_READ_ONLY is enabled."""

    effective = settings or get_settings()
    if effective.runtime.read_only:
        raise RuntimeError(f"QRP_READ_ONLY=true forbids {operation}")
    return effective


def apply_proxy_environment(
    settings: AppSettings | None = None,
    *,
    clear_unconfigured: bool = False,
    default_no_proxy: str | None = None,
) -> None:
    """Apply validated outbound proxy settings to standard process variables.

    clear_unconfigured is reserved for compatibility launchers that historically
    disabled inherited proxies. Normal application code should keep it false.
    """

    effective = settings or get_settings()
    external = effective.external_services
    values = {
        "HTTP_PROXY": external.http_proxy,
        "HTTPS_PROXY": external.https_proxy,
        "NO_PROXY": external.no_proxy or default_no_proxy,
    }
    for name, value in values.items():
        aliases = (name, name.lower())
        if value:
            for alias in aliases:
                os.environ[alias] = value
        elif clear_unconfigured:
            for alias in aliases:
                os.environ.pop(alias, None)

SUPPORTED_ENV_VARS = frozenset(
    {
        "QRP_ENV_FILE",
        "QRP_HOME",
        "QRP_DATA_DIR",
        "QRP_DUCKDB_PATH",
        "QRP_IRM_QA_DUCKDB_PATH",
        "QRP_STATE_DIR",
        "QRP_JOB_RUNTIME_DIR",
        "QRP_BACKTEST_RUNS_DIR",
        "QRP_BACKTEST_TASKS_DIR",
        "QRP_ROBUSTNESS_RUNS_DIR",
        "QRP_DECLARATIVE_STRATEGIES_DIR",
        "QRP_LOG_DIR",
        "QRP_TMP_DIR",
        "QRP_READ_ONLY",
        "QRP_API_HOST",
        "QRP_API_PORT",
        "QRP_API_CORS_ORIGINS",
        "QRP_AUTH_MODE",
        "QRP_LOCAL_USER_ID",
        "QRP_LOCAL_USERNAME",
        "QRP_LOCAL_DISPLAY_NAME",
        "QRP_AUTH_DATABASE_URL",
        "QRP_AUTH_SESSION_TTL_SECONDS",
        "TUSHARE_TOKEN",
        "QRP_TUSHARE_API_URL",
        "QRP_TUSHARE_REQUEST_INTERVAL_SECONDS",
        "QRP_TUSHARE_MAX_RETRIES",
        "QRP_TUSHARE_RETRY_BACKOFF_SECONDS",
        "QRP_HTTP_PROXY",
        "QRP_HTTPS_PROXY",
        "QRP_NO_PROXY",
        "QRP_LOG_LEVEL",
        "QRP_RUNTIME_ENV",
        "QRP_REMOTE_ACCESS_TOKEN_FILE",
        "QRP_REMOTE_ACCESS_DB_PATH",
        "QRP_REMOTE_ACCESS_PORT",
        "QRP_EPISODE_DB_PATH",
        "QRP_POOL_DB_PATH",
    }
)

LEGACY_ENV_ALIASES = frozenset(
    {
        "QRP_DB_READ_ONLY",
        "QRP_ATLAS_BACKTEST_RUNS_DIR",
        "QRP_ATLAS_BACKTEST_TASKS_DIR",
        "QRP_ATLAS_ROBUSTNESS_RUNS_DIR",
        "QRP_ATLAS_DECLARATIVE_STRATEGIES_DIR",
    }
)


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    """Return the process-wide validated settings object."""

    return AppSettings.load()


def reset_settings_cache() -> None:
    """Clear cached settings for tests or explicit configuration reloads."""

    get_settings.cache_clear()
