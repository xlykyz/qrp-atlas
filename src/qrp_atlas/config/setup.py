"""Guided, repeatable runtime setup for QRP Atlas v1.0."""

from __future__ import annotations

import getpass
import io
import os
import re
import shutil
import socket
import sys
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path

from dotenv import dotenv_values

from qrp_atlas.config.operations import CheckResult, doctor, has_failures, initialize_runtime
from qrp_atlas.config.settings import AppSettings, ConfigError, PROJECT_ROOT, SUPPORTED_ENV_VARS
from qrp_atlas.database import create_empty_database, validate_existing_database


class SetupError(RuntimeError):
    """Raised for a user-actionable setup failure."""


class SetupCancelled(RuntimeError):
    """Raised when setup is safely cancelled before completion."""


class DatabaseMode(StrEnum):
    CREATE = "create"
    REUSE = "reuse"
    SKIP = "skip"


@dataclass(frozen=True, slots=True)
class Profile:
    name: str
    label: str
    description: str
    defaults: Mapping[str, str]


PROFILES = {
    "local": Profile(
        name="local",
        label="本地单用户",
        description="仅本机访问，使用本地身份。",
        defaults={
            "QRP_API_HOST": "127.0.0.1",
            "QRP_API_PORT": "8000",
            "QRP_API_CORS_ORIGINS": "*",
            "QRP_AUTH_MODE": "local",
            "QRP_RUNTIME_ENV": "development",
            "QRP_LOG_LEVEL": "INFO",
        },
    ),
    "lan": Profile(
        name="lan",
        label="可信局域网",
        description="监听局域网地址；不得直接暴露公网。",
        defaults={
            "QRP_API_HOST": "0.0.0.0",
            "QRP_API_PORT": "8000",
            "QRP_API_CORS_ORIGINS": "*",
            "QRP_AUTH_MODE": "local",
            "QRP_RUNTIME_ENV": "development",
            "QRP_LOG_LEVEL": "INFO",
        },
    ),
    "production": Profile(
        name="production",
        label="正式多用户部署",
        description="使用 PostgreSQL 认证和显式 CORS。",
        defaults={
            "QRP_API_HOST": "0.0.0.0",
            "QRP_API_PORT": "8000",
            "QRP_API_CORS_ORIGINS": "",
            "QRP_AUTH_MODE": "database",
            "QRP_RUNTIME_ENV": "production",
            "QRP_LOG_LEVEL": "INFO",
        },
    ),
}

MANAGED_ENV_VARS = frozenset(
    {
        "QRP_HOME",
        "QRP_DATA_DIR",
        "QRP_DUCKDB_PATH",
        "QRP_IRM_QA_DUCKDB_PATH",
        "QRP_JOB_RUNTIME_DIR",
        "QRP_JOB_RUNTIME_DB_PATH",
        "QRP_REMOTE_ACCESS_DB_PATH",
        "QRP_EPISODE_DB_PATH",
        "QRP_POOL_DB_PATH",
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
        "QRP_HTTP_PROXY",
        "QRP_HTTPS_PROXY",
        "QRP_NO_PROXY",
        "QRP_LOG_LEVEL",
        "QRP_RUNTIME_ENV",
    }
)

SECRET_ENV_VARS = frozenset(
    {
        "QRP_AUTH_DATABASE_URL",
        "TUSHARE_TOKEN",
        "QRP_HTTP_PROXY",
        "QRP_HTTPS_PROXY",
    }
)

_ASSIGNMENT = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=")


@dataclass(slots=True)
class SetupIO:
    input_stream: io.TextIOBase = field(default_factory=lambda: sys.stdin)
    output_stream: io.TextIOBase = field(default_factory=lambda: sys.stdout)
    error_stream: io.TextIOBase = field(default_factory=lambda: sys.stderr)
    secret_reader: Callable[[str], str] | None = None
    interactive: bool | None = None

    def is_interactive(self) -> bool:
        if self.interactive is not None:
            return self.interactive
        return self.input_stream.isatty() and self.output_stream.isatty()

    def write(self, message: str = "") -> None:
        print(message, file=self.output_stream)

    def error(self, message: str) -> None:
        print(message, file=self.error_stream)

    def read(self, prompt: str) -> str:
        self.output_stream.write(prompt)
        self.output_stream.flush()
        line = self.input_stream.readline()
        if line == "":
            raise SetupCancelled("输入已结束，未保存任何配置。")
        return line.rstrip("\r\n")

    def read_secret(self, prompt: str) -> str:
        if self.secret_reader is not None:
            return self.secret_reader(prompt)
        try:
            return getpass.getpass(prompt, stream=self.output_stream)
        except EOFError as exc:
            raise SetupCancelled("输入已结束，未保存任何配置。") from exc


@dataclass(frozen=True, slots=True)
class SetupOptions:
    profile: str | None = None
    env_file: str | Path | None = None
    non_interactive: bool = False
    assume_yes: bool = False
    update_existing: bool = False
    database_mode: DatabaseMode | None = None
    home: str | None = None
    data_dir: str | None = None
    duckdb_path: str | None = None
    irm_qa_duckdb_path: str | None = None
    api_host: str | None = None
    api_port: int | None = None
    cors_origins: Sequence[str] = ()
    auth_mode: str | None = None
    local_username: str | None = None
    local_display_name: str | None = None


@dataclass(frozen=True, slots=True)
class SetupResult:
    settings: AppSettings
    env_file: Path
    profile: Profile
    database_mode: DatabaseMode
    database_status: str
    checks: tuple[CheckResult, ...]
    backup_path: Path | None

    @property
    def launch_command(self) -> str:
        default = self.settings.project_root / ".env"
        if self.env_file == default:
            return "qrp-atlas-api"
        return f'qrp-atlas-api --env-file "{self.env_file}"'


def _default_external_paths(project_root: Path) -> tuple[str, str]:
    base = Path.home() / ".qrp-atlas"
    return str(base / "runtime"), str(base / "data")


def profile_values(profile_name: str, *, project_root: Path = PROJECT_ROOT) -> dict[str, str]:
    try:
        profile = PROFILES[profile_name]
    except KeyError as exc:
        raise SetupError(f"未知使用场景：{profile_name}") from exc
    home, data_dir = _default_external_paths(project_root)
    values = dict(profile.defaults)
    values.update(
        {
            "QRP_HOME": home,
            "QRP_DATA_DIR": data_dir,
            "QRP_DUCKDB_PATH": str(Path(data_dir) / "db" / "quant.db"),
            "QRP_IRM_QA_DUCKDB_PATH": str(Path(data_dir) / "db" / "irm_qa.duckdb"),
            "QRP_AUTH_SESSION_TTL_SECONDS": "604800",
            "QRP_LOCAL_USERNAME": "ryan",
            "QRP_LOCAL_DISPLAY_NAME": "Ryan",
        }
    )
    return values


def _resolve_display_path(value: str, *, project_root: Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = project_root / path
    return path.resolve(strict=False)


def _read_existing(path: Path) -> tuple[str, dict[str, str]]:
    if not path.exists():
        return "", {}
    if not path.is_file():
        raise SetupError(f"配置路径不是普通文件：{path}")
    try:
        text = path.read_text(encoding="utf-8")
        parsed = dotenv_values(path)
    except OSError as exc:
        raise SetupError(f"无法读取现有配置文件：{path}") from exc
    return text, {name: value for name, value in parsed.items() if value is not None}


def _quote_env(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def render_env_file(original: str, values: Mapping[str, str]) -> str:
    """Update managed assignments while preserving comments and unknown variables."""

    remaining = dict(values)
    rendered: list[str] = []
    seen: set[str] = set()
    for line in original.splitlines():
        match = _ASSIGNMENT.match(line)
        name = match.group(1) if match else None
        if name in MANAGED_ENV_VARS:
            if name in seen:
                continue
            seen.add(name)
            value = remaining.pop(name, None)
            if value is not None and value != "":
                rendered.append(f"{name}={_quote_env(value)}")
            continue
        rendered.append(line)

    if remaining:
        if rendered and rendered[-1].strip():
            rendered.append("")
        rendered.append("# --- Managed by qrp-atlas-config setup ---")
        for name in sorted(remaining):
            value = remaining[name]
            if value != "":
                rendered.append(f"{name}={_quote_env(value)}")
    return "\n".join(rendered).rstrip() + "\n"


def validate_candidate(
    values: Mapping[str, str],
    *,
    project_root: Path,
    original: str = "",
) -> AppSettings:
    content = render_env_file(original, values)
    with tempfile.TemporaryDirectory(prefix="qrp-atlas-setup-") as directory:
        candidate = Path(directory) / "candidate.env"
        candidate.write_text(content, encoding="utf-8")
        return AppSettings.load(env_file=candidate, environ={}, project_root=project_root)


def _next_backup_path(path: Path) -> Path:
    candidate = path.with_name(path.name + ".bak")
    index = 1
    while candidate.exists():
        candidate = path.with_name(f"{path.name}.bak.{index}")
        index += 1
    return candidate


def atomic_write_env(path: Path, content: str) -> Path | None:
    """Atomically write a protected dotenv file and retain a protected backup."""

    path.parent.mkdir(parents=True, exist_ok=True)
    backup: Path | None = None
    if path.exists():
        backup = _next_backup_path(path)
        shutil.copy2(path, backup)
        if os.name == "posix":
            backup.chmod(0o600)

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
        text=True,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        if os.name == "posix":
            temporary.chmod(0o600)
        temporary.replace(path)
        if os.name == "posix":
            path.chmod(0o600)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return backup


def _restore_env(path: Path, *, existed: bool, backup: Path | None) -> None:
    if existed and backup and backup.exists():
        shutil.copy2(backup, path)
        if os.name == "posix":
            path.chmod(0o600)
    elif not existed:
        path.unlink(missing_ok=True)


def _choice(io_adapter: SetupIO, prompt: str, choices: Mapping[str, str], default: str) -> str:
    labels = "/".join(f"{key}:{label}" for key, label in choices.items())
    while True:
        value = io_adapter.read(f"{prompt} [{labels}]（默认 {default}）: ").strip().lower()
        value = value or default
        if value in choices:
            return value
        io_adapter.write("请输入有效选项。")


def _text(io_adapter: SetupIO, prompt: str, default: str, *, required: bool = True) -> str:
    while True:
        suffix = f" [{default}]" if default else ""
        value = io_adapter.read(f"{prompt}{suffix}: ").strip()
        value = value or default
        if value or not required:
            return value
        io_adapter.write("该项不能为空。")


def _confirm(io_adapter: SetupIO, prompt: str, *, default: bool = False) -> bool:
    marker = "Y/n" if default else "y/N"
    while True:
        value = io_adapter.read(f"{prompt} [{marker}]: ").strip().lower()
        if not value:
            return default
        if value in {"y", "yes", "是"}:
            return True
        if value in {"n", "no", "否"}:
            return False
        io_adapter.write("请输入 y 或 n。")


def _port_is_available(host: str, port: int) -> bool:
    probe_host = "127.0.0.1" if host in {"0.0.0.0", "::"} else host
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as connection:
            connection.settimeout(0.2)
            return connection.connect_ex((probe_host, port)) != 0
    except OSError:
        return True


def _validate_profile_safety(
    profile: Profile,
    values: Mapping[str, str],
    *,
    project_root: Path,
) -> None:
    if profile.name == "production":
        if values.get("QRP_RUNTIME_ENV") != "production":
            raise SetupError("production 场景必须使用 QRP_RUNTIME_ENV=production")
        if values.get("QRP_AUTH_MODE") != "database":
            raise SetupError("production 场景必须使用 database 认证")
        if not values.get("QRP_AUTH_DATABASE_URL"):
            raise SetupError("production 场景缺少 PostgreSQL DSN")
        cors = values.get("QRP_API_CORS_ORIGINS", "")
        if not cors or "*" in {item.strip() for item in cors.split(",")}:
            raise SetupError("production 场景必须配置显式 CORS，不能使用通配符")
        for name in ("QRP_HOME", "QRP_DATA_DIR"):
            path = _resolve_display_path(values[name], project_root=project_root)
            try:
                path.relative_to(project_root)
            except ValueError:
                continue
            raise SetupError(f"production 场景的 {name} 必须位于仓库之外")


def _show_existing_summary(io_adapter: SetupIO, settings: AppSettings) -> None:
    io_adapter.write("检测到已有配置（秘密已脱敏）：")
    io_adapter.write(f"  运行目录：{settings.paths.home}")
    io_adapter.write(f"  数据目录：{settings.paths.data_dir}")
    io_adapter.write(f"  DuckDB：{settings.paths.duckdb_path}")
    io_adapter.write(f"  API：http://{settings.api.host}:{settings.api.port}")
    io_adapter.write(f"  认证：{settings.authentication.mode.value}")
    io_adapter.write(
        "  Tushare："
        + ("已配置" if settings.external_services.tushare_token else "未配置")
    )


def _interactive_values(
    io_adapter: SetupIO,
    *,
    profile: Profile,
    values: dict[str, str],
    project_root: Path,
) -> tuple[dict[str, str], DatabaseMode]:
    io_adapter.write(f"\n使用场景：{profile.label} — {profile.description}")
    values["QRP_HOME"] = _text(io_adapter, "运行目录", values["QRP_HOME"])
    io_adapter.write(f"  解析为：{_resolve_display_path(values['QRP_HOME'], project_root=project_root)}")
    values["QRP_DATA_DIR"] = _text(io_adapter, "持久数据目录", values["QRP_DATA_DIR"])
    io_adapter.write(
        f"  解析为：{_resolve_display_path(values['QRP_DATA_DIR'], project_root=project_root)}"
    )
    default_database = str(Path(values["QRP_DATA_DIR"]).expanduser() / "db" / "quant.db")
    profile_database = profile_values(profile.name, project_root=project_root)[
        "QRP_DUCKDB_PATH"
    ]
    database_default = values.get("QRP_DUCKDB_PATH", default_database)
    if database_default == profile_database:
        database_default = default_database
    values["QRP_DUCKDB_PATH"] = _text(
        io_adapter,
        "DuckDB 文件路径",
        database_default,
    )

    default_irm_qa_database = str(
        Path(values["QRP_DATA_DIR"]).expanduser() / "db" / "irm_qa.duckdb"
    )
    profile_irm_qa_database = profile_values(profile.name, project_root=project_root)[
        "QRP_IRM_QA_DUCKDB_PATH"
    ]
    irm_qa_database_default = values.get(
        "QRP_IRM_QA_DUCKDB_PATH", default_irm_qa_database
    )
    if irm_qa_database_default == profile_irm_qa_database:
        irm_qa_database_default = default_irm_qa_database
    values["QRP_IRM_QA_DUCKDB_PATH"] = _text(
        io_adapter,
        "IRM 独立 DuckDB 文件路径",
        irm_qa_database_default,
    )

    database_choice = _choice(
        io_adapter,
        "数据库处理",
        {"1": "创建空库", "2": "复用已有库", "3": "暂不创建"},
        "1",
    )
    database_mode = {
        "1": DatabaseMode.CREATE,
        "2": DatabaseMode.REUSE,
        "3": DatabaseMode.SKIP,
    }[database_choice]

    if profile.name == "lan":
        io_adapter.write("警告：LAN 模式仅适用于可信局域网，不得直接暴露公网。")
        if not _confirm(io_adapter, "确认继续使用 LAN 模式"):
            raise SetupCancelled("已取消，未保存任何配置。")

    values["QRP_API_HOST"] = _text(io_adapter, "API 监听地址", values["QRP_API_HOST"])
    while True:
        values["QRP_API_PORT"] = _text(io_adapter, "API 端口", values["QRP_API_PORT"])
        try:
            port = int(values["QRP_API_PORT"])
        except ValueError:
            io_adapter.write("端口必须是 1 到 65535 的整数。")
            continue
        if not 1 <= port <= 65535:
            io_adapter.write("端口必须是 1 到 65535 的整数。")
            continue
        if not _port_is_available(values["QRP_API_HOST"], port):
            io_adapter.write(f"警告：端口 {port} 当前可能已被占用。")
            if not _confirm(io_adapter, "仍然使用该端口"):
                continue
        break
    cors_default = values.get("QRP_API_CORS_ORIGINS", "")
    values["QRP_API_CORS_ORIGINS"] = _text(
        io_adapter,
        "CORS 来源（逗号分隔）",
        cors_default,
        required=True,
    )
    if profile.name in {"lan", "production"} and values["QRP_API_CORS_ORIGINS"] == "*":
        if profile.name == "production":
            io_adapter.write("production 不允许通配符 CORS。")
            values["QRP_API_CORS_ORIGINS"] = _text(io_adapter, "请输入显式 CORS 来源", "")
        elif not _confirm(io_adapter, "确认允许任意浏览器来源访问此局域网 API"):
            values["QRP_API_CORS_ORIGINS"] = _text(io_adapter, "请输入显式 CORS 来源", "")

    auth_default = values.get("QRP_AUTH_MODE", profile.defaults["QRP_AUTH_MODE"])
    if profile.name == "production":
        auth_choice = "database"
        io_adapter.write("认证方式：PostgreSQL 多用户（production 必需）")
    else:
        auth_choice = _choice(
            io_adapter,
            "认证方式",
            {"local": "本地单用户", "database": "PostgreSQL 多用户"},
            auth_default,
        )
    values["QRP_AUTH_MODE"] = auth_choice
    if auth_choice == "local":
        values["QRP_LOCAL_USERNAME"] = _text(
            io_adapter, "登录名", values.get("QRP_LOCAL_USERNAME", "ryan")
        )
        values["QRP_LOCAL_DISPLAY_NAME"] = _text(
            io_adapter, "显示名", values.get("QRP_LOCAL_DISPLAY_NAME", "Ryan")
        )
    else:
        preserved = bool(values.get("QRP_AUTH_DATABASE_URL"))
        prompt = "PostgreSQL DSN（留空保留现有值）: " if preserved else "PostgreSQL DSN: "
        entered = io_adapter.read_secret(prompt).strip()
        if entered:
            values["QRP_AUTH_DATABASE_URL"] = entered
        elif not preserved:
            raise SetupError("database 认证必须配置 PostgreSQL DSN")

    has_token = bool(values.get("TUSHARE_TOKEN"))
    if _confirm(io_adapter, "配置 Tushare", default=has_token):
        prompt = "Tushare Token（留空保留现有值）: " if has_token else "Tushare Token: "
        token = io_adapter.read_secret(prompt).strip()
        if token:
            values["TUSHARE_TOKEN"] = token
        elif not has_token:
            raise SetupError("已选择配置 Tushare，但未输入 Token")
    else:
        values.pop("TUSHARE_TOKEN", None)
        io_adapter.write("Tushare pipeline 暂不可用，但后端和空数据库仍可启动。")

    has_proxy = any(values.get(name) for name in ("QRP_HTTP_PROXY", "QRP_HTTPS_PROXY"))
    if _confirm(io_adapter, "配置出站代理", default=has_proxy):
        for name, label in (
            ("QRP_HTTP_PROXY", "HTTP proxy"),
            ("QRP_HTTPS_PROXY", "HTTPS proxy"),
        ):
            preserved = bool(values.get(name))
            prompt = f"{label}（留空保留现有值）: " if preserved else f"{label}（可留空）: "
            entered = io_adapter.read_secret(prompt).strip()
            if entered:
                values[name] = entered
        values["QRP_NO_PROXY"] = _text(
            io_adapter,
            "NO_PROXY",
            values.get("QRP_NO_PROXY", "localhost,127.0.0.1"),
            required=False,
        )
    else:
        for name in ("QRP_HTTP_PROXY", "QRP_HTTPS_PROXY", "QRP_NO_PROXY"):
            values.pop(name, None)
    return values, database_mode


def _summary(
    io_adapter: SetupIO,
    *,
    profile: Profile,
    env_file: Path,
    settings: AppSettings,
    database_mode: DatabaseMode,
) -> None:
    database_labels = {
        DatabaseMode.CREATE: "创建新空数据库",
        DatabaseMode.REUSE: "验证并复用已有数据库",
        DatabaseMode.SKIP: "暂不创建数据库",
    }
    io_adapter.write("\n配置摘要（秘密已脱敏）")
    io_adapter.write(f"  使用场景：{profile.label}")
    io_adapter.write(f"  配置文件：{env_file}")
    io_adapter.write(f"  运行目录：{settings.paths.home}")
    io_adapter.write(f"  数据目录：{settings.paths.data_dir}")
    io_adapter.write(f"  DuckDB：{database_labels[database_mode]} ({settings.paths.duckdb_path})")
    io_adapter.write(f"  API：http://{settings.api.host}:{settings.api.port}")
    io_adapter.write(f"  认证：{settings.authentication.mode.value}")
    io_adapter.write(
        "  PostgreSQL DSN："
        + ("已配置" if settings.authentication.postgres_dsn else "未配置")
    )
    io_adapter.write(
        "  Tushare："
        + ("已配置" if settings.external_services.tushare_token else "未配置")
    )
    io_adapter.write(
        "  代理："
        + (
            "已配置"
            if settings.external_services.http_proxy or settings.external_services.https_proxy
            else "未配置"
        )
    )


def _non_interactive_values(
    options: SetupOptions,
    *,
    profile: Profile,
    existing: Mapping[str, str],
    environ: Mapping[str, str],
    project_root: Path,
) -> tuple[dict[str, str], DatabaseMode]:
    values = profile_values(profile.name, project_root=project_root)
    values.update({name: value for name, value in existing.items() if name in MANAGED_ENV_VARS})
    values.update(
        {
            name: value
            for name, value in environ.items()
            if name in MANAGED_ENV_VARS and value != ""
        }
    )
    explicit = {
        "QRP_HOME": options.home,
        "QRP_DATA_DIR": options.data_dir,
        "QRP_DUCKDB_PATH": options.duckdb_path,
        "QRP_IRM_QA_DUCKDB_PATH": options.irm_qa_duckdb_path,
        "QRP_API_HOST": options.api_host,
        "QRP_API_PORT": str(options.api_port) if options.api_port is not None else None,
        "QRP_API_CORS_ORIGINS": ",".join(options.cors_origins) if options.cors_origins else None,
        "QRP_AUTH_MODE": options.auth_mode,
        "QRP_LOCAL_USERNAME": options.local_username,
        "QRP_LOCAL_DISPLAY_NAME": options.local_display_name,
    }
    values.update({name: value for name, value in explicit.items() if value is not None})
    if (
        options.data_dir is not None
        and options.duckdb_path is None
        and "QRP_DUCKDB_PATH" not in existing
        and "QRP_DUCKDB_PATH" not in environ
    ):
        values["QRP_DUCKDB_PATH"] = str(
            Path(values["QRP_DATA_DIR"]).expanduser() / "db" / "quant.db"
        )
    database_mode = options.database_mode or DatabaseMode.CREATE
    if profile.name == "production":
        if not values.get("QRP_API_CORS_ORIGINS"):
            raise SetupError("非交互 production setup 缺少显式 QRP_API_CORS_ORIGINS")
        if not values.get("QRP_AUTH_DATABASE_URL"):
            raise SetupError("非交互 production setup 缺少环境变量 QRP_AUTH_DATABASE_URL")
    return values, database_mode


def run_setup(
    options: SetupOptions,
    *,
    io_adapter: SetupIO | None = None,
    environ: Mapping[str, str] | None = None,
    project_root: str | Path = PROJECT_ROOT,
) -> SetupResult:
    """Run guided or deterministic setup and return the verified result."""

    adapter = io_adapter or SetupIO()
    environment = dict(os.environ if environ is None else environ)
    root = Path(project_root).resolve(strict=False)
    if not options.non_interactive and not adapter.is_interactive():
        raise SetupError(
            "setup 需要交互式终端；自动化环境请使用 --non-interactive 并提供必要配置。"
        )

    adapter.write("QRP Atlas v1.0 初始配置")
    adapter.write("")
    adapter.write("该向导将帮助你配置运行目录、数据目录、数据库、")
    adapter.write("API、认证和可选外部服务。")
    adapter.write("")
    adapter.write("现有文件不会被静默覆盖。")
    adapter.write("秘密不会在终端摘要中显示。")

    default_env = root / ".env"
    if options.env_file is not None:
        env_file = _resolve_display_path(str(options.env_file), project_root=root)
    elif options.non_interactive:
        env_file = default_env
    else:
        env_file = _resolve_display_path(
            _text(adapter, "配置文件位置", str(default_env)), project_root=root
        )
    adapter.write(f"配置文件：{env_file}")

    original, existing = _read_existing(env_file)
    existed = env_file.exists()
    if existed:
        try:
            existing_settings = AppSettings.load(env_file=env_file, environ={}, project_root=root)
        except ConfigError as exc:
            raise SetupError(f"现有配置无法通过 AppSettings 校验：{exc}") from exc
        _show_existing_summary(adapter, existing_settings)
        if options.non_interactive:
            if not options.update_existing:
                raise SetupError("配置文件已存在；非交互更新必须显式使用 --update-existing")
        elif not _confirm(adapter, "更新该配置文件"):
            raise SetupCancelled("已取消，未保存任何配置。")

    if options.profile:
        try:
            profile = PROFILES[options.profile]
        except KeyError as exc:
            raise SetupError(f"未知使用场景：{options.profile}") from exc
    elif options.non_interactive:
        raise SetupError("非交互 setup 必须指定 --profile")
    else:
        selected = _choice(
            adapter,
            "选择使用场景",
            {name: item.label for name, item in PROFILES.items()},
            "local",
        )
        profile = PROFILES[selected]

    if not options.non_interactive:
        values = profile_values(profile.name, project_root=root)
        values.update({name: value for name, value in existing.items() if name in MANAGED_ENV_VARS})
        values["QRP_RUNTIME_ENV"] = profile.defaults["QRP_RUNTIME_ENV"]

    while True:
        if options.non_interactive:
            values, database_mode = _non_interactive_values(
                options,
                profile=profile,
                existing=existing,
                environ=environment,
                project_root=root,
            )
        else:
            values, database_mode = _interactive_values(
                adapter,
                profile=profile,
                values=values,
                project_root=root,
            )

        _validate_profile_safety(profile, values, project_root=root)
        try:
            candidate = validate_candidate(values, project_root=root, original=original)
        except ConfigError as exc:
            raise SetupError(f"候选配置未通过 AppSettings 校验：{exc}") from exc
        if options.non_interactive and not _port_is_available(
            candidate.api.host, candidate.api.port
        ):
            adapter.write(f"警告：API 端口 {candidate.api.port} 当前可能已被占用。")

        database_path = candidate.paths.duckdb_path
        if database_mode is DatabaseMode.CREATE and database_path.exists():
            if options.non_interactive:
                raise SetupError(f"不会覆盖已有数据库：{database_path}")
            adapter.write(f"数据库路径已存在：{database_path}")
            if _confirm(adapter, "改为只读验证并复用该数据库"):
                database_mode = DatabaseMode.REUSE
            else:
                raise SetupCancelled("已取消，未保存任何配置。")
        if database_mode is DatabaseMode.REUSE:
            try:
                validate_existing_database(database_path)
            except Exception as exc:
                raise SetupError(f"已有 DuckDB 无法只读验证：{type(exc).__name__}") from exc

        _summary(
            adapter,
            profile=profile,
            env_file=env_file,
            settings=candidate,
            database_mode=database_mode,
        )
        if options.non_interactive:
            if not options.assume_yes:
                raise SetupError("非交互 setup 必须使用 --yes 确认写入")
            break
        action = _choice(
            adapter,
            "下一步",
            {"save": "保存并初始化", "back": "返回修改", "cancel": "取消"},
            "save",
        )
        if action == "save":
            break
        if action == "cancel":
            raise SetupCancelled("已取消，未保存任何配置。")

    content = render_env_file(original, values)
    backup: Path | None = None
    created_database = False
    try:
        backup = atomic_write_env(env_file, content)
        settings = AppSettings.load(env_file=env_file, environ={}, project_root=root)
        init_results = initialize_runtime(settings)
        if has_failures(init_results):
            raise SetupError("运行目录初始化失败；原配置已恢复")

        if database_mode is DatabaseMode.CREATE:
            create_empty_database(settings.paths.duckdb_path)
            created_database = True
            database_status = "已创建"
        elif database_mode is DatabaseMode.REUSE:
            validate_existing_database(settings.paths.duckdb_path)
            database_status = "已验证"
        else:
            database_status = "尚未创建"

        checks = tuple(doctor(settings))
        if has_failures(list(checks)):
            raise SetupError("doctor 检测到阻塞问题；原配置已恢复")
    except Exception:
        if created_database:
            settings.paths.duckdb_path.unlink(missing_ok=True)
        _restore_env(env_file, existed=existed, backup=backup)
        raise

    result = SetupResult(
        settings=settings,
        env_file=env_file,
        profile=profile,
        database_mode=database_mode,
        database_status=database_status,
        checks=checks,
        backup_path=backup,
    )
    adapter.write("\nQRP Atlas 配置完成")
    adapter.write("")
    adapter.write(f"配置文件：{env_file}")
    adapter.write(f"数据目录：{settings.paths.data_dir}")
    adapter.write(f"数据库：{database_status}")
    adapter.write("诊断结果：通过（可能包含非阻塞警告）")
    if database_mode is DatabaseMode.SKIP:
        adapter.write("后端真实数据库接口在 DuckDB 准备完成前不可用。")
    adapter.write("")
    adapter.write("启动后端：")
    adapter.write(result.launch_command)
    adapter.write("")
    adapter.write("访问：")
    adapter.write(f"http://{settings.api.host}:{settings.api.port}/docs")
    if backup:
        adapter.write(f"原配置备份：{backup}")
    return result
