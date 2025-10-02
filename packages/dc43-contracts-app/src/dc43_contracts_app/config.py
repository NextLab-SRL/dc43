from __future__ import annotations

"""Configuration helpers for the contracts web application."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Mapping, MutableMapping
import os

import tomllib

__all__ = [
    "WorkspaceConfig",
    "BackendProcessConfig",
    "BackendConfig",
    "ContractsAppConfig",
    "load_config",
]


@dataclass(slots=True)
class WorkspaceConfig:
    """Filesystem settings for the contracts application."""

    root: Path | None = None


@dataclass(slots=True)
class BackendProcessConfig:
    """Runtime options when launching a standalone backend process."""

    host: str = "127.0.0.1"
    port: int = 8001
    log_level: str | None = None

    def url(self) -> str:
        """Return the HTTP base URL derived from ``host``/``port``."""

        return f"http://{self.host}:{self.port}"


@dataclass(slots=True)
class BackendConfig:
    """Backend service configuration for the contracts UI."""

    mode: Literal["embedded", "remote"] = "embedded"
    base_url: str | None = None
    process: BackendProcessConfig = field(default_factory=BackendProcessConfig)


@dataclass(slots=True)
class ContractsAppConfig:
    """Top-level configuration for the contracts application."""

    workspace: WorkspaceConfig = field(default_factory=WorkspaceConfig)
    backend: BackendConfig = field(default_factory=BackendConfig)


def _first_existing_path(paths: list[str | os.PathLike[str] | None]) -> Path | None:
    for candidate in paths:
        if not candidate:
            continue
        path = Path(candidate).expanduser()
        if path.is_file():
            return path
    return None


def _load_toml(path: Path | None) -> Mapping[str, Any]:
    if not path:
        return {}
    try:
        text = path.read_bytes()
    except OSError:
        return {}
    try:
        return tomllib.loads(text.decode("utf-8"))
    except tomllib.TOMLDecodeError:
        return {}


def _coerce_path(value: Any) -> Path | None:
    if value in {None, ""}:
        return None
    return Path(str(value)).expanduser()


def _coerce_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def load_config(path: str | os.PathLike[str] | None = None) -> ContractsAppConfig:
    """Load configuration from ``path`` or fall back to defaults."""

    default_path = Path(__file__).with_name("config").joinpath("default.toml")
    env_path = os.getenv("DC43_CONTRACTS_APP_CONFIG")
    config_path = _first_existing_path([path, env_path, default_path])
    payload = _load_toml(config_path)

    explicit_path = None
    if path:
        try:
            explicit_path = Path(path).expanduser().resolve()
        except (OSError, RuntimeError):
            explicit_path = Path(path).expanduser()

    selected_path = None
    if config_path:
        try:
            selected_path = config_path.resolve()
        except (OSError, RuntimeError):
            selected_path = config_path

    allow_env_overrides = not (explicit_path and selected_path and selected_path == explicit_path)

    workspace_section = payload.get("workspace") if isinstance(payload, MutableMapping) else {}
    backend_section = payload.get("backend") if isinstance(payload, MutableMapping) else {}
    process_section: Mapping[str, Any]
    if isinstance(backend_section, MutableMapping):
        process_section = backend_section.get("process", {})  # type: ignore[assignment]
    else:
        backend_section = {}
        process_section = {}

    workspace_root = _coerce_path(workspace_section.get("root")) if isinstance(workspace_section, MutableMapping) else None

    backend_mode = str(backend_section.get("mode", "embedded")).lower() if isinstance(backend_section, MutableMapping) else "embedded"
    backend_base_url = backend_section.get("base_url") if isinstance(backend_section, MutableMapping) else None
    backend_base_url = str(backend_base_url).strip() or None if backend_base_url is not None else None
    if backend_base_url:
        backend_base_url = backend_base_url.rstrip("/")

    process_host = str(process_section.get("host", "127.0.0.1")) if isinstance(process_section, MutableMapping) else "127.0.0.1"
    process_port = _coerce_int(process_section.get("port"), 8001) if isinstance(process_section, MutableMapping) else 8001
    process_log_level_raw = process_section.get("log_level") if isinstance(process_section, MutableMapping) else None
    process_log_level = str(process_log_level_raw).strip() or None if process_log_level_raw is not None else None

    if allow_env_overrides:
        env_root = os.getenv("DC43_CONTRACTS_APP_WORK_DIR") or os.getenv("DC43_DEMO_WORK_DIR")
        if env_root:
            workspace_root = _coerce_path(env_root)

        env_mode = os.getenv("DC43_CONTRACTS_APP_BACKEND_MODE")
        if env_mode:
            backend_mode = env_mode.strip().lower() or backend_mode

        env_base_url = os.getenv("DC43_CONTRACTS_APP_BACKEND_URL") or os.getenv("DC43_DEMO_BACKEND_URL")
        if env_base_url:
            backend_base_url = env_base_url.strip().rstrip("/") or None

        env_host = os.getenv("DC43_CONTRACTS_APP_BACKEND_HOST") or os.getenv("DC43_DEMO_BACKEND_HOST")
        if env_host:
            process_host = env_host.strip() or process_host

        env_port = os.getenv("DC43_CONTRACTS_APP_BACKEND_PORT") or os.getenv("DC43_DEMO_BACKEND_PORT")
        if env_port:
            process_port = _coerce_int(env_port, process_port)

        env_log = os.getenv("DC43_CONTRACTS_APP_BACKEND_LOG") or os.getenv("DC43_DEMO_BACKEND_LOG")
        if env_log:
            process_log_level = env_log.strip() or process_log_level

    backend_config = BackendConfig(
        mode="remote" if backend_mode == "remote" else "embedded",
        base_url=backend_base_url,
        process=BackendProcessConfig(
            host=process_host,
            port=process_port,
            log_level=process_log_level,
        ),
    )

    return ContractsAppConfig(
        workspace=WorkspaceConfig(root=workspace_root),
        backend=backend_config,
    )
