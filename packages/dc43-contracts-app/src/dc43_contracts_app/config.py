from __future__ import annotations

"""Configuration helpers for the contracts web application."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Literal, Mapping, MutableMapping, Sequence
import json
import os
import re

import tomllib

try:
    import tomlkit
except ModuleNotFoundError:  # pragma: no cover - exercised via fallback tests
    tomlkit = None

__all__ = [
    "WorkspaceConfig",
    "BackendProcessConfig",
    "BackendConfig",
    "ContractsAppConfig",
    "DocsChatConfig",
    "load_config",
    "config_to_mapping",
    "mapping_to_toml",
    "dumps",
    "dump",
]


_BARE_KEY_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")


def _format_key(value: str) -> str:
    """Return ``value`` formatted as a TOML key."""

    if _BARE_KEY_PATTERN.match(value):
        return value
    return json.dumps(value)


def _format_value(value: Any) -> str:
    """Return ``value`` rendered as TOML without relying on ``tomlkit``."""

    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if value is None:
        return '""'
    if isinstance(value, Mapping):
        if not value:
            return "{}"
        items = [
            f"{_format_key(str(key))} = {_format_value(item)}"
            for key, item in value.items()
        ]
        return "{ " + ", ".join(items) + " }"
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, str)):
        values = ", ".join(_format_value(item) for item in value)
        return f"[ {values} ]" if values else "[]"
    return json.dumps(str(value))


def _join_table(parts: Iterable[str]) -> str:
    """Return a dotted table path for ``parts``."""

    return ".".join(_format_key(part) for part in parts)


def _write_table(
    mapping: Mapping[str, Any],
    lines: list[str],
    prefix: tuple[str, ...] = (),
) -> None:
    """Append TOML lines representing ``mapping`` to ``lines``."""

    scalar_items: list[tuple[str, Any]] = []
    table_items: list[tuple[str, Mapping[str, Any]]] = []

    for key, value in mapping.items():
        key_str = str(key)
        if isinstance(value, Mapping):
            table_items.append((key_str, value))
            continue
        scalar_items.append((key_str, value))

    for key, value in scalar_items:
        lines.append(f"{_format_key(key)} = {_format_value(value)}")

    for key, value in table_items:
        table_prefix = prefix + (key,)
        has_scalars = any(not isinstance(item, Mapping) for item in value.values())
        if has_scalars or not value:
            if lines and lines[-1] != "":
                lines.append("")
            lines.append(f"[{_join_table(table_prefix)}]")
        _write_table(value, lines, table_prefix)


def _toml_dumps(payload: Mapping[str, Any]) -> str:
    """Return TOML for ``payload`` using ``tomlkit`` when available."""

    if tomlkit is not None:  # pragma: no branch
        return tomlkit.dumps(payload)
    lines: list[str] = []
    _write_table(payload, lines)
    if not lines:
        return ""
    text = "\n".join(lines)
    if not text.endswith("\n"):
        text += "\n"
    return text


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
class DocsChatConfig:
    """Configuration for the documentation chat assistant."""

    enabled: bool = False
    provider: str = "openai"
    model: str = "gpt-4o-mini"
    embedding_provider: str = "huggingface"
    embedding_model: str = "text-embedding-3-small"
    api_key_env: str = "OPENAI_API_KEY"
    api_key: str | None = None
    docs_path: Path | None = None
    index_path: Path | None = None
    code_paths: tuple[Path, ...] = ()
    reasoning_effort: str | None = None


@dataclass(slots=True)
class ContractsAppConfig:
    """Top-level configuration for the contracts application."""

    workspace: WorkspaceConfig = field(default_factory=WorkspaceConfig)
    backend: BackendConfig = field(default_factory=BackendConfig)
    docs_chat: DocsChatConfig = field(default_factory=DocsChatConfig)


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


def _coerce_path_list(value: Any) -> tuple[Path, ...]:
    if value is None:
        return ()
    if isinstance(value, str) and not value.strip():
        return ()
    paths: list[Path] = []
    if isinstance(value, (list, tuple, set)):
        items = value
    else:
        text = str(value)
        separators = [os.pathsep, ",", ";"]
        for sep in separators:
            if sep in text:
                items = [item.strip() for item in text.split(sep)]
                break
        else:
            items = [text]
    for item in items:
        if not item:
            continue
        path = _coerce_path(item)
        if path:
            paths.append(path)
    return tuple(paths)


def _coerce_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"true", "1", "yes", "on"}:
            return True
        if text in {"false", "0", "no", "off"}:
            return False
        if text == "":
            return default
    return default


def _looks_like_env_var_name(value: str) -> bool:
    if not value:
        return False
    first = value[0]
    if not (first.isalpha() or first == "_"):
        return False
    for char in value[1:]:
        if not (char.isalnum() or char == "_"):
            return False
    return True


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
    docs_chat_section = payload.get("docs_chat") if isinstance(payload, MutableMapping) else {}
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

    docs_chat_enabled = _coerce_bool(docs_chat_section.get("enabled"), False) if isinstance(docs_chat_section, MutableMapping) else False
    docs_chat_provider = (
        str(docs_chat_section.get("provider", "openai")).strip()
        if isinstance(docs_chat_section, MutableMapping)
        else "openai"
    ) or "openai"
    docs_chat_model = (
        str(docs_chat_section.get("model", "gpt-4o-mini")).strip()
        if isinstance(docs_chat_section, MutableMapping)
        else "gpt-4o-mini"
    ) or "gpt-4o-mini"
    docs_chat_embedding_provider = (
        str(docs_chat_section.get("embedding_provider", "huggingface")).strip()
        if isinstance(docs_chat_section, MutableMapping)
        else "huggingface"
    ) or "huggingface"
    docs_chat_embedding_model = (
        str(docs_chat_section.get("embedding_model", "text-embedding-3-small")).strip()
        if isinstance(docs_chat_section, MutableMapping)
        else "text-embedding-3-small"
    ) or "text-embedding-3-small"
    docs_chat_api_key_env = (
        str(docs_chat_section.get("api_key_env", "OPENAI_API_KEY")).strip()
        if isinstance(docs_chat_section, MutableMapping)
        else "OPENAI_API_KEY"
    ) or "OPENAI_API_KEY"
    docs_chat_api_key = None
    if isinstance(docs_chat_section, MutableMapping):
        raw_value = docs_chat_section.get("api_key")
        if raw_value is not None:
            value_text = str(raw_value).strip()
            docs_chat_api_key = value_text or None

    docs_chat_docs_path = (
        _coerce_path(docs_chat_section.get("docs_path"))
        if isinstance(docs_chat_section, MutableMapping)
        else None
    )
    docs_chat_index_path = (
        _coerce_path(docs_chat_section.get("index_path"))
        if isinstance(docs_chat_section, MutableMapping)
        else None
    )
    docs_chat_code_paths: tuple[Path, ...] = ()
    if isinstance(docs_chat_section, MutableMapping):
        raw_code_paths = docs_chat_section.get("code_paths")
        if raw_code_paths is None:
            for legacy_key in ("code-paths", "code-path"):
                if legacy_key in docs_chat_section:
                    raw_code_paths = docs_chat_section.get(legacy_key)
                    break
        docs_chat_code_paths = _coerce_path_list(raw_code_paths)
    docs_chat_reasoning_effort = None
    if isinstance(docs_chat_section, MutableMapping):
        raw_reasoning = docs_chat_section.get("reasoning_effort")
        if raw_reasoning is not None:
            value_text = str(raw_reasoning).strip()
            docs_chat_reasoning_effort = value_text or None

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

        env_docs_enabled = os.getenv("DC43_CONTRACTS_APP_DOCS_CHAT_ENABLED")
        if env_docs_enabled is not None:
            docs_chat_enabled = _coerce_bool(env_docs_enabled, docs_chat_enabled)

        env_docs_provider = os.getenv("DC43_CONTRACTS_APP_DOCS_CHAT_PROVIDER")
        if env_docs_provider:
            docs_chat_provider = env_docs_provider.strip() or docs_chat_provider

        env_docs_model = os.getenv("DC43_CONTRACTS_APP_DOCS_CHAT_MODEL")
        if env_docs_model:
            docs_chat_model = env_docs_model.strip() or docs_chat_model

        env_docs_embedding_provider = os.getenv("DC43_CONTRACTS_APP_DOCS_CHAT_EMBEDDING_PROVIDER")
        if env_docs_embedding_provider:
            docs_chat_embedding_provider = env_docs_embedding_provider.strip() or docs_chat_embedding_provider

        env_docs_embedding = os.getenv("DC43_CONTRACTS_APP_DOCS_CHAT_EMBEDDING_MODEL")
        if env_docs_embedding:
            docs_chat_embedding_model = env_docs_embedding.strip() or docs_chat_embedding_model

        env_docs_api_key_env = os.getenv("DC43_CONTRACTS_APP_DOCS_CHAT_API_KEY_ENV")
        if env_docs_api_key_env:
            docs_chat_api_key_env = env_docs_api_key_env.strip() or docs_chat_api_key_env

        env_docs_api_key = os.getenv("DC43_CONTRACTS_APP_DOCS_CHAT_API_KEY")
        if env_docs_api_key is not None:
            docs_chat_api_key = env_docs_api_key.strip() or None

        env_docs_path = os.getenv("DC43_CONTRACTS_APP_DOCS_CHAT_PATH")
        if env_docs_path:
            docs_chat_docs_path = _coerce_path(env_docs_path)

        env_docs_index = os.getenv("DC43_CONTRACTS_APP_DOCS_CHAT_INDEX")
        if env_docs_index:
            docs_chat_index_path = _coerce_path(env_docs_index)

        env_docs_code = os.getenv("DC43_CONTRACTS_APP_DOCS_CHAT_CODE_PATHS")
        if env_docs_code:
            docs_chat_code_paths = _coerce_path_list(env_docs_code)

        env_docs_reasoning = os.getenv("DC43_CONTRACTS_APP_DOCS_CHAT_REASONING_EFFORT")
        if env_docs_reasoning is not None:
            value_text = env_docs_reasoning.strip()
            docs_chat_reasoning_effort = value_text or None

    embedding_provider_normalized = docs_chat_embedding_provider.lower()
    if embedding_provider_normalized != "openai" and docs_chat_embedding_model == "text-embedding-3-small":
        docs_chat_embedding_model = "sentence-transformers/all-MiniLM-L6-v2"

    if docs_chat_api_key is None and docs_chat_api_key_env:
        if not _looks_like_env_var_name(docs_chat_api_key_env):
            docs_chat_api_key = docs_chat_api_key_env
            docs_chat_api_key_env = "OPENAI_API_KEY"

    backend_config = BackendConfig(
        mode="remote" if backend_mode == "remote" else "embedded",
        base_url=backend_base_url,
        process=BackendProcessConfig(
            host=process_host,
            port=process_port,
            log_level=process_log_level,
        ),
    )

    docs_chat_config = DocsChatConfig(
        enabled=docs_chat_enabled,
        provider=docs_chat_provider,
        model=docs_chat_model,
        embedding_provider=docs_chat_embedding_provider,
        embedding_model=docs_chat_embedding_model,
        api_key_env=docs_chat_api_key_env,
        api_key=docs_chat_api_key,
        docs_path=docs_chat_docs_path,
        index_path=docs_chat_index_path,
        code_paths=docs_chat_code_paths,
        reasoning_effort=docs_chat_reasoning_effort,
    )

    return ContractsAppConfig(
        workspace=WorkspaceConfig(root=workspace_root),
        backend=backend_config,
        docs_chat=docs_chat_config,
    )


def _stringify_path(path: Path | None) -> str | None:
    if path is None:
        return None
    try:
        return str(path)
    except Exception:  # pragma: no cover - defensive fallback
        return str(path)


def _workspace_mapping(config: WorkspaceConfig) -> dict[str, Any]:
    mapping: dict[str, Any] = {}
    if config.root:
        mapping["root"] = _stringify_path(config.root)
    return mapping


def _process_mapping(config: BackendProcessConfig) -> dict[str, Any]:
    mapping: dict[str, Any] = {}
    if config.host != "127.0.0.1":
        mapping["host"] = config.host
    if config.port != 8001:
        mapping["port"] = config.port
    if config.log_level:
        mapping["log_level"] = config.log_level
    return mapping


def _backend_mapping(config: BackendConfig) -> dict[str, Any]:
    mapping: dict[str, Any] = {"mode": config.mode}
    if config.base_url:
        mapping["base_url"] = config.base_url
    process_mapping = _process_mapping(config.process)
    if process_mapping:
        mapping["process"] = process_mapping
    return mapping


def _docs_chat_mapping(config: DocsChatConfig) -> dict[str, Any]:
    mapping: dict[str, Any] = {}
    if config.enabled:
        mapping["enabled"] = True
    if config.provider != "openai":
        mapping["provider"] = config.provider
    if config.model != "gpt-4o-mini":
        mapping["model"] = config.model
    if config.embedding_provider != "huggingface":
        mapping["embedding_provider"] = config.embedding_provider
    if config.embedding_model != "text-embedding-3-small":
        mapping["embedding_model"] = config.embedding_model
    if config.api_key_env != "OPENAI_API_KEY":
        mapping["api_key_env"] = config.api_key_env
    if config.api_key is not None:
        mapping["api_key"] = config.api_key
    if config.docs_path:
        mapping["docs_path"] = _stringify_path(config.docs_path)
    if config.index_path:
        mapping["index_path"] = _stringify_path(config.index_path)
    if config.code_paths:
        mapping["code_paths"] = [
            _stringify_path(path) for path in config.code_paths if path is not None
        ]
    if config.reasoning_effort:
        mapping["reasoning_effort"] = config.reasoning_effort
    return mapping


def config_to_mapping(config: ContractsAppConfig) -> dict[str, Any]:
    """Return a serialisable mapping derived from ``config``."""

    payload: dict[str, Any] = {}
    workspace_mapping = _workspace_mapping(config.workspace)
    if workspace_mapping:
        payload["workspace"] = workspace_mapping
    backend_mapping = _backend_mapping(config.backend)
    if backend_mapping:
        payload["backend"] = backend_mapping
    docs_chat_mapping = _docs_chat_mapping(config.docs_chat)
    if docs_chat_mapping:
        payload["docs_chat"] = docs_chat_mapping
    return payload


def _toml_ready_value(value: Any) -> Any:
    """Return ``value`` converted into TOML-compatible primitives."""

    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _toml_ready_value(item) for key, item in value.items()}
    if isinstance(value, set):
        return [_toml_ready_value(item) for item in sorted(value, key=repr)]
    if isinstance(value, (list, tuple)):
        return [_toml_ready_value(item) for item in value]
    return value


def dumps(config: ContractsAppConfig) -> str:
    """Return a TOML string representation of ``config``."""

    mapping = config_to_mapping(config)
    if not mapping:
        return ""
    prepared = _toml_ready_value(mapping)
    if not prepared:
        return ""
    return _toml_dumps(prepared)


def dump(path: str | os.PathLike[str], config: ContractsAppConfig) -> None:
    """Write ``config`` to ``path`` in TOML format."""

    Path(path).write_text(dumps(config), encoding="utf-8")


def mapping_to_toml(mapping: Mapping[str, Any]) -> str:
    """Return TOML for an arbitrary mapping."""

    if not mapping:
        return ""
    prepared = _toml_ready_value(mapping)
    if not prepared:
        return ""
    return _toml_dumps(prepared)
