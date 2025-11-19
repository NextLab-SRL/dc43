from __future__ import annotations

"""Configuration helpers for the dc43 service backend HTTP application."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, MutableMapping, Sequence
import json
import os
import re

import tomllib

try:
    import tomlkit
except ModuleNotFoundError:  # pragma: no cover - exercised via fallback tests
    tomlkit = None

__all__ = [
    "ContractStoreConfig",
    "DataProductStoreConfig",
    "DataQualityBackendConfig",
    "AuthConfig",
    "GovernanceConfig",
    "GovernanceStoreConfig",
    "UnityCatalogConfig",
    "ServiceBackendsConfig",
    "load_config",
    "config_to_mapping",
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
    """Return the TOML dotted path for ``parts``."""

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
class ContractStoreConfig:
    """Configuration for the active contract store implementation."""

    type: str = "filesystem"
    root: Path | None = None
    base_path: Path | None = None
    table: str | None = None
    base_url: str | None = None
    dsn: str | None = None
    schema: str | None = None
    token: str | None = None
    timeout: float = 10.0
    contracts_endpoint_template: str | None = None
    default_status: str = "Draft"
    status_filter: str | None = None
    catalog: dict[str, tuple[str, str]] = field(default_factory=dict)
    log_sql: bool = False


@dataclass(slots=True)
class AuthConfig:
    """Authentication configuration for protecting backend endpoints."""

    token: str | None = None


@dataclass(slots=True)
class DataProductStoreConfig:
    """Configuration for the active data product store implementation."""

    type: str = "memory"
    root: Path | None = None
    base_path: Path | None = None
    table: str | None = None
    dsn: str | None = None
    schema: str | None = None
    base_url: str | None = None
    catalog: str | None = None
    log_sql: bool = False


@dataclass(slots=True)
class DataQualityBackendConfig:
    """Configuration for data-quality backend delegates."""

    type: str = "local"
    base_url: str | None = None
    token: str | None = None
    token_header: str = "Authorization"
    token_scheme: str = "Bearer"
    headers: dict[str, str] = field(default_factory=dict)
    default_engine: str = "native"
    engines: dict[str, dict[str, object]] = field(default_factory=dict)


@dataclass(slots=True)
class GovernanceStoreConfig:
    """Configuration for persisting governance artefacts."""

    type: str = "memory"
    root: Path | None = None
    base_path: Path | None = None
    table: str | None = None
    status_table: str | None = None
    activity_table: str | None = None
    link_table: str | None = None
    metrics_table: str | None = None
    dsn: str | None = None
    schema: str | None = None
    base_url: str | None = None
    token: str | None = None
    token_header: str = "Authorization"
    token_scheme: str = "Bearer"
    timeout: float = 10.0
    headers: dict[str, str] = field(default_factory=dict)
    log_sql: bool = False


@dataclass(slots=True)
class UnityCatalogConfig:
    """Optional Databricks Unity Catalog synchronisation settings."""

    enabled: bool = False
    dataset_prefix: str = "table:"
    workspace_profile: str | None = None
    workspace_url: str | None = None
    workspace_token: str | None = None
    static_properties: dict[str, str] = field(default_factory=dict)

    @property
    def workspace_host(self) -> str | None:
        """Backwards-compatible accessor for legacy ``workspace_host`` keys."""

        return self.workspace_url

    @workspace_host.setter
    def workspace_host(self, value: str | None) -> None:
        self.workspace_url = value


@dataclass(slots=True)
class GovernanceConfig:
    """Governance service extension wiring sourced from configuration."""

    dataset_contract_link_builders: tuple[str, ...] = field(default_factory=tuple)


@dataclass(slots=True)
class ServiceBackendsConfig:
    """Top level configuration for the service backend application."""

    contract_store: ContractStoreConfig = field(default_factory=ContractStoreConfig)
    data_product_store: DataProductStoreConfig = field(default_factory=DataProductStoreConfig)
    data_quality: DataQualityBackendConfig = field(default_factory=DataQualityBackendConfig)
    auth: AuthConfig = field(default_factory=AuthConfig)
    unity_catalog: UnityCatalogConfig = field(default_factory=UnityCatalogConfig)
    governance: GovernanceConfig = field(default_factory=GovernanceConfig)
    governance_store: GovernanceStoreConfig = field(default_factory=GovernanceStoreConfig)


def _first_existing_path(paths: list[str | os.PathLike[str] | None]) -> Path | None:
    for candidate in paths:
        if not candidate:
            continue
        resolved = Path(candidate).expanduser()
        if resolved.is_file():
            return resolved
    return None


def _load_toml(path: Path | None) -> Mapping[str, Any]:
    if not path:
        return {}
    try:
        data = path.read_bytes()
    except OSError:
        return {}
    try:
        return tomllib.loads(data.decode("utf-8"))
    except tomllib.TOMLDecodeError:
        return {}


def _coerce_path(value: Any) -> Path | None:
    if value in {None, ""}:
        return None
    return Path(str(value)).expanduser()


def _coerce_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_catalog(section: Any) -> dict[str, tuple[str, str]]:
    catalog: dict[str, tuple[str, str]] = {}
    if not isinstance(section, MutableMapping):
        return catalog
    for contract_id, mapping in section.items():
        if not isinstance(mapping, MutableMapping):
            continue
        data_product = mapping.get("data_product")
        port = mapping.get("port")
        if data_product is None or port is None:
            continue
        contract_key = str(contract_id).strip()
        if not contract_key:
            continue
        catalog[contract_key] = (str(data_product).strip(), str(port).strip())
    return catalog


def _parse_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalised = value.strip().lower()
        if normalised in {"1", "true", "yes", "on"}:
            return True
        if normalised in {"0", "false", "no", "off"}:
            return False
    return default


def _parse_str_dict(section: Any) -> dict[str, str]:
    values: dict[str, str] = {}
    if not isinstance(section, MutableMapping):
        return values
    for key, value in section.items():
        key_str = str(key).strip()
        if not key_str:
            continue
        values[key_str] = str(value)
    return values


def load_config(path: str | os.PathLike[str] | None = None) -> ServiceBackendsConfig:
    """Load configuration from ``path`` or fall back to defaults."""

    default_path = Path(__file__).with_name("config").joinpath("default.toml")
    env_path = os.getenv("DC43_SERVICE_BACKENDS_CONFIG")
    config_path = _first_existing_path([path, env_path, default_path])
    payload = _load_toml(config_path)

    store_section = (
        payload.get("contract_store")
        if isinstance(payload, MutableMapping)
        else {}
    )
    data_product_section = (
        payload.get("data_product")
        if isinstance(payload, MutableMapping)
        else {}
    )

    auth_section = (
        payload.get("auth")
        if isinstance(payload, MutableMapping)
        else {}
    )
    dq_section = (
        payload.get("data_quality")
        if isinstance(payload, MutableMapping)
        else {}
    )

    governance_store_section = (
        payload.get("governance_store")
        if isinstance(payload, MutableMapping)
        else {}
    )

    unity_section = (
        payload.get("unity_catalog")
        if isinstance(payload, MutableMapping)
        else {}
    )
    governance_section = (
        payload.get("governance")
        if isinstance(payload, MutableMapping)
        else {}
    )

    store_type = "filesystem"
    root_value = None
    base_path_value = None
    table_value = None
    base_url_value = None
    dsn_value = None
    schema_value = None
    store_token_value = None
    timeout_value = 10.0
    endpoint_template = None
    default_status = "Draft"
    status_filter = None
    catalog_value: dict[str, tuple[str, str]] = {}
    store_log_sql = False
    if isinstance(store_section, MutableMapping):
        raw_type = store_section.get("type")
        if isinstance(raw_type, str) and raw_type.strip():
            store_type = raw_type.strip().lower()
        root_value = _coerce_path(store_section.get("root"))
        base_path_value = _coerce_path(store_section.get("base_path"))
        table_raw = store_section.get("table")
        if isinstance(table_raw, str) and table_raw.strip():
            table_value = table_raw.strip()
        base_url_raw = store_section.get("base_url")
        if base_url_raw is not None:
            base_url_value = str(base_url_raw).strip() or None
        dsn_raw = store_section.get("dsn")
        if dsn_raw is not None:
            dsn_value = str(dsn_raw).strip() or None
        schema_raw = store_section.get("schema")
        if schema_raw is not None:
            schema_value = str(schema_raw).strip() or None
        token_raw = store_section.get("token")
        if token_raw is not None:
            store_token_value = str(token_raw).strip() or None
        timeout_value = _coerce_float(store_section.get("timeout"), 10.0)
        template_raw = store_section.get("contracts_endpoint_template")
        if template_raw is not None:
            endpoint_template = str(template_raw).strip() or None
        default_status = str(store_section.get("default_status", "Draft")).strip() or "Draft"
        status_raw = store_section.get("status_filter")
        if status_raw is not None:
            status_filter = str(status_raw).strip() or None
        catalog_value = _parse_catalog(store_section.get("catalog"))
        store_log_sql = _parse_bool(store_section.get("log_sql"), False)

    dp_type = "memory"
    dp_root_value = None
    dp_base_path_value = None
    dp_table_value = None
    dp_dsn_value = None
    dp_schema_value = None
    dp_base_url_value = None
    dp_catalog_value = None
    dp_log_sql = False
    if isinstance(data_product_section, MutableMapping):
        raw_type = data_product_section.get("type")
        if isinstance(raw_type, str) and raw_type.strip():
            dp_type = raw_type.strip().lower()
        dp_root_value = _coerce_path(data_product_section.get("root"))
        dp_base_path_value = _coerce_path(data_product_section.get("base_path"))
        table_raw = data_product_section.get("table")
        if isinstance(table_raw, str) and table_raw.strip():
            dp_table_value = table_raw.strip()
        dsn_raw = data_product_section.get("dsn")
        if dsn_raw is not None:
            dp_dsn_value = str(dsn_raw).strip() or None
        schema_raw = data_product_section.get("schema")
        if schema_raw is not None:
            dp_schema_value = str(schema_raw).strip() or None
        base_url_raw = data_product_section.get("base_url")
        if base_url_raw is not None:
            dp_base_url_value = str(base_url_raw).strip() or None
        catalog_raw = data_product_section.get("catalog")
        if catalog_raw is not None:
            dp_catalog_value = str(catalog_raw).strip() or None
        dp_log_sql = _parse_bool(data_product_section.get("log_sql"), False)

    dq_type = "local"
    dq_base_url_value = None
    dq_token_value = None
    dq_token_header_value = "Authorization"
    dq_token_scheme_value = "Bearer"
    dq_headers_value: dict[str, str] = {}
    dq_default_engine_value = "native"
    dq_engines_value: dict[str, dict[str, object]] = {}
    if isinstance(dq_section, MutableMapping):
        raw_type = dq_section.get("type")
        if isinstance(raw_type, str) and raw_type.strip():
            dq_type = raw_type.strip().lower()
        base_url_raw = dq_section.get("base_url")
        if base_url_raw is not None:
            dq_base_url_value = str(base_url_raw).strip() or None
        token_raw = dq_section.get("token")
        if token_raw is not None:
            dq_token_value = str(token_raw).strip() or None
        token_header_raw = dq_section.get("token_header")
        if token_header_raw is not None:
            dq_token_header_value = str(token_header_raw).strip()
        token_scheme_raw = dq_section.get("token_scheme")
        if token_scheme_raw is not None:
            dq_token_scheme_value = str(token_scheme_raw).strip()
        headers_raw = dq_section.get("headers")
        if headers_raw is not None:
            dq_headers_value = _parse_str_dict(headers_raw)
        default_engine_raw = dq_section.get("default_engine")
        if isinstance(default_engine_raw, str) and default_engine_raw.strip():
            dq_default_engine_value = default_engine_raw.strip()
        engines_raw = dq_section.get("engines")
        if isinstance(engines_raw, MutableMapping):
            engine_mapping: dict[str, dict[str, object]] = {}
            for name, engine_section in engines_raw.items():
                key = str(name).strip()
                if not key or not isinstance(engine_section, MutableMapping):
                    continue
                engine_mapping[key] = {str(k): v for k, v in engine_section.items()}
            dq_engines_value = engine_mapping

    auth_token_value = None
    if isinstance(auth_section, MutableMapping):
        token_raw = auth_section.get("token")
        if token_raw is not None:
            auth_token_value = str(token_raw).strip() or None

    unity_enabled = False
    unity_prefix = "table:"
    unity_profile = None
    unity_url = None
    unity_token = None
    unity_static: dict[str, str] = {}
    if isinstance(unity_section, MutableMapping):
        unity_enabled = _parse_bool(unity_section.get("enabled"), False)
        prefix_raw = unity_section.get("dataset_prefix")
        if isinstance(prefix_raw, str) and prefix_raw.strip():
            unity_prefix = prefix_raw.strip()
        profile_raw = unity_section.get("workspace_profile")
        if isinstance(profile_raw, str) and profile_raw.strip():
            unity_profile = profile_raw.strip()
        host_raw = unity_section.get("workspace_url")
        if host_raw is None:
            host_raw = unity_section.get("workspace_host")
        if isinstance(host_raw, str) and host_raw.strip():
            unity_url = host_raw.strip()
        token_raw = unity_section.get("workspace_token")
        if isinstance(token_raw, str) and token_raw.strip():
            unity_token = token_raw.strip()
        unity_static = _parse_str_dict(unity_section.get("static_properties"))

    link_builder_specs: list[str] = []
    if isinstance(governance_section, MutableMapping):
        raw_builders = governance_section.get("dataset_contract_link_builders")
        if isinstance(raw_builders, (list, tuple, set)):
            for entry in raw_builders:
                text = str(entry).strip()
                if text:
                    link_builder_specs.append(text)
        elif isinstance(raw_builders, str):
            for chunk in raw_builders.split(","):
                text = chunk.strip()
                if text:
                    link_builder_specs.append(text)

    gov_store_type = "memory"
    gov_root_value = None
    gov_base_path_value = None
    gov_table_value = None
    gov_status_table_value = None
    gov_activity_table_value = None
    gov_link_table_value = None
    gov_dsn_value = None
    gov_schema_value = None
    gov_base_url_value = None
    gov_token_value = None
    gov_token_header_value = "Authorization"
    gov_token_scheme_value = "Bearer"
    gov_timeout_value = 10.0
    gov_headers_value: dict[str, str] = {}
    gov_log_sql = False
    gov_metrics_table_value = None
    if isinstance(governance_store_section, MutableMapping):
        raw_type = governance_store_section.get("type")
        if isinstance(raw_type, str) and raw_type.strip():
            gov_store_type = raw_type.strip().lower()
        gov_root_value = _coerce_path(governance_store_section.get("root"))
        gov_base_path_value = _coerce_path(governance_store_section.get("base_path"))
        table_raw = governance_store_section.get("table")
        if isinstance(table_raw, str) and table_raw.strip():
            gov_table_value = table_raw.strip()
        status_table_raw = governance_store_section.get("status_table")
        if isinstance(status_table_raw, str) and status_table_raw.strip():
            gov_status_table_value = status_table_raw.strip()
        activity_table_raw = governance_store_section.get("activity_table")
        if isinstance(activity_table_raw, str) and activity_table_raw.strip():
            gov_activity_table_value = activity_table_raw.strip()
        link_table_raw = governance_store_section.get("link_table")
        if isinstance(link_table_raw, str) and link_table_raw.strip():
            gov_link_table_value = link_table_raw.strip()
        metrics_table_raw = governance_store_section.get("metrics_table")
        if isinstance(metrics_table_raw, str) and metrics_table_raw.strip():
            gov_metrics_table_value = metrics_table_raw.strip()
        dsn_raw = governance_store_section.get("dsn")
        if dsn_raw is not None:
            gov_dsn_value = str(dsn_raw).strip() or None
        schema_raw = governance_store_section.get("schema")
        if schema_raw is not None:
            gov_schema_value = str(schema_raw).strip() or None
        base_url_raw = governance_store_section.get("base_url")
        if base_url_raw is not None:
            gov_base_url_value = str(base_url_raw).strip() or None
        token_raw = governance_store_section.get("token")
        if token_raw is not None:
            gov_token_value = str(token_raw).strip() or None
        token_header_raw = governance_store_section.get("token_header")
        if token_header_raw is not None:
            gov_token_header_value = str(token_header_raw).strip() or "Authorization"
        token_scheme_raw = governance_store_section.get("token_scheme")
        if token_scheme_raw is not None:
            gov_token_scheme_value = str(token_scheme_raw).strip() or "Bearer"
        gov_timeout_value = _coerce_float(governance_store_section.get("timeout"), 10.0)
        headers_raw = governance_store_section.get("headers")
        if headers_raw is not None:
            gov_headers_value = _parse_str_dict(headers_raw)
        gov_log_sql = _parse_bool(governance_store_section.get("log_sql"), False)

    env_contract_type = os.getenv("DC43_CONTRACT_STORE_TYPE")
    if env_contract_type:
        normalised_type = env_contract_type.strip().lower()
        if normalised_type:
            store_type = normalised_type

    env_root = os.getenv("DC43_CONTRACT_STORE")
    if env_root:
        root_value = _coerce_path(env_root)
        base_path_value = root_value if base_path_value is None else base_path_value

    env_contract_table = os.getenv("DC43_CONTRACT_STORE_TABLE")
    if env_contract_table:
        table_value = env_contract_table.strip() or table_value

    env_contract_dsn = os.getenv("DC43_CONTRACT_STORE_DSN")
    if env_contract_dsn:
        dsn_value = env_contract_dsn.strip() or dsn_value

    env_contract_schema = os.getenv("DC43_CONTRACT_STORE_SCHEMA")
    if env_contract_schema:
        schema_value = env_contract_schema.strip() or schema_value

    env_contract_log_sql = os.getenv("DC43_CONTRACT_STORE_LOG_SQL")
    if env_contract_log_sql is not None:
        store_log_sql = _parse_bool(env_contract_log_sql, store_log_sql)

    env_dp_root = os.getenv("DC43_DATA_PRODUCT_STORE")
    if env_dp_root:
        dp_root_value = _coerce_path(env_dp_root)
        dp_base_path_value = dp_root_value if dp_base_path_value is None else dp_base_path_value

    env_dp_table = os.getenv("DC43_DATA_PRODUCT_TABLE")
    if env_dp_table:
        dp_table_value = env_dp_table.strip() or dp_table_value

    env_dp_log_sql = os.getenv("DC43_DATA_PRODUCT_STORE_LOG_SQL")
    if env_dp_log_sql is not None:
        dp_log_sql = _parse_bool(env_dp_log_sql, dp_log_sql)

    env_token = os.getenv("DC43_BACKEND_TOKEN")
    if env_token:
        auth_token_value = env_token.strip() or None

    env_dq_type = os.getenv("DC43_DATA_QUALITY_BACKEND_TYPE")
    if env_dq_type:
        normalised = env_dq_type.strip().lower()
        if normalised:
            dq_type = normalised

    env_dq_url = os.getenv("DC43_DATA_QUALITY_BACKEND_URL")
    if env_dq_url:
        dq_base_url_value = env_dq_url.strip() or dq_base_url_value

    env_dq_token = os.getenv("DC43_DATA_QUALITY_BACKEND_TOKEN")
    if env_dq_token:
        dq_token_value = env_dq_token.strip() or dq_token_value

    env_dq_header = os.getenv("DC43_DATA_QUALITY_BACKEND_TOKEN_HEADER")
    if env_dq_header is not None:
        dq_token_header_value = env_dq_header.strip()

    env_dq_scheme = os.getenv("DC43_DATA_QUALITY_BACKEND_TOKEN_SCHEME")
    if env_dq_scheme is not None:
        dq_token_scheme_value = env_dq_scheme.strip()

    env_dq_default_engine = os.getenv("DC43_DATA_QUALITY_DEFAULT_ENGINE")
    if env_dq_default_engine:
        dq_default_engine_value = env_dq_default_engine.strip() or dq_default_engine_value

    env_unity_enabled = os.getenv("DC43_UNITY_CATALOG_ENABLED")
    if env_unity_enabled is not None:
        unity_enabled = _parse_bool(env_unity_enabled, unity_enabled)

    env_unity_prefix = os.getenv("DC43_UNITY_CATALOG_PREFIX")
    if env_unity_prefix:
        unity_prefix = env_unity_prefix.strip() or unity_prefix

    env_profile = os.getenv("DATABRICKS_CONFIG_PROFILE")
    if env_profile:
        unity_profile = env_profile.strip() or None

    env_host = os.getenv("DATABRICKS_HOST")
    if env_host:
        unity_url = env_host.strip() or None

    env_workspace_token = os.getenv("DATABRICKS_TOKEN") or os.getenv("DC43_DATABRICKS_TOKEN")
    if env_workspace_token:
        unity_token = env_workspace_token.strip() or None

    env_link_builders = os.getenv("DC43_GOVERNANCE_LINK_BUILDERS")
    if env_link_builders:
        for chunk in env_link_builders.split(","):
            text = chunk.strip()
            if text:
                link_builder_specs.append(text)

    env_gov_store_type = os.getenv("DC43_GOVERNANCE_STORE_TYPE")
    if env_gov_store_type:
        gov_store_type = env_gov_store_type.strip().lower() or gov_store_type

    env_gov_root = os.getenv("DC43_GOVERNANCE_STORE")
    if env_gov_root:
        gov_root_value = _coerce_path(env_gov_root)
        gov_base_path_value = gov_root_value if gov_base_path_value is None else gov_base_path_value

    env_gov_base_path = os.getenv("DC43_GOVERNANCE_STORE_BASE_PATH")
    if env_gov_base_path:
        gov_base_path_value = _coerce_path(env_gov_base_path)

    env_gov_table = os.getenv("DC43_GOVERNANCE_STORE_TABLE")
    if env_gov_table:
        gov_table_value = env_gov_table.strip() or gov_table_value

    env_gov_status_table = os.getenv("DC43_GOVERNANCE_STATUS_TABLE")
    if env_gov_status_table:
        gov_status_table_value = env_gov_status_table.strip() or gov_status_table_value

    env_gov_activity_table = os.getenv("DC43_GOVERNANCE_ACTIVITY_TABLE")
    if env_gov_activity_table:
        gov_activity_table_value = env_gov_activity_table.strip() or gov_activity_table_value

    env_gov_link_table = os.getenv("DC43_GOVERNANCE_LINK_TABLE")
    if env_gov_link_table:
        gov_link_table_value = env_gov_link_table.strip() or gov_link_table_value

    env_gov_metrics_table = os.getenv("DC43_GOVERNANCE_METRICS_TABLE")
    if env_gov_metrics_table:
        gov_metrics_table_value = env_gov_metrics_table.strip() or gov_metrics_table_value

    env_gov_dsn = os.getenv("DC43_GOVERNANCE_STORE_DSN")
    if env_gov_dsn:
        gov_dsn_value = env_gov_dsn.strip() or gov_dsn_value

    env_gov_schema = os.getenv("DC43_GOVERNANCE_STORE_SCHEMA")
    if env_gov_schema:
        gov_schema_value = env_gov_schema.strip() or gov_schema_value

    env_gov_log_sql = os.getenv("DC43_GOVERNANCE_STORE_LOG_SQL")
    if env_gov_log_sql is not None:
        gov_log_sql = _parse_bool(env_gov_log_sql, gov_log_sql)

    env_gov_url = os.getenv("DC43_GOVERNANCE_STORE_URL")
    if env_gov_url:
        gov_base_url_value = env_gov_url.strip() or gov_base_url_value

    env_gov_token = os.getenv("DC43_GOVERNANCE_STORE_TOKEN")
    if env_gov_token:
        gov_token_value = env_gov_token.strip() or gov_token_value

    env_gov_token_header = os.getenv("DC43_GOVERNANCE_STORE_TOKEN_HEADER")
    if env_gov_token_header is not None:
        gov_token_header_value = env_gov_token_header.strip()

    env_gov_token_scheme = os.getenv("DC43_GOVERNANCE_STORE_TOKEN_SCHEME")
    if env_gov_token_scheme is not None:
        gov_token_scheme_value = env_gov_token_scheme.strip()

    env_gov_timeout = os.getenv("DC43_GOVERNANCE_STORE_TIMEOUT")
    if env_gov_timeout:
        gov_timeout_value = _coerce_float(env_gov_timeout, gov_timeout_value)

    # Preserve configuration order while dropping duplicates that may arrive via
    # the configuration file and environment variables.
    seen_builders: set[str] = set()
    ordered_builders: list[str] = []
    for spec in link_builder_specs:
        if spec in seen_builders:
            continue
        seen_builders.add(spec)
        ordered_builders.append(spec)

    return ServiceBackendsConfig(
        contract_store=ContractStoreConfig(
            type=store_type,
            root=root_value,
            base_path=base_path_value,
            table=table_value,
            base_url=base_url_value,
            dsn=dsn_value,
            schema=schema_value,
            token=store_token_value,
            timeout=timeout_value,
            contracts_endpoint_template=endpoint_template,
            default_status=default_status,
            status_filter=status_filter,
            catalog=catalog_value,
            log_sql=store_log_sql,
        ),
        data_product_store=DataProductStoreConfig(
            type=dp_type,
            root=dp_root_value,
            base_path=dp_base_path_value,
            table=dp_table_value,
            dsn=dp_dsn_value,
            schema=dp_schema_value,
            base_url=dp_base_url_value,
            catalog=dp_catalog_value,
            log_sql=dp_log_sql,
        ),
        data_quality=DataQualityBackendConfig(
            type=dq_type,
            base_url=dq_base_url_value,
            token=dq_token_value,
            token_header=dq_token_header_value,
            token_scheme=dq_token_scheme_value,
            headers=dq_headers_value,
            default_engine=dq_default_engine_value,
            engines=dq_engines_value,
        ),
        auth=AuthConfig(token=auth_token_value),
        unity_catalog=UnityCatalogConfig(
            enabled=unity_enabled,
            dataset_prefix=unity_prefix,
            workspace_profile=unity_profile,
            workspace_url=unity_url,
            workspace_token=unity_token,
            static_properties=unity_static,
        ),
        governance=GovernanceConfig(
            dataset_contract_link_builders=tuple(ordered_builders),
        ),
        governance_store=GovernanceStoreConfig(
            type=gov_store_type,
            root=gov_root_value,
            base_path=gov_base_path_value,
            table=gov_table_value,
            status_table=gov_status_table_value,
            activity_table=gov_activity_table_value,
            link_table=gov_link_table_value,
            metrics_table=gov_metrics_table_value,
            dsn=gov_dsn_value,
            schema=gov_schema_value,
            base_url=gov_base_url_value,
            token=gov_token_value,
            token_header=gov_token_header_value,
            token_scheme=gov_token_scheme_value,
            timeout=gov_timeout_value,
            headers=gov_headers_value,
            log_sql=gov_log_sql,
        ),
    )


def _stringify_path(path: Path | None) -> str | None:
    if path is None:
        return None
    try:
        return str(path)
    except Exception:  # pragma: no cover - defensive, should not happen
        return str(path)


def _contract_store_mapping(config: ContractStoreConfig) -> dict[str, Any]:
    mapping: dict[str, Any] = {}
    if config.type:
        mapping["type"] = config.type
    if config.root:
        mapping["root"] = _stringify_path(config.root)
    if config.base_path:
        mapping["base_path"] = _stringify_path(config.base_path)
    if config.table:
        mapping["table"] = config.table
    if config.base_url:
        mapping["base_url"] = config.base_url
    if config.dsn:
        mapping["dsn"] = config.dsn
    if config.schema:
        mapping["schema"] = config.schema
    if config.token:
        mapping["token"] = config.token
    if config.timeout != 10.0:
        mapping["timeout"] = config.timeout
    if config.contracts_endpoint_template:
        mapping["contracts_endpoint_template"] = config.contracts_endpoint_template
    if config.default_status and config.default_status != "Draft":
        mapping["default_status"] = config.default_status
    if config.status_filter:
        mapping["status_filter"] = config.status_filter
    if config.catalog:
        catalog_mapping: dict[str, Any] = {}
        for key, value in config.catalog.items():
            if not isinstance(value, tuple) or len(value) != 2:
                continue
            catalog_mapping[key] = {
                "data_product": value[0],
                "port": value[1],
            }
        if catalog_mapping:
            mapping["catalog"] = catalog_mapping
    if config.log_sql:
        mapping["log_sql"] = True
    return mapping


def _data_product_store_mapping(config: DataProductStoreConfig) -> dict[str, Any]:
    mapping: dict[str, Any] = {}
    if config.type:
        mapping["type"] = config.type
    if config.root:
        mapping["root"] = _stringify_path(config.root)
    if config.base_path:
        mapping["base_path"] = _stringify_path(config.base_path)
    if config.table:
        mapping["table"] = config.table
    if config.dsn:
        mapping["dsn"] = config.dsn
    if config.schema:
        mapping["schema"] = config.schema
    if config.base_url:
        mapping["base_url"] = config.base_url
    if config.catalog:
        mapping["catalog"] = config.catalog
    if config.log_sql:
        mapping["log_sql"] = True
    return mapping


def _data_quality_backend_mapping(config: DataQualityBackendConfig) -> dict[str, Any]:
    mapping: dict[str, Any] = {}
    if config.type:
        mapping["type"] = config.type
    if config.base_url:
        mapping["base_url"] = config.base_url
    if config.token:
        mapping["token"] = config.token
    if config.token_header and config.token_header != "Authorization":
        mapping["token_header"] = config.token_header
    if config.token_scheme and config.token_scheme != "Bearer":
        mapping["token_scheme"] = config.token_scheme
    if config.headers:
        mapping["headers"] = dict(config.headers)
    if config.default_engine and config.default_engine != "native":
        mapping["default_engine"] = config.default_engine
    if config.engines:
        mapping["engines"] = {name: dict(values) for name, values in config.engines.items()}
    return mapping


def _auth_mapping(config: AuthConfig) -> dict[str, Any]:
    if not config.token:
        return {}
    return {"token": config.token}


def _unity_catalog_mapping(config: UnityCatalogConfig) -> dict[str, Any]:
    mapping: dict[str, Any] = {}
    if config.enabled:
        mapping["enabled"] = True
    if config.dataset_prefix:
        mapping["dataset_prefix"] = config.dataset_prefix
    if config.workspace_profile:
        mapping["workspace_profile"] = config.workspace_profile
    if config.workspace_url:
        mapping["workspace_url"] = config.workspace_url
    if config.workspace_token:
        mapping["workspace_token"] = config.workspace_token
    if config.static_properties:
        mapping["static_properties"] = dict(config.static_properties)
    return mapping


def _governance_mapping(config: GovernanceConfig) -> dict[str, Any]:
    if not config.dataset_contract_link_builders:
        return {}
    return {
        "dataset_contract_link_builders": list(config.dataset_contract_link_builders)
    }


def _governance_store_mapping(config: GovernanceStoreConfig) -> dict[str, Any]:
    mapping: dict[str, Any] = {}
    if config.type:
        mapping["type"] = config.type
    if config.root:
        mapping["root"] = _stringify_path(config.root)
    if config.base_path:
        mapping["base_path"] = _stringify_path(config.base_path)
    if config.table:
        mapping["table"] = config.table
    if config.status_table:
        mapping["status_table"] = config.status_table
    if config.activity_table:
        mapping["activity_table"] = config.activity_table
    if config.link_table:
        mapping["link_table"] = config.link_table
    if config.metrics_table:
        mapping["metrics_table"] = config.metrics_table
    if config.dsn:
        mapping["dsn"] = config.dsn
    if config.schema:
        mapping["schema"] = config.schema
    if config.base_url:
        mapping["base_url"] = config.base_url
    if config.token:
        mapping["token"] = config.token
    if config.token_header and config.token_header != "Authorization":
        mapping["token_header"] = config.token_header
    if config.token_scheme and config.token_scheme != "Bearer":
        mapping["token_scheme"] = config.token_scheme
    if config.timeout != 10.0:
        mapping["timeout"] = config.timeout
    if config.headers:
        mapping["headers"] = dict(config.headers)
    if config.log_sql:
        mapping["log_sql"] = True
    return mapping


def config_to_mapping(config: ServiceBackendsConfig) -> dict[str, Any]:
    """Return a serialisable mapping derived from ``config``."""

    payload: dict[str, Any] = {}

    contract_mapping = _contract_store_mapping(config.contract_store)
    if contract_mapping:
        payload["contract_store"] = contract_mapping

    product_mapping = _data_product_store_mapping(config.data_product_store)
    if product_mapping:
        payload["data_product"] = product_mapping

    dq_mapping = _data_quality_backend_mapping(config.data_quality)
    if dq_mapping:
        payload["data_quality"] = dq_mapping

    auth_mapping = _auth_mapping(config.auth)
    if auth_mapping:
        payload["auth"] = auth_mapping

    unity_mapping = _unity_catalog_mapping(config.unity_catalog)
    if unity_mapping:
        payload["unity_catalog"] = unity_mapping

    governance_mapping = _governance_mapping(config.governance)
    if governance_mapping:
        payload["governance"] = governance_mapping

    governance_store_mapping = _governance_store_mapping(config.governance_store)
    if governance_store_mapping:
        payload["governance_store"] = governance_store_mapping

    return payload


def _toml_ready_value(value: Any) -> Any:
    """Return ``value`` converted into TOML-friendly primitives."""

    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _toml_ready_value(item) for key, item in value.items()}
    if isinstance(value, set):
        return [_toml_ready_value(item) for item in sorted(value, key=repr)]
    if isinstance(value, (list, tuple)):
        return [_toml_ready_value(item) for item in value]
    return value


def dumps(config: ServiceBackendsConfig) -> str:
    """Return a TOML string representation of ``config``."""

    mapping = config_to_mapping(config)
    if not mapping:
        return ""
    prepared = _toml_ready_value(mapping)
    if not prepared:
        return ""
    return _toml_dumps(prepared)


def dump(path: str | os.PathLike[str], config: ServiceBackendsConfig) -> None:
    """Write ``config`` to ``path`` in TOML format."""

    output = dumps(config)
    Path(path).write_text(output, encoding="utf-8")
