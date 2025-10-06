from __future__ import annotations

"""Configuration helpers for the dc43 service backend HTTP application."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, MutableMapping
import os

import tomllib

__all__ = [
    "ContractStoreConfig",
    "DataProductStoreConfig",
    "AuthConfig",
    "GovernanceConfig",
    "UnityCatalogConfig",
    "ServiceBackendsConfig",
    "load_config",
]


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


@dataclass(slots=True)
class UnityCatalogConfig:
    """Optional Databricks Unity Catalog synchronisation settings."""

    enabled: bool = False
    dataset_prefix: str = "table:"
    workspace_profile: str | None = None
    workspace_host: str | None = None
    workspace_token: str | None = None
    static_properties: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class GovernanceConfig:
    """Governance service extension wiring sourced from configuration."""

    dataset_contract_link_builders: tuple[str, ...] = field(default_factory=tuple)


@dataclass(slots=True)
class ServiceBackendsConfig:
    """Top level configuration for the service backend application."""

    contract_store: ContractStoreConfig = field(default_factory=ContractStoreConfig)
    data_product_store: DataProductStoreConfig = field(default_factory=DataProductStoreConfig)
    auth: AuthConfig = field(default_factory=AuthConfig)
    unity_catalog: UnityCatalogConfig = field(default_factory=UnityCatalogConfig)
    governance: GovernanceConfig = field(default_factory=GovernanceConfig)


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

    dp_type = "memory"
    dp_root_value = None
    dp_base_path_value = None
    dp_table_value = None
    if isinstance(data_product_section, MutableMapping):
        raw_type = data_product_section.get("type")
        if isinstance(raw_type, str) and raw_type.strip():
            dp_type = raw_type.strip().lower()
        dp_root_value = _coerce_path(data_product_section.get("root"))
        dp_base_path_value = _coerce_path(data_product_section.get("base_path"))
        table_raw = data_product_section.get("table")
        if isinstance(table_raw, str) and table_raw.strip():
            dp_table_value = table_raw.strip()

    auth_token_value = None
    if isinstance(auth_section, MutableMapping):
        token_raw = auth_section.get("token")
        if token_raw is not None:
            auth_token_value = str(token_raw).strip() or None

    unity_enabled = False
    unity_prefix = "table:"
    unity_profile = None
    unity_host = None
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
        host_raw = unity_section.get("workspace_host")
        if isinstance(host_raw, str) and host_raw.strip():
            unity_host = host_raw.strip()
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

    env_dp_root = os.getenv("DC43_DATA_PRODUCT_STORE")
    if env_dp_root:
        dp_root_value = _coerce_path(env_dp_root)
        dp_base_path_value = dp_root_value if dp_base_path_value is None else dp_base_path_value

    env_dp_table = os.getenv("DC43_DATA_PRODUCT_TABLE")
    if env_dp_table:
        dp_table_value = env_dp_table.strip() or dp_table_value

    env_token = os.getenv("DC43_BACKEND_TOKEN")
    if env_token:
        auth_token_value = env_token.strip() or None

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
        unity_host = env_host.strip() or None

    env_workspace_token = os.getenv("DATABRICKS_TOKEN") or os.getenv("DC43_DATABRICKS_TOKEN")
    if env_workspace_token:
        unity_token = env_workspace_token.strip() or None

    env_link_builders = os.getenv("DC43_GOVERNANCE_LINK_BUILDERS")
    if env_link_builders:
        for chunk in env_link_builders.split(","):
            text = chunk.strip()
            if text:
                link_builder_specs.append(text)

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
        ),
        data_product_store=DataProductStoreConfig(
            type=dp_type,
            root=dp_root_value,
            base_path=dp_base_path_value,
            table=dp_table_value,
        ),
        auth=AuthConfig(token=auth_token_value),
        unity_catalog=UnityCatalogConfig(
            enabled=unity_enabled,
            dataset_prefix=unity_prefix,
            workspace_profile=unity_profile,
            workspace_host=unity_host,
            workspace_token=unity_token,
            static_properties=unity_static,
        ),
        governance=GovernanceConfig(
            dataset_contract_link_builders=tuple(ordered_builders),
        ),
    )
