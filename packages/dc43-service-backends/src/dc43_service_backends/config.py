from __future__ import annotations

"""Configuration helpers for the dc43 service backend HTTP application."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, MutableMapping
import os

import tomllib

__all__ = [
    "ContractStoreConfig",
    "AuthConfig",
    "ServiceBackendsConfig",
    "load_config",
]


@dataclass(slots=True)
class ContractStoreConfig:
    """Configuration for the active contract store implementation."""

    type: str = "filesystem"
    root: Path | None = None
    base_path: Path | None = None
    base_url: str | None = None
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
class ServiceBackendsConfig:
    """Top level configuration for the service backend application."""

    contract_store: ContractStoreConfig = field(default_factory=ContractStoreConfig)
    auth: AuthConfig = field(default_factory=AuthConfig)


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
    auth_section = (
        payload.get("auth")
        if isinstance(payload, MutableMapping)
        else {}
    )

    store_type = "filesystem"
    root_value = None
    base_path_value = None
    base_url_value = None
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
        base_url_raw = store_section.get("base_url")
        if base_url_raw is not None:
            base_url_value = str(base_url_raw).strip() or None
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

    auth_token_value = None
    if isinstance(auth_section, MutableMapping):
        token_raw = auth_section.get("token")
        if token_raw is not None:
            auth_token_value = str(token_raw).strip() or None

    env_root = os.getenv("DC43_CONTRACT_STORE")
    if env_root:
        root_value = _coerce_path(env_root)
        base_path_value = root_value if base_path_value is None else base_path_value

    env_token = os.getenv("DC43_BACKEND_TOKEN")
    if env_token:
        auth_token_value = env_token.strip() or None

    return ServiceBackendsConfig(
        contract_store=ContractStoreConfig(
            type=store_type,
            root=root_value,
            base_path=base_path_value,
            base_url=base_url_value,
            token=store_token_value,
            timeout=timeout_value,
            contracts_endpoint_template=endpoint_template,
            default_status=default_status,
            status_filter=status_filter,
            catalog=catalog_value,
        ),
        auth=AuthConfig(token=auth_token_value),
    )
