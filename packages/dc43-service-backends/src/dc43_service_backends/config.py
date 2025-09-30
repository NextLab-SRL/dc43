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
    """Filesystem storage configuration for service backends."""

    root: Path | None = None


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

    root_value = None
    if isinstance(store_section, MutableMapping):
        root_value = _coerce_path(store_section.get("root"))

    token_value = None
    if isinstance(auth_section, MutableMapping):
        token_raw = auth_section.get("token")
        if token_raw is not None:
            token_value = str(token_raw).strip() or None

    env_root = os.getenv("DC43_CONTRACT_STORE")
    if env_root:
        root_value = _coerce_path(env_root)

    env_token = os.getenv("DC43_BACKEND_TOKEN")
    if env_token:
        token_value = env_token.strip() or None

    return ServiceBackendsConfig(
        contract_store=ContractStoreConfig(root=root_value),
        auth=AuthConfig(token=token_value),
    )
