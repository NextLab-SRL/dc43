from __future__ import annotations

import sys
from pathlib import Path

from starlette.requests import Request

ROOT = Path(__file__).resolve().parents[3]
SRC_DIRS = [
    ROOT / "packages" / "dc43-service-backends" / "src",
    ROOT / "packages" / "dc43-contracts-app" / "src",
]
for src_dir in SRC_DIRS:
    if src_dir.exists():
        str_path = str(src_dir)
        if str_path not in sys.path:
            sys.path.insert(0, str_path)

from dc43_contracts_app import server


def test_delta_databricks_values_fill_unity_config() -> None:
    state = {
        "selected_options": {
            "contracts_backend": "delta_lake",
            "products_backend": "delta_lake",
            "governance_extensions": "none",
        },
        "configuration": {
            "contracts_backend": {
                "storage_path": "s3://contracts",  # optional but ensures delta path present
                "schema": "contracts",
                "workspace_url": "https://adb-123.example.net",
                "workspace_profile": "cli-profile",
                "workspace_token": "token-contract",
            },
            "products_backend": {
                "schema": "products",
                "workspace_url": "https://adb-products.example.net",
                "workspace_profile": "products-profile",
                "workspace_token": "token-products",
            },
        },
    }

    config = server._service_backends_config_from_state(state)
    assert config is not None
    assert config.contract_store.type == "delta"
    assert config.data_product_store.type == "delta"

    unity_cfg = config.unity_catalog
    assert unity_cfg.enabled is False
    assert unity_cfg.workspace_host == "https://adb-123.example.net"
    assert unity_cfg.workspace_profile == "cli-profile"
    assert unity_cfg.workspace_token == "token-contract"


def test_unity_hook_credentials_remain_authoritative() -> None:
    state = {
        "selected_options": {
            "contracts_backend": "delta_lake",
            "products_backend": "delta_lake",
            "governance_extensions": "unity_catalog",
        },
        "configuration": {
            "contracts_backend": {
                "schema": "contracts",
                "workspace_url": "https://adb-contracts.example.net",
                "workspace_profile": "contracts-profile",
                "workspace_token": "token-contract",
            },
            "products_backend": {
                "schema": "products",
                "workspace_profile": "products-profile",
            },
            "governance_extensions": {
                "workspace_url": "https://adb-governance.example.net",
                "workspace_profile": "governance-profile",
                "token": "token-governance",
            },
        },
    }

    config = server._service_backends_config_from_state(state)
    assert config is not None
    unity_cfg = config.unity_catalog
    assert unity_cfg.enabled is True
    assert unity_cfg.workspace_host == "https://adb-governance.example.net"
    assert unity_cfg.workspace_profile == "governance-profile"
    assert unity_cfg.workspace_token == "token-governance"


def test_remote_data_quality_backend_configuration() -> None:
    state = {
        "selected_options": {
            "data_quality": "remote_http",
        },
        "configuration": {
            "data_quality": {
                "base_url": "https://quality.example.com",
                "api_token": "secret-token",
                "token_header": "X-Api-Key",
                "token_scheme": "Token",
                "default_engine": "soda",
                "extra_headers": "X-Org=governance\nX-Region=emea",
            }
        },
    }

    config = server._service_backends_config_from_state(state)
    assert config is not None

    dq_cfg = config.data_quality
    assert dq_cfg.type == "http"
    assert dq_cfg.base_url == "https://quality.example.com"
    assert dq_cfg.token == "secret-token"
    assert dq_cfg.token_header == "X-Api-Key"
    assert dq_cfg.token_scheme == "Token"
    assert dq_cfg.default_engine == "soda"
    assert dq_cfg.headers == {"X-Org": "governance", "X-Region": "emea"}


def test_governance_store_filesystem_configuration() -> None:
    state = {
        "selected_options": {
            "governance_store": "filesystem",
        },
        "configuration": {
            "governance_store": {
                "storage_path": "/var/lib/dc43/governance",
            }
        },
    }

    config = server._service_backends_config_from_state(state)
    assert config is not None

    store_cfg = config.governance_store
    assert store_cfg.type == "filesystem"
    assert str(store_cfg.root) == "/var/lib/dc43/governance"


def test_governance_store_delta_adds_databricks_credentials() -> None:
    state = {
        "selected_options": {
            "governance_store": "delta_lake",
        },
        "configuration": {
            "governance_store": {
                "workspace_url": "https://adb-governance.example.net",
                "workspace_profile": "governance-profile",
                "workspace_token": "token-governance",
            }
        },
    }

    config = server._service_backends_config_from_state(state)
    assert config is not None

    store_cfg = config.governance_store
    assert store_cfg.type == "delta"

    unity_cfg = config.unity_catalog
    assert unity_cfg.workspace_host == "https://adb-governance.example.net"
    assert unity_cfg.workspace_profile == "governance-profile"
    assert unity_cfg.workspace_token == "token-governance"


def test_governance_store_http_configuration() -> None:
    state = {
        "selected_options": {
            "governance_store": "remote_http",
        },
        "configuration": {
            "governance_store": {
                "base_url": "https://governance.example.com",
                "api_token": "secret-token",
                "token_header": "X-Api-Key",
                "token_scheme": "Token",
                "timeout": "30",
                "extra_headers": "X-Org=governance,X-Team=quality",
            }
        },
    }

    config = server._service_backends_config_from_state(state)
    assert config is not None

    store_cfg = config.governance_store
    assert store_cfg.type == "http"
    assert store_cfg.base_url == "https://governance.example.com"
    assert store_cfg.token == "secret-token"
    assert store_cfg.token_header == "X-Api-Key"
    assert store_cfg.token_scheme == "Token"
    assert store_cfg.timeout == 30.0
    assert store_cfg.headers == {"X-Org": "governance", "X-Team": "quality"}


def test_governance_store_module_sits_with_storage_foundations() -> None:
    request = Request({"type": "http", "method": "GET", "path": "/", "headers": []})
    state = server._default_setup_state()

    context = server._build_setup_context(request, state)
    groups = context.get("module_groups", [])
    storage_group = next((group for group in groups if group.get("key") == "storage_foundations"), None)

    assert storage_group is not None
    module_keys = [module.get("key") for module in storage_group.get("modules", [])]
    assert "governance_store" in module_keys
