from __future__ import annotations

import sys
from pathlib import Path

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
