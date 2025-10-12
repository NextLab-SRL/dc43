from __future__ import annotations

from pathlib import Path

import pytest

from dc43_service_backends.config import load_config


def test_load_config_from_file(tmp_path: Path) -> None:
    config_path = tmp_path / "backends.toml"
    config_path.write_text(
        "\n".join(
            [
                "[contract_store]",
                f"root = '{tmp_path / 'contracts'}'",
                "", 
                "[data_product]",
                f"root = '{tmp_path / 'products'}'",
                "",
                "[data_quality]",
                "type = 'local'",
                "",
                "[auth]",
                "token = 'secret'",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    config = load_config(config_path)
    assert config.contract_store.type == "filesystem"
    assert config.contract_store.root == tmp_path / "contracts"
    assert config.data_product_store.type == "memory"
    assert config.data_product_store.root == tmp_path / "products"
    assert config.data_quality.type == "local"
    assert config.data_quality.base_url is None
    assert config.data_quality.default_engine == "native"
    assert config.auth.token == "secret"
    assert config.governance.dataset_contract_link_builders == ()
    assert config.governance_store.type == "memory"


def test_load_config_env_overrides(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_path = tmp_path / "backends.toml"
    config_path.write_text("", encoding="utf-8")

    monkeypatch.setenv("DC43_SERVICE_BACKENDS_CONFIG", str(config_path))
    monkeypatch.setenv("DC43_CONTRACT_STORE", str(tmp_path / "override"))
    monkeypatch.setenv("DC43_BACKEND_TOKEN", "env-token")
    monkeypatch.setenv("DC43_DATA_PRODUCT_STORE", str(tmp_path / "dp"))
    monkeypatch.setenv("DC43_DATA_QUALITY_BACKEND_TYPE", "http")
    monkeypatch.setenv("DC43_DATA_QUALITY_BACKEND_URL", "https://quality.local")
    monkeypatch.setenv("DC43_DATA_QUALITY_BACKEND_TOKEN", "dq-token")
    monkeypatch.setenv("DC43_DATA_QUALITY_BACKEND_TOKEN_HEADER", "X-Api")
    monkeypatch.setenv("DC43_DATA_QUALITY_BACKEND_TOKEN_SCHEME", "")
    monkeypatch.setenv("DC43_GOVERNANCE_STORE_TYPE", "filesystem")
    monkeypatch.setenv("DC43_GOVERNANCE_STORE", str(tmp_path / "gov"))
    monkeypatch.setenv("DC43_DATA_QUALITY_DEFAULT_ENGINE", "soda")

    config = load_config()
    assert config.contract_store.type == "filesystem"
    assert config.contract_store.root == tmp_path / "override"
    assert config.data_product_store.root == tmp_path / "dp"
    assert config.data_quality.type == "http"
    assert config.data_quality.base_url == "https://quality.local"
    assert config.data_quality.token == "dq-token"
    assert config.data_quality.token_header == "X-Api"
    assert config.data_quality.token_scheme == ""
    assert config.data_quality.default_engine == "soda"
    assert config.auth.token == "env-token"
    assert config.unity_catalog.enabled is False
    assert config.governance.dataset_contract_link_builders == ()
    assert config.governance_store.type == "filesystem"
    assert config.governance_store.root == tmp_path / "gov"


def test_env_overrides_contract_store_type(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_path = tmp_path / "backends.toml"
    config_path.write_text("", encoding="utf-8")

    monkeypatch.setenv("DC43_SERVICE_BACKENDS_CONFIG", str(config_path))
    monkeypatch.setenv("DC43_CONTRACT_STORE_TYPE", "SQL")
    monkeypatch.setenv("DC43_CONTRACT_STORE_DSN", "sqlite:///example.db")

    config = load_config()
    assert config.contract_store.type == "sql"
    assert config.contract_store.dsn == "sqlite:///example.db"


def test_load_collibra_stub_config(tmp_path: Path) -> None:
    config_path = tmp_path / "backends.toml"
    config_path.write_text(
        "\n".join(
            [
                "[contract_store]",
                "type = 'collibra_stub'",
                "base_path = './stub-cache'",
                "default_status = 'Validated'",
                "status_filter = 'Validated'",
                "",
                "[contract_store.catalog.contract_a]",
                "data_product = 'dp-a'",
                "port = 'port-a'",
                "",
                "[contract_store.catalog.'contract-b']",
                "data_product = 'dp-b'",
                "port = 'port-b'",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    config = load_config(config_path)
    assert config.contract_store.type == "collibra_stub"
    assert config.contract_store.base_path == Path("./stub-cache").expanduser()
    assert config.contract_store.default_status == "Validated"
    assert config.contract_store.status_filter == "Validated"
    assert config.contract_store.catalog == {
        "contract_a": ("dp-a", "port-a"),
        "contract-b": ("dp-b", "port-b"),
    }


def test_delta_store_config(tmp_path: Path) -> None:
    config_path = tmp_path / "backends.toml"
    config_path.write_text(
        "\n".join(
            [
                "[contract_store]",
                "type = 'delta'",
                "table = 'governed.meta.contracts'",
                "",
                "[data_product]",
                "type = 'delta'",
                "table = 'governed.meta.data_products'",
                "",
                "[data_quality]",
                "type = 'http'",
                "base_url = 'https://observability.example.com'",
                "token = 'api-token'",
                "token_header = 'X-Token'",
                "token_scheme = ''",
                "",
                "[data_quality.headers]",
                "X-Org = 'governed'",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    config = load_config(config_path)
    assert config.contract_store.type == "delta"
    assert config.contract_store.table == "governed.meta.contracts"
    assert config.data_product_store.type == "delta"
    assert config.data_product_store.table == "governed.meta.data_products"
    assert config.data_quality.type == "http"
    assert config.data_quality.base_url == "https://observability.example.com"
    assert config.data_quality.token == "api-token"
    assert config.data_quality.token_header == "X-Token"
    assert config.data_quality.token_scheme == ""
    assert config.data_quality.headers == {"X-Org": "governed"}


def test_data_quality_engines_config(tmp_path: Path) -> None:
    suite_path = tmp_path / "suite.json"
    suite_path.write_text("{}", encoding="utf-8")
    config_path = tmp_path / "backends.toml"
    config_path.write_text(
        "\n".join(
            [
                "[data_quality]",
                "default_engine = 'great_expectations'",
                "",
                "[data_quality.engines.native]",
                "type = 'native'",
                "strict_types = false",
                "",
                "[data_quality.engines.great_expectations]",
                f"suite_path = '{suite_path}'",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    config = load_config(config_path)
    assert config.data_quality.default_engine == "great_expectations"
    assert "native" in config.data_quality.engines
    native_cfg = config.data_quality.engines["native"]
    assert native_cfg["type"] == "native"
    assert native_cfg["strict_types"] is False
    ge_cfg = config.data_quality.engines["great_expectations"]
    assert ge_cfg["suite_path"] == str(suite_path)


def test_load_collibra_http_config(tmp_path: Path) -> None:
    config_path = tmp_path / "backends.toml"
    config_path.write_text(
        "\n".join(
            [
                "[contract_store]",
                "type = 'collibra_http'",
                "base_url = 'https://collibra.example.com'",
                "token = 'api-token'",
                "timeout = 5.5",
                "contracts_endpoint_template = '/custom/{data_product}/{port}'",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    config = load_config(config_path)
    assert config.contract_store.type == "collibra_http"
    assert config.contract_store.base_url == "https://collibra.example.com"
    assert config.contract_store.token == "api-token"
    assert config.contract_store.timeout == 5.5
    assert config.contract_store.contracts_endpoint_template == "/custom/{data_product}/{port}"


def test_unity_catalog_config_section(tmp_path: Path) -> None:
    config_path = tmp_path / "backends.toml"
    config_path.write_text(
        "\n".join(
            [
                "[unity_catalog]",
                "enabled = true",
                "dataset_prefix = 'table:'",
                "workspace_profile = 'prod'",
                "workspace_host = 'https://adb.example.com'",
                "workspace_token = 'token-123'",
                "",
                "[unity_catalog.static_properties]",
                "owner = 'governance'",
                "",
                "[governance]",
                "dataset_contract_link_builders = [",
                "  'example.module:builder',",
                "]",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    config = load_config(config_path)
    assert config.unity_catalog.enabled is True
    assert config.unity_catalog.dataset_prefix == "table:"
    assert config.unity_catalog.workspace_profile == "prod"
    assert config.unity_catalog.workspace_host == "https://adb.example.com"
    assert config.unity_catalog.workspace_token == "token-123"
    assert config.unity_catalog.static_properties == {"owner": "governance"}
    assert config.governance.dataset_contract_link_builders == (
        "example.module:builder",
    )


def test_unity_catalog_env_overrides(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_path = tmp_path / "backends.toml"
    config_path.write_text("", encoding="utf-8")

    monkeypatch.setenv("DC43_SERVICE_BACKENDS_CONFIG", str(config_path))
    monkeypatch.setenv("DC43_UNITY_CATALOG_ENABLED", "yes")
    monkeypatch.setenv("DC43_UNITY_CATALOG_PREFIX", "cat:")
    monkeypatch.setenv("DATABRICKS_CONFIG_PROFILE", "unity-prod")
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb.example.com")
    monkeypatch.setenv("DATABRICKS_TOKEN", "env-token")
    monkeypatch.setenv(
        "DC43_GOVERNANCE_LINK_BUILDERS",
        "custom.module:builder, other.module.hooks:make",
    )

    config = load_config()
    assert config.unity_catalog.enabled is True
    assert config.unity_catalog.dataset_prefix == "cat:"
    assert config.unity_catalog.workspace_profile == "unity-prod"
    assert config.unity_catalog.workspace_host == "https://adb.example.com"
    assert config.unity_catalog.workspace_token == "env-token"
    assert config.governance.dataset_contract_link_builders == (
        "custom.module:builder",
        "other.module.hooks:make",
    )
