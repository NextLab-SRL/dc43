from __future__ import annotations

from pathlib import Path

import pytest
import tomllib

from dc43_service_backends.config import (
    AuthConfig,
    ContractStoreConfig,
    DataProductStoreConfig,
    DataQualityBackendConfig,
    GovernanceConfig,
    GovernanceStoreConfig,
    ServiceBackendsConfig,
    UnityCatalogConfig,
    config_to_mapping,
    dumps,
    load_config,
)


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


def test_load_config_log_sql_flags(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_path = tmp_path / "backends.toml"
    config_path.write_text(
        "\n".join(
            [
                "[contract_store]",
                "type = 'sql'",
                "dsn = 'sqlite:///:memory:'",
                "log_sql = true",
                "",
                "[data_product]",
                "type = 'sql'",
                "dsn = 'sqlite:///:memory:'",
                "log_sql = false",
                "",
                "[governance_store]",
                "type = 'sql'",
                "dsn = 'sqlite:///:memory:'",
                "log_sql = false",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    config = load_config(config_path)
    assert config.contract_store.log_sql is True
    assert config.data_product_store.log_sql is False
    assert config.governance_store.log_sql is False

    monkeypatch.setenv("DC43_SERVICE_BACKENDS_CONFIG", str(config_path))
    monkeypatch.setenv("DC43_DATA_PRODUCT_STORE_LOG_SQL", "1")
    monkeypatch.setenv("DC43_GOVERNANCE_STORE_LOG_SQL", "true")

    env_config = load_config()
    assert env_config.contract_store.log_sql is True
    assert env_config.data_product_store.log_sql is True
    assert env_config.governance_store.log_sql is True


def test_governance_metrics_table_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_path = tmp_path / "backends.toml"
    config_path.write_text(
        "\n".join(
            [
                "[governance_store]",
                "type = 'sql'",
                "metrics_table = 'dq_status_metrics'",
                "status_table = 'dq_status'",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    config = load_config(config_path)
    assert config.governance_store.metrics_table == "dq_status_metrics"

    monkeypatch.setenv("DC43_SERVICE_BACKENDS_CONFIG", str(config_path))
    monkeypatch.setenv("DC43_GOVERNANCE_METRICS_TABLE", "custom_metrics")

    env_config = load_config()
    assert env_config.governance_store.metrics_table == "custom_metrics"


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
                "workspace_url = 'https://adb.example.com'",
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
    assert config.unity_catalog.workspace_url == "https://adb.example.com"
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
    assert config.unity_catalog.workspace_url == "https://adb.example.com"
    assert config.unity_catalog.workspace_token == "env-token"
    assert config.governance.dataset_contract_link_builders == (
        "custom.module:builder",
        "other.module.hooks:make",
    )


def test_dumps_matches_mapping_including_workspace_url() -> None:
    config = ServiceBackendsConfig(
        contract_store=ContractStoreConfig(type="delta", table="governed.contracts"),
        data_quality=DataQualityBackendConfig(
            type="http",
            base_url="https://quality.example.com",
            token="dq-token",
            headers={"X-Env": "prod"},
        ),
        unity_catalog=UnityCatalogConfig(
            enabled=True,
            workspace_url="https://adb.example.com",
            workspace_profile="prod",
            workspace_token="token-123",
            dataset_prefix="table:",
            static_properties={"catalog": "main", "schema": "contracts"},
        ),
        auth=AuthConfig(token="auth-token"),
    )

    toml_text = dumps(config)
    parsed = tomllib.loads(toml_text)

    assert parsed == config_to_mapping(config)
    assert parsed["unity_catalog"]["workspace_url"] == "https://adb.example.com"


def test_dumps_handles_missing_tomlkit(monkeypatch: pytest.MonkeyPatch) -> None:
    from dc43_service_backends import config as backends_config

    service_config = ServiceBackendsConfig(
        contract_store=ContractStoreConfig(type="filesystem"),
        data_product_store=DataProductStoreConfig(type="memory"),
        auth=AuthConfig(token="shared"),
    )

    original = backends_config.tomlkit
    monkeypatch.setattr(backends_config, "tomlkit", None)
    try:
        toml_text = backends_config.dumps(service_config)
    finally:
        monkeypatch.setattr(backends_config, "tomlkit", original)

    parsed = tomllib.loads(toml_text)
    assert parsed == config_to_mapping(service_config)


def test_config_to_mapping_covers_all_sections() -> None:
    config = ServiceBackendsConfig(
        contract_store=ContractStoreConfig(
            type="collibra_http",
            root=Path("/var/contracts"),
            base_path=Path("/delta/contracts"),
            table="governance.contracts",
            base_url="https://contracts.example.com",
            dsn="postgresql+psycopg://user:pass@host/contracts",
            schema="governance",
            token="contract-token",
            timeout=22.5,
            contracts_endpoint_template="/custom/contracts/{data_product}/{port}",
            default_status="Validated",
            status_filter="Validated",
            catalog={"product-quality": ("dp-quality", "gold")},
        ),
        data_product_store=DataProductStoreConfig(
            type="collibra_http",
            root=Path("/var/products"),
            base_path=Path("/delta/products"),
            table="governance.products",
            dsn="postgresql+psycopg://user:pass@host/products",
            schema="governance",
            base_url="https://products.example.com",
            catalog="governance-products",
        ),
        data_quality=DataQualityBackendConfig(
            type="http",
            base_url="https://dq.example.com",
            token="dq-token",
            token_header="X-Token",
            token_scheme="Token",
            headers={"X-Team": "quality"},
            default_engine="soda",
            engines={
                "native": {"strict_types": False},
                "great_expectations": {"suite_path": "/tmp/suite.json"},
            },
        ),
        auth=AuthConfig(token="shared-token"),
        unity_catalog=UnityCatalogConfig(
            enabled=True,
            dataset_prefix="table:",
            workspace_profile="prod",
            workspace_url="https://adb.example.com",
            workspace_token="uc-token",
            static_properties={"catalog": "main", "schema": "governance"},
        ),
        governance=GovernanceConfig(
            dataset_contract_link_builders=("custom.module:builder",),
        ),
        governance_store=GovernanceStoreConfig(
            type="http",
            root=Path("/var/governance"),
            base_path=Path("/delta/governance"),
            table="governance.status",
            status_table="governance.status_history",
            activity_table="governance.activity",
            link_table="governance.links",
            dsn="postgresql+psycopg://user:pass@host/governance",
            schema="governance",
            base_url="https://governance.example.com",
            token="governance-token",
            token_header="X-Gov-Token",
            token_scheme="Token",
            timeout=45.5,
            headers={"X-Env": "prod"},
        ),
    )

    mapping = config_to_mapping(config)

    assert mapping["contract_store"] == {
        "type": "collibra_http",
        "root": str(Path("/var/contracts")),
        "base_path": str(Path("/delta/contracts")),
        "table": "governance.contracts",
        "base_url": "https://contracts.example.com",
        "dsn": "postgresql+psycopg://user:pass@host/contracts",
        "schema": "governance",
        "token": "contract-token",
        "timeout": 22.5,
        "contracts_endpoint_template": "/custom/contracts/{data_product}/{port}",
        "default_status": "Validated",
        "status_filter": "Validated",
        "catalog": {
            "product-quality": {
                "data_product": "dp-quality",
                "port": "gold",
            }
        },
    }
    assert mapping["data_product"] == {
        "type": "collibra_http",
        "root": str(Path("/var/products")),
        "base_path": str(Path("/delta/products")),
        "table": "governance.products",
        "dsn": "postgresql+psycopg://user:pass@host/products",
        "schema": "governance",
        "base_url": "https://products.example.com",
        "catalog": "governance-products",
    }
    assert mapping["data_quality"] == {
        "type": "http",
        "base_url": "https://dq.example.com",
        "token": "dq-token",
        "token_header": "X-Token",
        "token_scheme": "Token",
        "headers": {"X-Team": "quality"},
        "default_engine": "soda",
        "engines": {
            "native": {"strict_types": False},
            "great_expectations": {"suite_path": "/tmp/suite.json"},
        },
    }
    assert mapping["auth"] == {"token": "shared-token"}
    assert mapping["unity_catalog"] == {
        "enabled": True,
        "dataset_prefix": "table:",
        "workspace_profile": "prod",
        "workspace_url": "https://adb.example.com",
        "workspace_token": "uc-token",
        "static_properties": {"catalog": "main", "schema": "governance"},
    }
    assert mapping["governance"] == {
        "dataset_contract_link_builders": ["custom.module:builder"],
    }
    assert mapping["governance_store"] == {
        "type": "http",
        "root": str(Path("/var/governance")),
        "base_path": str(Path("/delta/governance")),
        "table": "governance.status",
        "status_table": "governance.status_history",
        "activity_table": "governance.activity",
        "link_table": "governance.links",
        "dsn": "postgresql+psycopg://user:pass@host/governance",
        "schema": "governance",
        "base_url": "https://governance.example.com",
        "token": "governance-token",
        "token_header": "X-Gov-Token",
        "token_scheme": "Token",
        "timeout": 45.5,
        "headers": {"X-Env": "prod"},
    }
