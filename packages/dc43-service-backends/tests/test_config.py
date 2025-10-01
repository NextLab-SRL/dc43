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
    assert config.auth.token == "secret"


def test_load_config_env_overrides(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_path = tmp_path / "backends.toml"
    config_path.write_text("", encoding="utf-8")

    monkeypatch.setenv("DC43_SERVICE_BACKENDS_CONFIG", str(config_path))
    monkeypatch.setenv("DC43_CONTRACT_STORE", str(tmp_path / "override"))
    monkeypatch.setenv("DC43_BACKEND_TOKEN", "env-token")

    config = load_config()
    assert config.contract_store.type == "filesystem"
    assert config.contract_store.root == tmp_path / "override"
    assert config.auth.token == "env-token"


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
