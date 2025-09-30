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
    assert config.contract_store.root == tmp_path / "contracts"
    assert config.auth.token == "secret"


def test_load_config_env_overrides(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_path = tmp_path / "backends.toml"
    config_path.write_text("", encoding="utf-8")

    monkeypatch.setenv("DC43_SERVICE_BACKENDS_CONFIG", str(config_path))
    monkeypatch.setenv("DC43_CONTRACT_STORE", str(tmp_path / "override"))
    monkeypatch.setenv("DC43_BACKEND_TOKEN", "env-token")

    config = load_config()
    assert config.contract_store.root == tmp_path / "override"
    assert config.auth.token == "env-token"
