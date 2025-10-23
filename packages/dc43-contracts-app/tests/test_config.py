from __future__ import annotations

from pathlib import Path

import pytest
import tomllib

from dc43_contracts_app.config import (
    BackendConfig,
    BackendProcessConfig,
    ContractsAppConfig,
    DocsChatConfig,
    WorkspaceConfig,
    config_to_mapping,
    dumps,
    load_config,
)


def test_load_config_from_file(tmp_path: Path) -> None:
    config_path = tmp_path / "contracts.toml"
    config_path.write_text(
        """
[workspace]
root = "./workspace"

[backend]
mode = "remote"
base_url = "http://localhost:9005/"

  [backend.process]
  host = "localhost"
  port = 9006
  log_level = "info"
"""
    )

    config = load_config(config_path)
    assert config.workspace.root == Path("./workspace").expanduser()
    assert config.backend.mode == "remote"
    assert config.backend.base_url == "http://localhost:9005"
    assert config.backend.process.host == "localhost"
    assert config.backend.process.port == 9006
    assert config.backend.process.log_level == "info"


def test_load_config_from_file_ignores_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_path = tmp_path / "contracts.toml"
    config_path.write_text(
        """
[workspace]
root = "./workspace"
"""
    )

    monkeypatch.setenv("DC43_CONTRACTS_APP_WORK_DIR", str(tmp_path / "other"))

    config = load_config(config_path)
    assert config.workspace.root == Path("./workspace").expanduser()


def test_load_config_env_overrides(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_path = tmp_path / "contracts.toml"
    config_path.write_text("[backend]\nmode='embedded'\n")

    monkeypatch.setenv("DC43_CONTRACTS_APP_CONFIG", str(config_path))
    monkeypatch.setenv("DC43_CONTRACTS_APP_WORK_DIR", str(tmp_path / "root"))
    monkeypatch.setenv("DC43_CONTRACTS_APP_BACKEND_MODE", "remote")
    monkeypatch.setenv("DC43_CONTRACTS_APP_BACKEND_URL", "http://svc:9000/")
    monkeypatch.setenv("DC43_CONTRACTS_APP_BACKEND_HOST", "svc")
    monkeypatch.setenv("DC43_CONTRACTS_APP_BACKEND_PORT", "9100")
    monkeypatch.setenv("DC43_CONTRACTS_APP_BACKEND_LOG", "debug")

    config = load_config()
    assert config.workspace.root == Path(tmp_path / "root")
    assert config.backend.mode == "remote"
    assert config.backend.base_url == "http://svc:9000"
    assert config.backend.process.host == "svc"
    assert config.backend.process.port == 9100
    assert config.backend.process.log_level == "debug"


def test_dumps_matches_mapping_for_docs_chat() -> None:
    config = ContractsAppConfig(
        workspace=WorkspaceConfig(root=Path("/opt/dc43/workspace")),
        backend=BackendConfig(
            mode="remote",
            base_url="https://backend.example.com",
            process=BackendProcessConfig(host="0.0.0.0", port=8100, log_level="debug"),
        ),
        docs_chat=DocsChatConfig(
            enabled=True,
            provider="openai",
            model="gpt-4o",
            embedding_provider="huggingface",
            embedding_model="sentence-transformers/all-MiniLM-L6-v2",
            api_key_env="CUSTOM_KEY",
            docs_path=Path("/docs"),
            index_path=Path("/index"),
            code_paths=(Path("/src/contracts"),),
            reasoning_effort="medium",
        ),
    )

    toml_text = dumps(config)
    parsed = tomllib.loads(toml_text)

    assert parsed == config_to_mapping(config)
    assert parsed["workspace"]["root"] == str(Path("/opt/dc43/workspace"))
    assert parsed["docs_chat"]["reasoning_effort"] == "medium"
    assert parsed["docs_chat"]["code_paths"] == ["/src/contracts"]


def test_mapping_to_toml_handles_missing_tomlkit(monkeypatch: pytest.MonkeyPatch) -> None:
    from dc43_contracts_app import config as contracts_config

    mapping = {
        "workspace": {"root": "/data/workspace"},
        "backend": {"mode": "embedded"},
        "docs_chat": {"enabled": True},
    }

    original = contracts_config.tomlkit
    monkeypatch.setattr(contracts_config, "tomlkit", None)
    try:
        toml_text = contracts_config.mapping_to_toml(mapping)
    finally:
        monkeypatch.setattr(contracts_config, "tomlkit", original)

    parsed = tomllib.loads(toml_text)
    assert parsed == mapping


def test_config_to_mapping_includes_all_fields() -> None:
    config = ContractsAppConfig(
        workspace=WorkspaceConfig(root=Path("/srv/contracts")),
        backend=BackendConfig(
            mode="remote",
            base_url="https://contracts-backend.example.com",
            process=BackendProcessConfig(host="0.0.0.0", port=8200, log_level="info"),
        ),
        docs_chat=DocsChatConfig(
            enabled=True,
            provider="anthropic",
            model="claude-3",
            embedding_provider="openai",
            embedding_model="text-embedding-3-large",
            api_key_env="DOCS_CHAT_KEY",
            api_key="inline-secret",
            docs_path=Path("/data/docs"),
            index_path=Path("/data/index"),
            code_paths=(Path("/src/contracts"), Path("/src/shared")),
            reasoning_effort="high",
        ),
    )

    mapping = config_to_mapping(config)

    assert mapping["workspace"] == {"root": str(Path("/srv/contracts"))}
    assert mapping["backend"] == {
        "mode": "remote",
        "base_url": "https://contracts-backend.example.com",
        "process": {"host": "0.0.0.0", "port": 8200, "log_level": "info"},
    }
    assert mapping["docs_chat"] == {
        "enabled": True,
        "provider": "anthropic",
        "model": "claude-3",
        "embedding_provider": "openai",
        "embedding_model": "text-embedding-3-large",
        "api_key_env": "DOCS_CHAT_KEY",
        "api_key": "inline-secret",
        "docs_path": str(Path("/data/docs")),
        "index_path": str(Path("/data/index")),
        "code_paths": [
            str(Path("/src/contracts")),
            str(Path("/src/shared")),
        ],
        "reasoning_effort": "high",
    }
