import logging
import os
import textwrap
from pathlib import Path

import tomllib

from dc43_demo_app import runner


def write_config(path: Path, content: str) -> str:
    path.write_text(textwrap.dedent(content).strip() + "\n", encoding="utf-8")
    return str(path)


def _log_messages(caplog) -> str:
    return "\n".join(
        record.message
        for record in caplog.records
        if record.name == "dc43_demo_app.runner"
    )


def test_build_contracts_config_merges_docs_chat(tmp_path: Path, caplog) -> None:
    caplog.set_level(logging.INFO, logger="dc43_demo_app.runner")
    config_path = tmp_path / "contracts.toml"
    override = write_config(
        config_path,
        """
        [docs_chat]
        enabled = true
        provider = "openai"
        model = "gpt-4o"
        embedding_model = "text-embedding"
        api_key_env = "CUSTOM_KEY"
        """
    )

    config = runner._build_contracts_config(  # type: ignore[attr-defined]
        workspace_root=tmp_path / "workspace",
        backend_host="127.0.0.1",
        backend_port=9999,
        backend_url="http://127.0.0.1:9999",
        backend_log_level="info",
        override_path=override,
    )

    assert config.docs_chat.enabled is True
    assert config.docs_chat.api_key_env == "CUSTOM_KEY"
    assert str(config.workspace.root) == str((tmp_path / "workspace"))
    assert config.backend.base_url == "http://127.0.0.1:9999"
    assert config.backend.process.log_level == "info"
    log_output = _log_messages(caplog)
    assert "docs_chat=enabled provider=openai" in log_output
    assert "embeddings=huggingface" in log_output


def test_build_contracts_config_without_override(tmp_path: Path, caplog) -> None:
    caplog.set_level(logging.INFO, logger="dc43_demo_app.runner")
    config = runner._build_contracts_config(  # type: ignore[attr-defined]
        workspace_root=tmp_path / "workspace",
        backend_host="127.0.0.1",
        backend_port=9999,
        backend_url="http://127.0.0.1:9999",
        backend_log_level=None,
        override_path=None,
    )

    assert config.docs_chat.enabled is False
    assert str(config.workspace.root) == str((tmp_path / "workspace"))
    assert config.backend.process.log_level is None
    assert "docs_chat=disabled" in _log_messages(caplog)


def test_load_env_file(tmp_path: Path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        textwrap.dedent(
            """
            # sample configuration
            OPENAI_API_KEY = sk-demo
            QUOTED="value"
            EMPTY=
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("QUOTED", raising=False)
    monkeypatch.delenv("EMPTY", raising=False)

    runner._load_env_file(str(env_file))  # type: ignore[attr-defined]

    assert os.environ["OPENAI_API_KEY"] == "sk-demo"
    assert os.environ["QUOTED"] == "value"
    assert os.environ["EMPTY"] == ""


def test_write_backend_config_uses_shared_serializer(tmp_path: Path) -> None:
    config_path = tmp_path / "service_backends.toml"
    contracts_dir = tmp_path / "contracts"
    contracts_dir.mkdir()

    runner._write_backend_config(  # type: ignore[attr-defined]
        config_path,
        contracts_dir,
        token="demo-token",
    )

    data = tomllib.loads(config_path.read_text(encoding="utf-8"))
    assert data["contract_store"]["root"] == str(contracts_dir)
    assert data["auth"]["token"] == "demo-token"


def test_write_backend_config_omits_auth_when_token_missing(tmp_path: Path) -> None:
    config_path = tmp_path / "service_backends.toml"
    contracts_dir = tmp_path / "contracts"
    contracts_dir.mkdir()

    runner._write_backend_config(  # type: ignore[attr-defined]
        config_path,
        contracts_dir,
        token=None,
    )

    data = tomllib.loads(config_path.read_text(encoding="utf-8"))
    assert data["contract_store"]["root"] == str(contracts_dir)
    assert "auth" not in data
