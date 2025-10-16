import textwrap
from pathlib import Path

from dc43_demo_app import runner


def write_config(path: Path, content: str) -> str:
    path.write_text(textwrap.dedent(content).strip() + "\n", encoding="utf-8")
    return str(path)


def test_build_contracts_config_merges_docs_chat(tmp_path: Path) -> None:
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


def test_build_contracts_config_without_override(tmp_path: Path) -> None:
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
