from __future__ import annotations

from pathlib import Path

from dc43_contracts_app import server
from dc43_contracts_app.config import load_config


def test_docs_chat_defaults(monkeypatch):
    monkeypatch.delenv("DC43_CONTRACTS_APP_CONFIG", raising=False)
    monkeypatch.delenv("DC43_CONTRACTS_APP_DOCS_CHAT_ENABLED", raising=False)
    monkeypatch.delenv("DC43_CONTRACTS_APP_DOCS_CHAT_PROVIDER", raising=False)
    monkeypatch.delenv("DC43_CONTRACTS_APP_DOCS_CHAT_MODEL", raising=False)
    monkeypatch.delenv("DC43_CONTRACTS_APP_DOCS_CHAT_EMBEDDING_PROVIDER", raising=False)
    monkeypatch.delenv("DC43_CONTRACTS_APP_DOCS_CHAT_EMBEDDING_MODEL", raising=False)
    monkeypatch.delenv("DC43_CONTRACTS_APP_DOCS_CHAT_API_KEY_ENV", raising=False)
    monkeypatch.delenv("DC43_CONTRACTS_APP_DOCS_CHAT_PATH", raising=False)
    monkeypatch.delenv("DC43_CONTRACTS_APP_DOCS_CHAT_INDEX", raising=False)
    monkeypatch.delenv("DC43_CONTRACTS_APP_DOCS_CHAT_CODE_PATHS", raising=False)
    monkeypatch.delenv("DC43_CONTRACTS_APP_DOCS_CHAT_REASONING_EFFORT", raising=False)

    config = load_config()
    docs_chat = config.docs_chat
    assert docs_chat.enabled is False
    assert docs_chat.provider == "openai"
    assert docs_chat.model == "gpt-4o-mini"
    assert docs_chat.embedding_provider == "huggingface"
    assert docs_chat.embedding_model == "sentence-transformers/all-MiniLM-L6-v2"
    assert docs_chat.api_key_env == "OPENAI_API_KEY"
    assert docs_chat.api_key is None
    assert docs_chat.docs_path is None
    assert docs_chat.index_path is None
    assert docs_chat.code_paths == ()
    assert docs_chat.reasoning_effort is None


def test_docs_chat_env_overrides(monkeypatch, tmp_path):
    docs_path = tmp_path / "docs"
    index_path = tmp_path / "index"

    monkeypatch.setenv("DC43_CONTRACTS_APP_DOCS_CHAT_ENABLED", "1")
    monkeypatch.setenv("DC43_CONTRACTS_APP_DOCS_CHAT_PROVIDER", "openai")
    monkeypatch.setenv("DC43_CONTRACTS_APP_DOCS_CHAT_MODEL", "gpt-4.1-mini")
    monkeypatch.setenv("DC43_CONTRACTS_APP_DOCS_CHAT_EMBEDDING_PROVIDER", "huggingface")
    monkeypatch.setenv("DC43_CONTRACTS_APP_DOCS_CHAT_EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L12-v2")
    monkeypatch.setenv("DC43_CONTRACTS_APP_DOCS_CHAT_API_KEY_ENV", "MY_CUSTOM_KEY")
    monkeypatch.setenv("DC43_CONTRACTS_APP_DOCS_CHAT_API_KEY", "super-secret")
    monkeypatch.setenv("DC43_CONTRACTS_APP_DOCS_CHAT_PATH", str(docs_path))
    monkeypatch.setenv("DC43_CONTRACTS_APP_DOCS_CHAT_INDEX", str(index_path))
    monkeypatch.setenv("DC43_CONTRACTS_APP_DOCS_CHAT_CODE_PATHS", ":".join([str(tmp_path / "src"), str(tmp_path / "pkg")] ))
    monkeypatch.setenv("DC43_CONTRACTS_APP_DOCS_CHAT_REASONING_EFFORT", "medium")

    config = load_config()
    docs_chat = config.docs_chat
    assert docs_chat.enabled is True
    assert docs_chat.provider == "openai"
    assert docs_chat.model == "gpt-4.1-mini"
    assert docs_chat.embedding_provider == "huggingface"
    assert docs_chat.embedding_model == "sentence-transformers/all-MiniLM-L12-v2"
    assert docs_chat.api_key_env == "MY_CUSTOM_KEY"
    assert docs_chat.api_key == "super-secret"
    assert docs_chat.docs_path == docs_path
    assert docs_chat.index_path == index_path
    assert docs_chat.code_paths == (tmp_path / "src", tmp_path / "pkg")
    assert docs_chat.reasoning_effort == "medium"


def test_docs_chat_coerces_inline_api_key_from_env_field(tmp_path, monkeypatch):
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[docs_chat]
enabled = true
api_key_env = "sk-example-token"
""".strip()
    )

    monkeypatch.delenv("DC43_CONTRACTS_APP_DOCS_CHAT_API_KEY_ENV", raising=False)
    monkeypatch.delenv("DC43_CONTRACTS_APP_DOCS_CHAT_API_KEY", raising=False)

    config = load_config(config_path)
    docs_chat = config.docs_chat

    assert docs_chat.enabled is True
    assert docs_chat.api_key == "sk-example-token"
    assert docs_chat.api_key_env == "OPENAI_API_KEY"


def test_docs_chat_supports_hyphenated_code_paths_key(tmp_path):
    docs_dir = tmp_path / "docs"
    docs_dir.mkdir()
    src_dir = tmp_path / "src"
    pkg_dir = tmp_path / "packages"
    src_dir.mkdir()
    pkg_dir.mkdir()

    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[docs_chat]
enabled = true
code-path = ["{src}", "{pkg}"]
""".strip().format(src=src_dir, pkg=pkg_dir)
    )

    config = load_config(config_path)
    docs_chat = config.docs_chat

    assert docs_chat.enabled is True
    assert docs_chat.code_paths == (src_dir, pkg_dir)


def test_wizard_state_enables_docs_chat():
    state = {
        "configuration": {
            "contracts_backend": {"work_dir": "/tmp/workspace"},
            "docs_assistant": {
                "provider": "openai",
                "model": "gpt-4.1-mini",
                "embedding_provider": "huggingface",
                "embedding_model": "sentence-transformers/all-MiniLM-L12-v2",
                "api_key_env": "TEAM_OPENAI_KEY",
                "api_key": "inline-secret",
                "docs_path": "~/shared-docs",
                "index_path": "~/shared-index",
                "code_paths": ["~/shared-src", "~/shared-packages"],
                "reasoning_effort": "high",
            },
        },
        "selected_options": {
            "docs_assistant": "openai_embedded",
        },
    }

    config = server._contracts_app_config_from_state(state)
    docs_chat = config.docs_chat

    assert docs_chat.enabled is True
    assert docs_chat.provider == "openai"
    assert docs_chat.model == "gpt-4.1-mini"
    assert docs_chat.embedding_provider == "huggingface"
    assert docs_chat.embedding_model == "sentence-transformers/all-MiniLM-L12-v2"
    assert docs_chat.api_key_env == "TEAM_OPENAI_KEY"
    assert docs_chat.api_key == "inline-secret"
    assert docs_chat.docs_path == Path("~/shared-docs").expanduser()
    assert docs_chat.index_path == Path("~/shared-index").expanduser()
    assert docs_chat.code_paths == (
        Path("~/shared-src").expanduser(),
        Path("~/shared-packages").expanduser(),
    )
    assert docs_chat.reasoning_effort == "high"


def test_wizard_state_defaults_to_disabled_docs_chat():
    config = server._contracts_app_config_from_state({
        "configuration": {},
        "selected_options": {},
    })

    docs_chat = config.docs_chat
    assert docs_chat.enabled is False
