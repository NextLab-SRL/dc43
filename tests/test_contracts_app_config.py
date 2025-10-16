from __future__ import annotations

from dc43_contracts_app.config import load_config


def test_docs_chat_defaults(monkeypatch):
    monkeypatch.delenv("DC43_CONTRACTS_APP_CONFIG", raising=False)
    monkeypatch.delenv("DC43_CONTRACTS_APP_DOCS_CHAT_ENABLED", raising=False)
    monkeypatch.delenv("DC43_CONTRACTS_APP_DOCS_CHAT_PROVIDER", raising=False)
    monkeypatch.delenv("DC43_CONTRACTS_APP_DOCS_CHAT_MODEL", raising=False)
    monkeypatch.delenv("DC43_CONTRACTS_APP_DOCS_CHAT_EMBEDDING_MODEL", raising=False)
    monkeypatch.delenv("DC43_CONTRACTS_APP_DOCS_CHAT_API_KEY_ENV", raising=False)
    monkeypatch.delenv("DC43_CONTRACTS_APP_DOCS_CHAT_PATH", raising=False)
    monkeypatch.delenv("DC43_CONTRACTS_APP_DOCS_CHAT_INDEX", raising=False)

    config = load_config()
    docs_chat = config.docs_chat
    assert docs_chat.enabled is False
    assert docs_chat.provider == "openai"
    assert docs_chat.model == "gpt-4o-mini"
    assert docs_chat.embedding_model == "text-embedding-3-small"
    assert docs_chat.api_key_env == "OPENAI_API_KEY"
    assert docs_chat.docs_path is None
    assert docs_chat.index_path is None


def test_docs_chat_env_overrides(monkeypatch, tmp_path):
    docs_path = tmp_path / "docs"
    index_path = tmp_path / "index"

    monkeypatch.setenv("DC43_CONTRACTS_APP_DOCS_CHAT_ENABLED", "1")
    monkeypatch.setenv("DC43_CONTRACTS_APP_DOCS_CHAT_PROVIDER", "openai")
    monkeypatch.setenv("DC43_CONTRACTS_APP_DOCS_CHAT_MODEL", "gpt-4.1-mini")
    monkeypatch.setenv("DC43_CONTRACTS_APP_DOCS_CHAT_EMBEDDING_MODEL", "text-embedding-3-large")
    monkeypatch.setenv("DC43_CONTRACTS_APP_DOCS_CHAT_API_KEY_ENV", "MY_CUSTOM_KEY")
    monkeypatch.setenv("DC43_CONTRACTS_APP_DOCS_CHAT_PATH", str(docs_path))
    monkeypatch.setenv("DC43_CONTRACTS_APP_DOCS_CHAT_INDEX", str(index_path))

    config = load_config()
    docs_chat = config.docs_chat
    assert docs_chat.enabled is True
    assert docs_chat.provider == "openai"
    assert docs_chat.model == "gpt-4.1-mini"
    assert docs_chat.embedding_model == "text-embedding-3-large"
    assert docs_chat.api_key_env == "MY_CUSTOM_KEY"
    assert docs_chat.docs_path == docs_path
    assert docs_chat.index_path == index_path
