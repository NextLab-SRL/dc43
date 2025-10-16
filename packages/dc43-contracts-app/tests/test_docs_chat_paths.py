from dc43_contracts_app import docs_chat
from dc43_contracts_app.config import DocsChatConfig


def test_resolve_docs_root_prefers_existing_candidates(tmp_path, monkeypatch):
    missing = tmp_path / "missing"
    existing = tmp_path / "docs"
    existing.mkdir()

    monkeypatch.setattr(docs_chat, "_candidate_docs_roots", lambda: [missing, existing])

    config = DocsChatConfig(enabled=True)

    resolved = docs_chat._resolve_docs_root(config)  # type: ignore[attr-defined]
    assert resolved == existing
