from pathlib import Path

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


def test_resolve_content_sources_includes_code_paths(tmp_path, monkeypatch):
    docs_dir = tmp_path / "docs"
    docs_dir.mkdir()
    code_dir = tmp_path / "src"
    code_dir.mkdir()

    monkeypatch.setattr(docs_chat, "_candidate_docs_roots", lambda: [docs_dir])
    monkeypatch.setattr(docs_chat, "_candidate_code_paths", lambda: [code_dir])

    config = DocsChatConfig(enabled=True)
    sources = docs_chat._resolve_content_sources(config)  # type: ignore[attr-defined]

    assert sources[0].root == docs_dir
    assert any(source.root == code_dir and source.kind == "code" for source in sources)


def test_candidate_paths_stay_within_repository():
    repo_root = Path(__file__).resolve().parents[4]

    code_candidates = docs_chat._candidate_code_paths()  # type: ignore[attr-defined]
    doc_candidates = docs_chat._candidate_docs_roots()  # type: ignore[attr-defined]

    for path in (*code_candidates, *doc_candidates):
        assert path.resolve().is_relative_to(repo_root)
