from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from dc43_contracts_app import docs_chat_index


class DummyStatus:
    def __init__(self, *, ready: bool, message: str | None = None):
        self.enabled = True
        self.ready = ready
        self.message = message


def test_docs_chat_index_cli(monkeypatch, tmp_path, capsys):
    config_path = tmp_path / "contracts-app.toml"
    config_path.write_text(
        """
[docs_chat]
enabled = true
embedding_provider = "huggingface"
""".strip()
    )

    workspace_root = tmp_path / "workspace"

    calls: dict[str, object] = {}

    def _fake_configure(config, *, base_dir=None):  # type: ignore[unused-argument]
        calls["configure"] = (config, base_dir)

    def _fake_status_ready():
        return DummyStatus(ready=True)

    def _fake_describe():
        index_dir = workspace_root / "docs_chat" / "index"
        index_dir.mkdir(parents=True, exist_ok=True)
        return SimpleNamespace(
            docs_root=config_path.parent,
            code_paths=(),
            index_dir=index_dir,
            embedding_provider="huggingface",
            embedding_model="sentence-transformers/all-MiniLM-L6-v2",
        )

    def _fake_warm_up(*, block: bool, progress):  # type: ignore[override]
        assert block is True
        progress("step one")
        calls["warm_up"] = True
        index_dir = workspace_root / "docs_chat" / "index"
        index_dir.mkdir(parents=True, exist_ok=True)
        (index_dir / "manifest.json").write_text("{}")
        (index_dir / "index.faiss").write_bytes(b"")

    monkeypatch.setattr(docs_chat_index, "configure", _fake_configure)
    monkeypatch.setattr(docs_chat_index, "warm_up", _fake_warm_up)
    monkeypatch.setattr(docs_chat_index, "status", _fake_status_ready)
    monkeypatch.setattr(docs_chat_index, "describe_configuration", _fake_describe)

    exit_code = docs_chat_index.main([
        "--config",
        str(config_path),
        "--workspace-root",
        str(workspace_root),
    ])

    assert exit_code == 0
    assert "configure" in calls
    assert "warm_up" in calls
    _, configured_base_dir = calls["configure"]
    assert configured_base_dir == workspace_root

    captured = capsys.readouterr()
    assert "Resolved docs chat configuration:" in captured.out
    assert "Warm-up result:" in captured.out
    assert "Documentation index ready" in captured.out


def test_docs_chat_index_cli_fails_when_not_ready(monkeypatch, tmp_path, capsys):
    config_path = tmp_path / "contracts-app.toml"
    config_path.write_text("[docs_chat]\nenabled = true\n")

    def _fake_configure(config, *, base_dir=None):  # type: ignore[unused-argument]
        pass

    def _fake_status_not_ready():
        return DummyStatus(ready=False, message="missing API key")

    monkeypatch.setattr(docs_chat_index, "configure", _fake_configure)
    monkeypatch.setattr(docs_chat_index, "status", _fake_status_not_ready)
    monkeypatch.setattr(
        docs_chat_index,
        "describe_configuration",
        lambda: SimpleNamespace(
            docs_root=tmp_path,
            code_paths=(),
            index_dir=tmp_path / "index",
            embedding_provider="openai",
            embedding_model="text-embedding-3-small",
        ),
    )

    exit_code = docs_chat_index.main(["--config", str(config_path)])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "missing API key" in captured.err
