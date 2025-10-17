from __future__ import annotations

from pathlib import Path

from dc43_contracts_app import docs_chat_index


def test_docs_chat_index_cli(monkeypatch, tmp_path):
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

    def _fake_configure(config, workspace):  # type: ignore[unused-argument]
        calls["configure"] = (config, workspace)

    def _fake_warm_up(*, block: bool, progress):  # type: ignore[override]
        assert block is True
        progress("step one")
        calls["warm_up"] = True

    monkeypatch.setattr(docs_chat_index, "configure", _fake_configure)
    monkeypatch.setattr(docs_chat_index, "warm_up", _fake_warm_up)

    exit_code = docs_chat_index.main([
        "--config",
        str(config_path),
        "--workspace-root",
        str(workspace_root),
    ])

    assert exit_code == 0
    assert "configure" in calls
    assert "warm_up" in calls
    _, workspace_obj = calls["configure"]
    assert isinstance(workspace_obj.root, Path)
    assert workspace_obj.root == workspace_root
