from __future__ import annotations

from pathlib import Path

from dc43_contracts_app import server
from dc43_contracts_app.config import ContractsAppConfig


def test_configure_from_config_warms_docs_chat(monkeypatch):
    warm_calls: list[tuple[bool, object | None]] = []
    configured_dirs: list[Path | None] = []

    monkeypatch.setattr(server, "configure_backend", lambda config: None)

    def _capture_configure(cfg, *, base_dir=None):
        configured_dirs.append(base_dir)

    monkeypatch.setattr(server.docs_chat, "configure", _capture_configure)
    monkeypatch.setattr(server, "_set_active_config", lambda cfg: cfg)

    def _warm_up(*, block: bool = False, progress=None) -> None:
        warm_calls.append((block, progress))

    monkeypatch.setattr(server.docs_chat, "warm_up", _warm_up)

    config = ContractsAppConfig()
    config.workspace.root = Path("/tmp/dc43-workspace")
    config.docs_chat.enabled = True

    result = server.configure_from_config(config)

    assert result is config
    assert configured_dirs == [config.workspace.root]
    assert warm_calls and warm_calls[0][0] is False
    assert callable(warm_calls[0][1])
