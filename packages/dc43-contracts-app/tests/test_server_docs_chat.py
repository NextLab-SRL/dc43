from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from dc43_contracts_app import server
from dc43_contracts_app.config import ContractsAppConfig


def test_configure_from_config_warms_docs_chat(monkeypatch):
    workspace = SimpleNamespace(root=Path("/tmp/dc43-workspace"))
    warm_calls: list[tuple[bool, object | None]] = []

    monkeypatch.setattr(server, "workspace_from_env", lambda default_root=None: (workspace, None))
    monkeypatch.setattr(server, "configure_workspace", lambda ws: None)
    monkeypatch.setattr(server, "configure_backend", lambda config: None)
    monkeypatch.setattr(server.docs_chat, "configure", lambda cfg, ws: None)
    monkeypatch.setattr(server, "_set_active_config", lambda cfg: cfg)

    def _warm_up(*, block: bool = False, progress=None) -> None:
        warm_calls.append((block, progress))

    monkeypatch.setattr(server.docs_chat, "warm_up", _warm_up)

    config = ContractsAppConfig()
    config.docs_chat.enabled = True

    result = server.configure_from_config(config)

    assert result is config
    assert warm_calls and warm_calls[0][0] is True
    assert callable(warm_calls[0][1])
