import os
from fastapi.testclient import TestClient

from dc43.portal_app import server as portal_server


def _reset_portal_backend(base_url: str | None) -> None:
    """Helper to restore the portal backend configuration for tests."""

    portal_server._initialise_backend(base_url=base_url)


class _PortalEnv:
    """Context manager restoring portal backend state across tests."""

    def __init__(self) -> None:
        self._previous_base_url = os.getenv("DC43_PORTAL_BACKEND_URL")
        self._original_close = portal_server._close_backend_client

    def __enter__(self) -> None:
        portal_server._close_backend_client = lambda: None  # type: ignore[assignment]
        portal_server._initialise_backend(base_url=None)

    def __exit__(self, exc_type, exc, tb) -> None:
        portal_server._close_backend_client = self._original_close
        try:
            portal_server._close_backend_client()
        finally:
            _reset_portal_backend(self._previous_base_url)


def test_index_redirects_to_contracts(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DC43_PORTAL_STORE", str(tmp_path))
    with _PortalEnv():
        app = portal_server.create_app()
        with TestClient(app) as client:
            response = client.get("/", follow_redirects=False)
    assert response.status_code == 307
    assert response.headers["location"] == "/contracts"


def test_contracts_page_renders_without_records(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DC43_PORTAL_STORE", str(tmp_path))
    with _PortalEnv():
        app = portal_server.create_app()
        with TestClient(app) as client:
            response = client.get("/contracts")
    assert response.status_code == 200
    assert "No contracts stored." in response.text


def test_new_contract_form_renders(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DC43_PORTAL_STORE", str(tmp_path))
    with _PortalEnv():
        app = portal_server.create_app()
        with TestClient(app) as client:
            response = client.get("/contracts/new")
    assert response.status_code == 200
    assert "New Contract" in response.text


def test_datasets_page_renders_without_activity(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DC43_PORTAL_STORE", str(tmp_path))
    with _PortalEnv():
        app = portal_server.create_app()
        with TestClient(app) as client:
            response = client.get("/datasets")
    assert response.status_code == 200
    assert "No dataset activity has been recorded." in response.text
