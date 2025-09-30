from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path

import pytest

pytest.importorskip("fastapi")

from fastapi import FastAPI
from fastapi.testclient import TestClient


def _reload_webapp(tmp_path: Path, token: str | None) -> FastAPI:
    os.environ["DC43_CONTRACT_STORE"] = str(tmp_path / "contracts")
    if token is None:
        os.environ.pop("DC43_BACKEND_TOKEN", None)
    else:
        os.environ["DC43_BACKEND_TOKEN"] = token

    sys.modules.pop("dc43_service_backends.webapp", None)
    module = importlib.import_module("dc43_service_backends.webapp")

    # Clean up for callers so subsequent tests can choose their own settings.
    os.environ.pop("DC43_CONTRACT_STORE", None)
    os.environ.pop("DC43_BACKEND_TOKEN", None)

    return module.app  # type: ignore[return-value]


def test_create_app_uses_environment(tmp_path):
    app = _reload_webapp(tmp_path, token=None)
    assert isinstance(app, FastAPI)

    # Requests succeed without authentication when no token is configured.
    client = TestClient(app)
    response = client.get("/contracts/foo/latest")
    assert response.status_code == 404


def test_root_redirects_to_docs(tmp_path):
    app = _reload_webapp(tmp_path, token=None)
    client = TestClient(app)

    response = client.get("/", follow_redirects=False)

    assert response.status_code in {302, 307}
    assert response.headers["location"] in {app.docs_url, app.openapi_url}


def test_authentication_dependency(tmp_path):
    app = _reload_webapp(tmp_path, token="secret-token")

    client = TestClient(app)
    unauthenticated = client.get("/contracts/foo/latest")
    assert unauthenticated.status_code in {401, 403}

    authenticated = client.get(
        "/contracts/foo/latest",
        headers={"Authorization": "Bearer secret-token"},
    )
    assert authenticated.status_code in {401, 404}

