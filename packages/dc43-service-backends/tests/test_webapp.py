from __future__ import annotations

import importlib
import json
import os
import sys
from pathlib import Path

import pytest

pytest.importorskip("fastapi")

from fastapi import FastAPI
from fastapi.testclient import TestClient

from dc43_service_backends.server import build_app
from dc43_service_clients.data_quality import ValidationResult


def _write_config(tmp_path: Path, token: str | None) -> Path:
    config_path = tmp_path / "backends.toml"
    lines = [
        "[contract_store]",
        f"root = {json.dumps(str(tmp_path / 'contracts'))}",
    ]
    if token is not None:
        lines.extend(
            [
                "",
                "[auth]",
                f"token = {json.dumps(token)}",
            ]
        )
    config_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return config_path


def _reload_webapp(tmp_path: Path, token: str | None) -> FastAPI:
    config_path = _write_config(tmp_path, token)
    os.environ["DC43_SERVICE_BACKENDS_CONFIG"] = str(config_path)
    sys.modules.pop("dc43_service_backends.webapp", None)
    module = importlib.import_module("dc43_service_backends.webapp")

    # Clean up for callers so subsequent tests can choose their own settings.
    os.environ.pop("DC43_SERVICE_BACKENDS_CONFIG", None)

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


class _StubServiceBackend:
    """Generic backend that raises for unexpected calls."""

    def __getattr__(self, name):
        def _missing(*_args, **_kwargs):  # pragma: no cover - guard for stray calls
            raise NotImplementedError(f"{name} is not implemented in _StubServiceBackend")

        return _missing


class _StubGovernanceBackend(_StubServiceBackend):
    def get_status_matrix(self, *, dataset_id, contract_ids=None, dataset_versions=None):
        assert dataset_id == "orders"
        return (
            {
                "dataset_id": dataset_id,
                "dataset_version": "2024-01-01",
                "contract_id": "sales.orders",
                "contract_version": "1.0.0",
                "status": {
                    "ok": True,
                    "errors": [],
                    "warnings": [],
                    "metrics": {},
                    "schema": {},
                    "status": "ok",
                    "reason": None,
                    "details": {},
                },
            },
            {
                "dataset_id": dataset_id,
                "dataset_version": "2024-01-02",
                "contract_id": "sales.orders",
                "contract_version": "1.1.0",
                "status": ValidationResult(status="warn"),
            },
            {
                "dataset_id": dataset_id,
                "dataset_version": "2024-01-03",
                "contract_id": "sales.orders",
                "contract_version": "1.2.0",
                "status": "unexpected",
            },
        )


def test_status_matrix_handles_mixed_status_payloads():
    app = build_app(
        contract_backend=_StubServiceBackend(),
        dq_backend=_StubServiceBackend(),
        governance_backend=_StubGovernanceBackend(),
        data_product_backend=_StubServiceBackend(),
    )

    client = TestClient(app)
    response = client.get("/governance/status-matrix", params={"dataset_id": "orders"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["dataset_id"] == "orders"
    entries = payload["entries"]
    assert len(entries) == 3
    assert entries[0]["status"]["status"] == "ok"
    assert entries[1]["status"]["status"] == "warn"
    assert entries[2]["status"] is None

