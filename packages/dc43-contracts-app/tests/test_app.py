import pytest
from fastapi.testclient import TestClient

from dc43_contracts_app import server


@pytest.fixture()
def client() -> TestClient:
    return TestClient(server.app)


def test_contracts_index(client: TestClient) -> None:
    resp = client.get("/contracts")
    assert resp.status_code == 200
    assert "Contracts" in resp.text


def test_datasets_index(client: TestClient) -> None:
    resp = client.get("/datasets")
    assert resp.status_code == 200
    assert "datasets" in resp.text.lower()
