from fastapi.testclient import TestClient

from dc43.demo_app.server import app, load_records


def test_contract_detail_page():
    rec = load_records()[0]
    client = TestClient(app)
    resp = client.get(f"/contracts/{rec.contract_id}/{rec.contract_version}")
    assert resp.status_code == 200


def test_contract_versions_page():
    rec = load_records()[0]
    client = TestClient(app)
    resp = client.get(f"/contracts/{rec.contract_id}")
    assert resp.status_code == 200


def test_dataset_detail_page():
    rec = load_records()[0]
    client = TestClient(app)
    resp = client.get(f"/datasets/{rec.dataset_name}/{rec.dataset_version}")
    assert resp.status_code == 200


def test_dataset_versions_page():
    rec = load_records()[0]
    client = TestClient(app)
    resp = client.get(f"/datasets/{rec.dataset_name}")
    assert resp.status_code == 200

