from fastapi.testclient import TestClient

from dc43.demo_app.server import (
    app,
    load_records,
    save_records,
    DatasetRecord,
    queue_flash,
)


def test_contracts_page():
    client = TestClient(app)
    resp = client.get("/contracts")
    assert resp.status_code == 200


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


def test_customers_contract_versions_page():
    client = TestClient(app)
    resp = client.get("/contracts/customers")
    assert resp.status_code == 200


def test_dataset_detail_page():
    rec = load_records()[0]
    client = TestClient(app)
    resp = client.get(f"/datasets/{rec.dataset_name}/{rec.dataset_version}")
    assert resp.status_code == 200
    assert "order_id" in resp.text


def test_dataset_versions_page():
    rec = load_records()[0]
    client = TestClient(app)
    resp = client.get(f"/datasets/{rec.dataset_name}")
    assert resp.status_code == 200


def test_dataset_pages_without_contract():
    original = load_records()
    record = DatasetRecord(
        contract_id="",
        contract_version="",
        dataset_name="missing-contract-dataset",
        dataset_version="1.0.0",
        status="error",
        dq_details={},
        run_type="enforce",
        violations=0,
    )
    save_records([*original, record])
    client = TestClient(app)
    try:
        resp = client.get("/datasets")
        assert resp.status_code == 200
        assert "No contract" in resp.text

        resp_versions = client.get("/datasets/missing-contract-dataset")
        assert resp_versions.status_code == 200
        assert "No contract" in resp_versions.text

        resp_detail = client.get("/datasets/missing-contract-dataset/1.0.0")
        assert resp_detail.status_code == 200
        assert "No contract recorded for this run" in resp_detail.text
    finally:
        save_records(original)


def test_flash_message_consumed_once():
    token = queue_flash(message="Hello there", error=None)
    client = TestClient(app)

    first = client.get(f"/datasets?flash={token}")
    assert first.status_code == 200
    assert "Hello there" in first.text

    second = client.get(f"/datasets?flash={token}")
    assert second.status_code == 200
    assert "Hello there" not in second.text

