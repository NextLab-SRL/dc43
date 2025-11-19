import pytest

from dc43_contracts_app import server
from dc43_contracts_app.server import DatasetRecord
from dc43_service_clients.odps import DataProductOutputPort, OpenDataProductStandard


@pytest.fixture(name="sample_record")
def _sample_record() -> DatasetRecord:
    return DatasetRecord(
        contract_id="contracts.orders",
        contract_version="1.0.0",
        dataset_name="analytics.orders",
        dataset_version="2024-01-01",
        status="ok",
        dq_details={},
        run_type="batch",
        violations=0,
    )


def _product_with_output(custom_properties: list[dict[str, object]]) -> OpenDataProductStandard:
    return OpenDataProductStandard(
        id="dp.analytics.orders",
        status="draft",
        version="1.2.3",
        name="Orders",
        output_ports=[
            DataProductOutputPort(
                name="primary",
                version="1.0.0",
                contract_id="contracts.orders",
                custom_properties=custom_properties,
            )
        ],
    )


def test_data_product_catalog_infers_runs_from_dataset(monkeypatch: pytest.MonkeyPatch, sample_record: DatasetRecord) -> None:
    product = _product_with_output(
        custom_properties=[{"property": "dc43.dataset.id", "value": "analytics.orders"}]
    )
    monkeypatch.setattr(server, "load_data_products", lambda: [product])

    catalog = server.data_product_catalog([sample_record])

    assert catalog[0]["id"] == product.id
    assert catalog[0]["run_count"] == 1
    assert catalog[0]["latest_run"].dataset_version == sample_record.dataset_version


def test_describe_data_product_infers_records_from_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    record = DatasetRecord(
        contract_id="contracts.orders",
        contract_version="2.0.0",
        dataset_name="",
        dataset_version="2024-01-05",
        status="ok",
        dq_details={},
        run_type="batch",
        violations=0,
    )
    product = _product_with_output(custom_properties=[])
    product.output_ports[0].version = "2.0.0"
    monkeypatch.setattr(server, "load_data_products", lambda: [product])

    details = server.describe_data_product(product.id, [record])

    assert details is not None
    assert [r.dataset_version for r in details["records"]] == [record.dataset_version]


def test_data_products_for_contract_surface_records(monkeypatch: pytest.MonkeyPatch, sample_record: DatasetRecord) -> None:
    product = _product_with_output(
        custom_properties=[{"property": "dc43.dataset.id", "value": "analytics.orders"}]
    )
    monkeypatch.setattr(server, "load_data_products", lambda: [product])

    matches = server.data_products_for_contract("contracts.orders", [sample_record])

    assert matches
    assert matches[0]["product_id"] == product.id
    assert [r.dataset_version for r in matches[0]["records"]] == [sample_record.dataset_version]


def test_data_products_for_dataset_infers_from_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    record = DatasetRecord(
        contract_id="contracts.orders",
        contract_version="1.0.0",
        dataset_name="analytics.orders",
        dataset_version="2024-01-02",
        status="ok",
        dq_details={},
        run_type="batch",
        violations=0,
    )
    product = _product_with_output(custom_properties=[])
    product.output_ports[0].contract_id = record.contract_id
    product.output_ports[0].version = record.contract_version
    monkeypatch.setattr(server, "load_data_products", lambda: [product])

    associations = server.data_products_for_dataset(record.dataset_name, [record])

    assert associations
    assert associations[0]["product_id"] == product.id
    assert associations[0]["direction"] == "output"


def test_data_products_for_dataset_keeps_recorded_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    record = DatasetRecord(
        contract_id="contracts.orders",
        contract_version="1.0.0",
        dataset_name="analytics.orders",
        dataset_version="2024-01-05",
        status="ok",
        dq_details={},
        run_type="batch",
        violations=0,
        data_product_id="dp.orders",
        data_product_port="primary",
        data_product_role="output",
    )
    monkeypatch.setattr(server, "load_data_products", lambda: [])

    associations = server.data_products_for_dataset(record.dataset_name, [record])

    assert associations
    assert associations[0]["product_id"] == record.data_product_id
