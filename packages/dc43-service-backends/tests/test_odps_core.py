import pytest

from open_data_contract_standard.model import (
    Description,
    OpenDataContractStandard as ODCSModel,
    SchemaObject,
    SchemaProperty,
)

from dc43_service_backends.core.odps import (
    DataProductInputPort,
    DataProductOutputPort,
    OpenDataProductStandard,
    as_odps_dict,
    evolve_to_draft,
    next_draft_version,
)


def test_next_draft_version_uniqueness() -> None:
    existing = ["0.2.0-draft", "0.2.0-draft.2"]
    version = next_draft_version(
        current_version="0.1.0",
        existing_versions=existing,
        bump="minor",
    )
    assert version.startswith("0.2.0-draft")
    assert version not in existing


def test_evolve_to_draft_sets_status_and_version() -> None:
    product = OpenDataProductStandard(
        id="dp.orders",
        status="active",
        version="1.0.0",
    )
    evolve_to_draft(product, existing_versions=["1.0.0"], bump="minor")
    assert product.status == "draft"
    assert product.version is not None
    assert product.version.startswith("1.1.0-draft")


def test_ensure_input_port_replaces_existing() -> None:
    product = OpenDataProductStandard(id="dp.sales", status="draft")
    port_a = DataProductInputPort(name="orders", version="1.0.0", contract_id="orders")
    product.ensure_input_port(port_a)
    port_b = DataProductInputPort(name="orders", version="1.1.0", contract_id="orders")
    changed = product.ensure_input_port(port_b)
    assert changed is True
    assert product.input_ports[0].version == "1.1.0"


def test_ensure_output_port_idempotent() -> None:
    product = OpenDataProductStandard(id="dp.sales", status="draft")
    port = DataProductOutputPort(name="report", version="2.0.0", contract_id="report")
    first = product.ensure_output_port(port)
    second = product.ensure_output_port(port)
    assert first is True
    assert second is False


def test_as_odps_dict_rejects_contract() -> None:
    contract = ODCSModel(
        version="1.0.0",
        kind="DataContract",
        apiVersion="3.0.2",
        id="test.orders",
        name="Orders",
        description=Description(usage="Orders facts"),
        schema=[
            SchemaObject(
                name="orders",
                properties=[
                    SchemaProperty(name="order_id", physicalType="bigint", required=True)
                ],
            )
        ],
    )

    with pytest.raises(TypeError, match="OpenDataContractStandard"):
        as_odps_dict(contract)
