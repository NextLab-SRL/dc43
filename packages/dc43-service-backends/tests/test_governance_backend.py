from __future__ import annotations

from typing import Mapping

import pytest
from open_data_contract_standard.model import (  # type: ignore
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
    Server,
)

from dc43_service_backends.contracts.backend.local import LocalContractServiceBackend
from dc43_service_backends.contracts.backend.stores.filesystem import FSContractStore
from dc43_service_backends.data_quality import LocalDataQualityServiceBackend
from dc43_service_backends.governance.backend.local import LocalGovernanceServiceBackend
from dc43_service_clients.data_quality import ObservationPayload, ValidationResult
from dc43_service_clients.data_products.models import (
    DataProductInputBinding,
    DataProductOutputBinding,
)
from dc43_service_clients.governance.models import (
    ContractReference,
    GovernanceReadContext,
    GovernanceWriteContext,
)
from dc43_service_clients.odps import DataProductInputPort, DataProductOutputPort
from dc43_service_clients.testing.backends import LocalDataProductServiceBackend


class RecordingDataProductBackend(LocalDataProductServiceBackend):
    """Capture registration calls for assertions."""

    def __init__(self) -> None:
        super().__init__()
        self.last_input_call: Mapping[str, object] | None = None
        self.last_output_call: Mapping[str, object] | None = None

    def register_input_port(self, **kwargs):  # type: ignore[override]
        self.last_input_call = dict(kwargs)
        return super().register_input_port(**kwargs)

    def register_output_port(self, **kwargs):  # type: ignore[override]
        self.last_output_call = dict(kwargs)
        return super().register_output_port(**kwargs)


def _sample_contract(version: str = "1.0.0") -> OpenDataContractStandard:
    return OpenDataContractStandard(
        version=version,
        kind="DatasetContract",
        apiVersion="3.0.2",
        id="sales.orders",
        name="Sales Orders",
        schema=[
            SchemaObject(
                name="orders",
                properties=[
                    SchemaProperty(
                        name="order_id",
                        physicalType="integer",
                        required=True,
                    ),
                    SchemaProperty(
                        name="order_ts",
                        physicalType="string",
                    ),
                ],
            )
        ],
        servers=[
            Server(server="s3", type="s3", path="datalake/orders", format="delta")
        ],
    )


@pytest.fixture()
def governance_fixture(tmp_path):
    contract = _sample_contract()
    store = FSContractStore(str(tmp_path / "contracts"))
    contract_backend = LocalContractServiceBackend(store)
    contract_backend.put(contract)

    dq_backend = LocalDataQualityServiceBackend()
    data_product_backend = RecordingDataProductBackend()
    data_product_backend.register_input_port(
        data_product_id="dp.analytics",
        port=DataProductInputPort(
            name="orders", version=contract.version, contract_id=contract.id
        ),
    )
    data_product_backend.register_output_port(
        data_product_id="dp.analytics",
        port=DataProductOutputPort(
            name="primary", version=contract.version, contract_id=contract.id
        ),
    )

    product = data_product_backend.latest("dp.analytics")
    if product is not None:
        product.status = "active"
        if product.version and product.version.endswith("-draft"):
            product.version = product.version[: -len("-draft")]
        data_product_backend.put(product)

    backend = LocalGovernanceServiceBackend(
        contract_client=contract_backend,
        dq_client=dq_backend,
        data_product_client=data_product_backend,
        draft_store=store,
    )
    return backend, data_product_backend, contract


def test_resolve_read_context_uses_data_product_binding(governance_fixture):
    backend, data_product_backend, contract = governance_fixture

    context = GovernanceReadContext(
        input_binding=DataProductInputBinding(
            data_product="dp.analytics",
            port_name="orders",
        ),
        dataset_id="analytics.orders",
        draft_on_violation=True,
    )

    plan = backend.resolve_read_context(context=context)
    assert plan.contract_id == contract.id
    assert plan.dataset_id == "analytics.orders"
    assert plan.input_binding is not None

    assessment = backend.evaluate_read_plan(
        plan=plan,
        validation=ValidationResult(ok=True, status="ok"),
        observations=lambda: ObservationPayload(metrics={}, schema=None),
    )
    backend.register_read_activity(plan=plan, assessment=assessment)

    assert data_product_backend.last_input_call is not None
    assert data_product_backend.last_input_call["data_product_id"] == "dp.analytics"


def test_resolve_write_context_prefers_contract_reference(governance_fixture):
    backend, data_product_backend, contract = governance_fixture

    context = GovernanceWriteContext(
        contract=ContractReference(
            contract_id=contract.id,
            contract_version=contract.version,
        ),
        output_binding=DataProductOutputBinding(
            data_product="dp.analytics",
            port_name="derived",
        ),
        dataset_id="analytics.orders.out",
    )

    plan = backend.resolve_write_context(context=context)
    assert plan.contract_version == contract.version
    assessment = backend.evaluate_write_plan(
        plan=plan,
        validation=ValidationResult(ok=True, status="ok"),
        observations=lambda: ObservationPayload(metrics={}, schema=None),
    )
    with pytest.raises(RuntimeError) as excinfo:
        backend.register_write_activity(plan=plan, assessment=assessment)

    assert data_product_backend.last_output_call is not None
    assert data_product_backend.last_output_call["data_product_id"] == "dp.analytics"
    assert data_product_backend.last_output_call["port"].name == "derived"
    assert "requires review" in str(excinfo.value)


def test_resolve_write_context_from_existing_output(governance_fixture):
    backend, _, contract = governance_fixture

    context = GovernanceWriteContext(
        output_binding=DataProductOutputBinding(
            data_product="dp.analytics",
            port_name="primary",
        )
    )

    plan = backend.resolve_write_context(context=context)
    assert plan.contract_id == contract.id
    assert plan.output_binding is not None

    assessment = backend.evaluate_write_plan(
        plan=plan,
        validation=ValidationResult(ok=True, status="ok"),
        observations=lambda: ObservationPayload(metrics={}, schema=None),
    )
    backend.register_write_activity(plan=plan, assessment=assessment)


def test_resolve_read_context_rejects_draft_product(governance_fixture):
    backend, data_product_backend, _ = governance_fixture

    product = data_product_backend.latest("dp.analytics")
    assert product is not None
    product.status = "draft"
    data_product_backend.put(product)

    context = GovernanceReadContext(
        input_binding=DataProductInputBinding(
            data_product="dp.analytics",
            port_name="orders",
        )
    )

    with pytest.raises(ValueError, match="status"):
        backend.resolve_read_context(context=context)


def test_resolve_read_context_enforces_source_contract_version(governance_fixture):
    backend, _, _ = governance_fixture

    context = GovernanceReadContext(
        input_binding=DataProductInputBinding(
            data_product="dp.analytics",
            port_name="orders",
            source_data_product="dp.analytics",
            source_output_port="primary",
            source_contract_version="==9.9.9",
        )
    )

    with pytest.raises(ValueError, match="does not satisfy"):
        backend.resolve_read_context(context=context)


def test_resolve_write_context_rejects_draft_product(governance_fixture):
    backend, data_product_backend, _ = governance_fixture

    product = data_product_backend.latest("dp.analytics")
    assert product is not None
    product.status = "draft"
    data_product_backend.put(product)

    context = GovernanceWriteContext(
        output_binding=DataProductOutputBinding(
            data_product="dp.analytics",
            port_name="primary",
        )
    )

    with pytest.raises(ValueError, match="status"):
        backend.resolve_write_context(context=context)


def test_resolve_write_context_enforces_product_version(governance_fixture):
    backend, _, _ = governance_fixture

    context = GovernanceWriteContext(
        output_binding=DataProductOutputBinding(
            data_product="dp.analytics",
            port_name="primary",
            data_product_version="==9.9.9",
        )
    )

    with pytest.raises(ValueError, match="could not be retrieved"):
        backend.resolve_write_context(context=context)
