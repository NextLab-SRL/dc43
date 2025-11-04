from __future__ import annotations

from typing import Mapping

import pytest
from open_data_contract_standard.model import (  # type: ignore
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
    Server,
)

pytest.importorskip("dc43_service_backends")

from dc43_service_backends.contracts.backend.local import LocalContractServiceBackend
from dc43_service_backends.contracts.backend.stores.filesystem import FSContractStore
from dc43_service_backends.data_quality import LocalDataQualityServiceBackend
from dc43_service_backends.governance.backend.local import LocalGovernanceServiceBackend
from dc43_service_clients.data_quality import ObservationPayload, ValidationResult
from dc43_service_clients.data_products.models import (
    DataProductInputBinding,
    DataProductOutputBinding,
)
from dc43_service_clients.governance import (
    ContractReference,
    GovernanceReadContext,
    GovernanceWriteContext,
    LocalGovernanceServiceClient,
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
def governance_client(tmp_path):
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

    # Mirror the approval workflow by marking the initial draft as an active
    # release so read/write flows exercise the post-review path.  The local
    # backend keeps every submitted draft, so promote the latest version while
    # leaving the draft history in place for other assertions.
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
    client = LocalGovernanceServiceClient(backend)
    return client, data_product_backend, contract


def test_client_resolves_read_context(governance_client):
    client, data_product_backend, contract = governance_client

    context = GovernanceReadContext(
        input_binding=DataProductInputBinding(
            data_product="dp.analytics",
            port_name="orders",
        )
    )

    plan = client.resolve_read_context(context=context)
    assert plan.contract_id == contract.id
    assessment = client.evaluate_read_plan(
        plan=plan,
        validation=ValidationResult(ok=True, status="ok"),
        observations=lambda: ObservationPayload(metrics={}, schema=None),
    )
    client.register_read_activity(plan=plan, assessment=assessment)
    assert data_product_backend.last_input_call is not None


def test_client_resolves_write_context(governance_client):
    client, data_product_backend, contract = governance_client

    context = GovernanceWriteContext(
        contract=ContractReference(
            contract_id=contract.id,
            contract_version=contract.version,
        ),
        output_binding=DataProductOutputBinding(
            data_product="dp.analytics",
            port_name="publish",
        ),
    )

    plan = client.resolve_write_context(context=context)
    assert plan.contract_version == contract.version
    assessment = client.evaluate_write_plan(
        plan=plan,
        validation=ValidationResult(ok=True, status="ok"),
        observations=lambda: ObservationPayload(metrics={}, schema=None),
    )
    with pytest.raises(RuntimeError, match="requires review"):
        client.register_write_activity(plan=plan, assessment=assessment)
    assert data_product_backend.last_output_call is not None


def test_client_register_write_activity_when_port_exists(governance_client):
    client, data_product_backend, contract = governance_client

    context = GovernanceWriteContext(
        contract=ContractReference(
            contract_id=contract.id,
            contract_version=contract.version,
        ),
        output_binding=DataProductOutputBinding(
            data_product="dp.analytics",
            port_name="primary",
        ),
    )

    plan = client.resolve_write_context(context=context)
    assert plan.contract_version == contract.version
    assessment = client.evaluate_write_plan(
        plan=plan,
        validation=ValidationResult(ok=True, status="ok"),
        observations=lambda: ObservationPayload(metrics={}, schema=None),
    )
    client.register_write_activity(plan=plan, assessment=assessment)
    assert data_product_backend.last_output_call is not None
