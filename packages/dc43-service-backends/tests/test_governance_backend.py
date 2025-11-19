from __future__ import annotations

from typing import Mapping, Sequence

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
from dc43_service_clients.governance.lineage import decode_lineage_event
from dc43_service_clients.odps import (
    DataProductInputPort,
    DataProductOutputPort,
    OpenDataProductStandard,
)
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


def test_register_read_activity_respects_binding_version(governance_fixture):
    backend, data_product_backend, contract = governance_fixture

    product_v1 = OpenDataProductStandard(
        id="Sales.KPIs",
        status="draft",
        version="0.1.0-draft",
    )
    product_v1.ensure_input_port(
        DataProductInputPort(
            name="sales.orders",
            version=contract.version,
            contract_id=contract.id,
        )
    )
    data_product_backend.put(product_v1)

    product_v2 = product_v1.clone()
    product_v2.version = "0.2.0-draft"
    data_product_backend.put(product_v2)

    context = GovernanceReadContext(
        input_binding=DataProductInputBinding(
            data_product="Sales.KPIs",
            port_name="sales.orders",
            data_product_version="0.1.0-draft",
        ),
        allowed_data_product_statuses=("draft",),
        enforce_data_product_status=False,
    )

    plan = backend.resolve_read_context(context=context)
    assert plan.input_binding is not None
    assert plan.input_binding.data_product_version == "0.1.0-draft"

    assessment = backend.evaluate_read_plan(
        plan=plan,
        validation=ValidationResult(ok=True, status="ok"),
        observations=lambda: ObservationPayload(metrics={}, schema=None),
    )

    backend.register_read_activity(plan=plan, assessment=assessment)


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


def test_register_write_activity_respects_binding_version(governance_fixture):
    backend, data_product_backend, contract = governance_fixture

    product_v1 = OpenDataProductStandard(
        id="Sales.KPIs",
        status="draft",
        version="0.1.0-draft",
    )
    product_v1.ensure_output_port(
        DataProductOutputPort(
            name="kpis.simple",
            version=contract.version,
            contract_id=contract.id,
        )
    )
    data_product_backend.put(product_v1)

    product_v2 = product_v1.clone()
    product_v2.version = "0.2.0-draft"
    data_product_backend.put(product_v2)

    context = GovernanceWriteContext(
        output_binding=DataProductOutputBinding(
            data_product="Sales.KPIs",
            port_name="kpis.simple",
            data_product_version="0.1.0-draft",
        ),
        dataset_id="sales.kpis",
        dataset_version="2024-01-01",
        allowed_data_product_statuses=("draft",),
        enforce_data_product_status=False,
    )

    plan = backend.resolve_write_context(context=context)
    assert plan.output_binding is not None
    assert plan.output_binding.data_product_version == "0.1.0-draft"

    assessment = backend.evaluate_write_plan(
        plan=plan,
        validation=ValidationResult(ok=True, status="ok"),
        observations=lambda: ObservationPayload(metrics={}, schema=None),
    )

    backend.register_write_activity(plan=plan, assessment=assessment)


def test_get_dataset_records_returns_runs(governance_fixture):
    backend, _, contract = governance_fixture

    backend.evaluate_dataset(
        contract_id=contract.id,
        contract_version=contract.version,
        dataset_id="analytics.orders",
        dataset_version="2024-01-01",
        validation=ValidationResult(ok=True, status="ok"),
        observations=lambda: ObservationPayload(metrics={}, schema=None),
    )

    records = backend.get_dataset_records(dataset_id="analytics.orders")

    assert records
    assert records[0]["dataset_name"] == "analytics.orders"
    assert records[0]["status"] == "ok"


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


def test_register_write_skips_registration_for_pinned_version(governance_fixture):
    backend, data_product_backend, _ = governance_fixture

    product = data_product_backend.latest("dp.analytics")
    assert product is not None
    version = product.version
    assert version
    port = product.find_output_port("primary")
    assert port is not None
    port.custom_properties = [
        {"property": "dc43.output.physical_location", "value": "table"}
    ]
    data_product_backend.put(product)

    data_product_backend.last_output_call = None

    context = GovernanceWriteContext(
        output_binding=DataProductOutputBinding(
            data_product="dp.analytics",
            port_name="primary",
            data_product_version=version,
        ),
        dataset_id="analytics.orders.out",
    )

    plan = backend.resolve_write_context(context=context)
    assessment = backend.evaluate_write_plan(
        plan=plan,
        validation=ValidationResult(ok=True, status="ok"),
        observations=lambda: ObservationPayload(metrics={}, schema=None),
    )

    backend.register_write_activity(plan=plan, assessment=assessment)

    assert data_product_backend.last_output_call is None


def test_register_read_skips_registration_for_pinned_version(governance_fixture):
    backend, data_product_backend, _ = governance_fixture

    product = data_product_backend.latest("dp.analytics")
    assert product is not None
    version = product.version
    assert version
    port = product.find_input_port("orders")
    assert port is not None
    port.custom_properties = [
        {"property": "dc43.input.refresh_mode", "value": "batch"}
    ]
    data_product_backend.put(product)

    data_product_backend.last_input_call = None

    context = GovernanceReadContext(
        input_binding=DataProductInputBinding(
            data_product="dp.analytics",
            port_name="orders",
            data_product_version=version,
        )
    )

    plan = backend.resolve_read_context(context=context)
    assessment = backend.evaluate_read_plan(
        plan=plan,
        validation=ValidationResult(ok=True, status="ok"),
        observations=lambda: ObservationPayload(metrics={}, schema=None),
    )

    product.status = "draft"
    data_product_backend.put(product)

    with pytest.raises(ValueError, match="status"):
        backend.register_read_activity(plan=plan, assessment=assessment)

    assert data_product_backend.last_input_call is None


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


def test_resolve_read_context_allows_draft_when_permitted(governance_fixture):
    backend, data_product_backend, _ = governance_fixture

    product = data_product_backend.latest("dp.analytics")
    assert product is not None
    product.status = "draft"
    data_product_backend.put(product)

    context = GovernanceReadContext(
        input_binding=DataProductInputBinding(
            data_product="dp.analytics",
            port_name="orders",
        ),
        allowed_data_product_statuses=("active", "draft"),
    )

    plan = backend.resolve_read_context(context=context)
    assert plan.allowed_data_product_statuses == ("active", "draft")

    assessment = backend.evaluate_read_plan(
        plan=plan,
        validation=ValidationResult(ok=True, status="ok"),
        observations=lambda: ObservationPayload(metrics={}, schema=None),
    )
    backend.register_read_activity(plan=plan, assessment=assessment)


def test_publish_lineage_event_registers_links(governance_fixture):
    backend, data_product_backend, contract = governance_fixture

    dataset_id = "analytics.orders"
    dataset_version = "2024-01-01"
    run_id = "44695653-fc1a-4ec6-8c2a-6c6a44ec5ad9"
    lineage_payload = {
        "eventType": "COMPLETE",
        "eventTime": "2024-01-01T00:00:00Z",
        "producer": "https://dc43.example/tests",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#",
        "run": {
            "runId": run_id,
            "facets": {
                "dc43PipelineContext": {"context": {"job": "orders.read"}},
                "dc43Validation": {"status": "ok", "ok": True},
            },
        },
        "job": {"namespace": "dc43", "name": "orders-job"},
        "inputs": [
            {
                "namespace": "dc43",
                "name": dataset_id,
                "facets": {
                    "dc43Dataset": {
                        "datasetId": dataset_id,
                        "datasetVersion": dataset_version,
                        "operation": "read",
                    },
                    "dc43Contract": {
                        "contractId": contract.id,
                        "contractVersion": contract.version,
                    },
                    "dc43DataProduct": {
                        "dataProduct": "dp.analytics",
                        "portName": "orders",
                    },
                    "version": {"datasetVersion": dataset_version},
                },
            }
        ],
        "outputs": [],
    }

    event = decode_lineage_event(lineage_payload)
    assert event is not None

    backend.publish_lineage_event(event=event)

    assert data_product_backend.last_input_call is not None
    assert backend.get_linked_contract_version(
        dataset_id=dataset_id,
        dataset_version=dataset_version,
    ) == f"{contract.id}:{contract.version}"

    activity = backend.get_pipeline_activity(
        dataset_id=dataset_id,
        dataset_version=dataset_version,
    )
    assert activity, "pipeline activity should include lineage event"
    lineage_entry = activity[0].get("lineage_event")
    assert isinstance(lineage_entry, Mapping)
    assert lineage_entry.get("run", {}).get("runId") == run_id
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


def test_resolve_write_context_allows_draft_when_permitted(governance_fixture):
    backend, data_product_backend, _ = governance_fixture

    product = data_product_backend.latest("dp.analytics")
    assert product is not None
    product.status = "draft"
    data_product_backend.put(product)

    context = GovernanceWriteContext(
        output_binding=DataProductOutputBinding(
            data_product="dp.analytics",
            port_name="primary",
        ),
        allowed_data_product_statuses=("active", "draft"),
    )

    plan = backend.resolve_write_context(context=context)
    assert plan.allowed_data_product_statuses == ("active", "draft")

    assessment = backend.evaluate_write_plan(
        plan=plan,
        validation=ValidationResult(ok=True, status="ok"),
        observations=lambda: ObservationPayload(metrics={}, schema=None),
    )
    backend.register_write_activity(plan=plan, assessment=assessment)


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


def test_pipeline_activity_uses_batch_statuses() -> None:
    class DummyStore:
        def __init__(self) -> None:
            self.matrix_calls = 0
            self.status_calls = 0

        def load_pipeline_activity(self, *, dataset_id: str, dataset_version: str | None = None):
            return (
                {
                    "dataset_id": dataset_id,
                    "dataset_version": dataset_version or "2024-01-01",
                    "contract_id": "sales.orders",
                    "contract_version": "1.0.0",
                },
            )

        def load_status_matrix_entries(
            self,
            *,
            dataset_id: str,
            dataset_versions: Sequence[str] | None = None,
            contract_ids: Sequence[str] | None = None,
        ):
            self.matrix_calls += 1
            version = (dataset_versions or ["2024-01-01"])[0]
            return (
                {
                    "dataset_id": dataset_id,
                    "dataset_version": version,
                    "contract_id": "sales.orders",
                    "contract_version": "1.0.0",
                    "status": ValidationResult(status="ok"),
                },
            )

        def load_status(self, **_kwargs):
            self.status_calls += 1
            return ValidationResult(status="warn")

    store = DummyStore()
    backend = LocalGovernanceServiceBackend(
        contract_client=object(),
        dq_client=object(),
        data_product_client=None,
        store=store,
    )

    records = backend.get_pipeline_activity(
        dataset_id="sales.orders",
        dataset_version="2024-01-01",
        include_status=True,
    )

    assert records
    assert isinstance(records[0].get("validation_status"), ValidationResult)
    assert store.matrix_calls == 1
    assert store.status_calls == 0
