from __future__ import annotations

import asyncio

import pytest

try:  # pragma: no cover - optional dependency guard for test environment
    import httpx
except ModuleNotFoundError:  # pragma: no cover - skip if HTTP extras absent
    pytest.skip("httpx is required to run remote client tests", allow_module_level=True)
from open_data_contract_standard.model import (  # type: ignore
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
    Server,
)

from httpx import ASGITransport, BaseTransport

from dc43_service_backends.auth import bearer_token_dependency
from dc43_service_backends.contracts.backend.local import LocalContractServiceBackend
from dc43_service_backends.contracts.backend.stores import FSContractStore
from dc43_service_backends.data_quality.backend.local import LocalDataQualityServiceBackend
from dc43_service_backends.governance.backend.local import LocalGovernanceServiceBackend
from dc43_service_backends.server import build_app
from dc43_service_clients.contracts.client.remote import RemoteContractServiceClient
from dc43_service_clients.data_quality import ObservationPayload
from dc43_service_clients.data_quality.client.remote import RemoteDataQualityServiceClient
from dc43_service_clients.governance.client.remote import RemoteGovernanceServiceClient


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
                    SchemaProperty(name="order_id", physicalType="integer", required=True),
                    SchemaProperty(name="order_ts", physicalType="string"),
                ],
            )
        ],
        servers=[
            Server(server="s3", type="s3", path="datalake/orders", format="delta")
        ],
    )


class _SyncASGITransport(BaseTransport):
    def __init__(self, app):
        self._transport = ASGITransport(app=app)

    def handle_request(self, request):  # type: ignore[override]
        async def _dispatch() -> httpx.Response:
            response = await self._transport.handle_async_request(request)
            content = await response.aread()
            return httpx.Response(
                status_code=response.status_code,
                headers=response.headers,
                content=content,
                request=request,
                extensions=response.extensions,
            )

        return asyncio.run(_dispatch())

    def close(self) -> None:  # type: ignore[override]
        asyncio.run(self._transport.aclose())


@pytest.fixture()
def service_app(tmp_path):
    store = FSContractStore(str(tmp_path / "contracts"))
    contract = _sample_contract()
    store.put(contract)

    contract_backend = LocalContractServiceBackend(store)
    dq_backend = LocalDataQualityServiceBackend()
    governance_backend = LocalGovernanceServiceBackend(
        contract_client=contract_backend,
        dq_client=dq_backend,
        draft_store=store,
    )

    token = "super-secret"
    app = build_app(
        contract_backend=contract_backend,
        dq_backend=dq_backend,
        governance_backend=governance_backend,
        dependencies=[bearer_token_dependency(token)],
    )

    return {"app": app, "contract": contract, "token": token}


@pytest.fixture()
def http_clients(service_app):
    app = service_app["app"]
    contract = service_app["contract"]
    token = service_app["token"]

    contract_client = RemoteContractServiceClient(
        base_url="http://dc43-services",
        transport=_SyncASGITransport(app),
        token=token,
    )
    dq_client = RemoteDataQualityServiceClient(
        base_url="http://dc43-services",
        transport=_SyncASGITransport(app),
        token=token,
    )
    governance_client = RemoteGovernanceServiceClient(
        base_url="http://dc43-services",
        transport=_SyncASGITransport(app),
        token=token,
    )

    clients = {
        "contract": contract_client,
        "dq": dq_client,
        "governance": governance_client,
        "contract_model": contract,
        "app": app,
        "token": token,
    }
    try:
        yield clients
    finally:
        contract_client.close()
        dq_client.close()
        governance_client.close()


def test_remote_contract_client_roundtrip(http_clients):
    contract_client: RemoteContractServiceClient = http_clients["contract"]
    contract: OpenDataContractStandard = http_clients["contract_model"]

    retrieved = contract_client.get(contract.id, contract.version)
    assert retrieved.version == contract.version
    assert contract_client.list_versions(contract.id) == [contract.version]

    latest = contract_client.latest(contract.id)
    assert latest is not None and latest.version == contract.version

    # Linking succeeds even though the local backend is a no-op for dataset metadata.
    contract_client.link_dataset_contract(
        dataset_id="orders",
        dataset_version="2024-01-01",
        contract_id=contract.id,
        contract_version=contract.version,
    )
    assert contract_client.get_linked_contract_version(dataset_id="orders") is None


def test_remote_data_quality_client(http_clients):
    contract: OpenDataContractStandard = http_clients["contract_model"]
    dq_client: RemoteDataQualityServiceClient = http_clients["dq"]

    payload = ObservationPayload(
        metrics={"row_count": 10, "violations.not_null_order_id": 0},
        schema={
            "order_id": {"odcs_type": "integer", "nullable": False},
            "order_ts": {"odcs_type": "string", "nullable": True},
        },
    )
    result = dq_client.evaluate(contract=contract, payload=payload)
    assert result.ok

    expectations = dq_client.describe_expectations(contract=contract)
    assert isinstance(expectations, list)
    assert expectations


def test_remote_governance_client(http_clients):
    contract: OpenDataContractStandard = http_clients["contract_model"]
    governance_client: RemoteGovernanceServiceClient = http_clients["governance"]

    payload = ObservationPayload(
        metrics={"row_count": 10, "violations.not_null_order_id": 0},
        schema={
            "order_id": {"odcs_type": "integer", "nullable": False},
            "order_ts": {"odcs_type": "string", "nullable": True},
        },
    )
    assessment = governance_client.evaluate_dataset(
        contract_id=contract.id,
        contract_version=contract.version,
        dataset_id="orders",
        dataset_version="2024-01-01",
        validation=None,
        observations=lambda: payload,
        bump="minor",
        context=None,
        pipeline_context={"pipeline": "demo"},
        operation="read",
        draft_on_violation=True,
    )
    assert assessment.status is not None

    status = governance_client.get_status(
        contract_id=contract.id,
        contract_version=contract.version,
        dataset_id="orders",
        dataset_version="2024-01-01",
    )
    assert status is not None

    governance_client.link_dataset_contract(
        dataset_id="orders",
        dataset_version="2024-01-01",
        contract_id=contract.id,
        contract_version=contract.version,
    )

    linked_version = governance_client.get_linked_contract_version(
        dataset_id="orders",
        dataset_version="2024-01-01",
    )
    assert linked_version == f"{contract.id}:{contract.version}"

    activity = governance_client.get_pipeline_activity(dataset_id="orders")
    assert isinstance(activity, list)
    assert activity


def test_http_clients_require_authentication(service_app):
    app = service_app["app"]
    contract = service_app["contract"]

    client = RemoteContractServiceClient(
        base_url="http://dc43-services",
        transport=_SyncASGITransport(app),
    )
    try:
        with pytest.raises(httpx.HTTPStatusError):
            client.list_versions(contract.id)
    finally:
        client.close()
