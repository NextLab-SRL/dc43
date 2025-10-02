from __future__ import annotations

import json
from collections.abc import Mapping

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

try:
    from dc43_service_backends.data_products import LocalDataProductServiceBackend
except ModuleNotFoundError:  # pragma: no cover - exercise fallback when backends missing
    from dc43_service_clients.testing import LocalDataProductServiceBackend
from dc43_service_clients.odps import DataProductInputPort, DataProductOutputPort
from dc43_service_clients.contracts.client.remote import RemoteContractServiceClient
from dc43_service_clients.data_quality import ObservationPayload
from dc43_service_clients.data_products.client.remote import RemoteDataProductServiceClient
from dc43_service_clients.data_quality.client.remote import RemoteDataQualityServiceClient
from dc43_service_clients.data_quality.models import ValidationResult
from dc43_service_clients.data_quality.transport import encode_validation_result
from dc43_service_clients.governance.client.remote import RemoteGovernanceServiceClient


def _build_validation(payload: Mapping[str, object] | None) -> ValidationResult:
    metrics: Mapping[str, object] | None = None
    schema_raw: Mapping[str, Mapping[str, object]] | None = None
    reused = False
    if isinstance(payload, Mapping):
        metrics = payload.get("metrics") if isinstance(payload.get("metrics"), Mapping) else None
        schema_candidate = payload.get("schema")
        if isinstance(schema_candidate, Mapping):
            schema_raw = {
                key: dict(value) if isinstance(value, Mapping) else {}
                for key, value in schema_candidate.items()
            }
        reused = bool(payload.get("reused", False))
    return ValidationResult(
        ok=True,
        metrics=dict(metrics or {}),
        schema=dict(schema_raw or {}),
        status="ok",
        details={"reused": reused},
    )


def _contract_payload(contract: OpenDataContractStandard) -> dict[str, object]:
    return contract.model_dump(by_alias=True, exclude_none=True)


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


class _ServiceBackendMock:
    def __init__(self, contract: OpenDataContractStandard, *, token: str) -> None:
        self._contract = contract
        self._token = token
        self._contracts = {contract.version: contract}
        self._dataset_links: dict[tuple[str, str], str] = {}
        self._governance_status: dict[tuple[str, str], ValidationResult] = {}
        self._pipeline_activity: list[dict[str, object]] = []
        self._data_products = LocalDataProductServiceBackend()
        self._data_products.register_output_port(
            data_product_id="dp.analytics",
            port=DataProductOutputPort(
                name="primary",
                version=contract.version,
                contract_id=contract.id,
            ),
        )

    def __call__(self, request: "httpx.Request") -> "httpx.Response":  # pragma: no cover - exercised in tests
        auth_error = self._require_token(request)
        if auth_error is not None:
            return auth_error
        method = request.method
        path = request.url.path

        if path.startswith("/contracts/"):
            return self._handle_contracts(request, method, path)
        if path.startswith("/data-quality/"):
            return self._handle_data_quality(request, method, path)
        if path.startswith("/governance/"):
            return self._handle_governance(request, method, path)
        if path.startswith("/data-products/"):
            return self._handle_data_products(request, method, path)
        return httpx.Response(status_code=404, json={"detail": "Not Found"})

    def _require_token(self, request: "httpx.Request") -> "httpx.Response | None":
        authorization = request.headers.get("Authorization")
        expected = f"Bearer {self._token}"
        if authorization != expected:
            return httpx.Response(status_code=401, json={"detail": "Unauthorized"})
        return None

    def _handle_contracts(self, request: "httpx.Request", method: str, path: str) -> "httpx.Response":
        segments = path.strip("/").split("/")
        if len(segments) >= 4 and segments[2] == "versions" and method == "GET":
            contract_id, contract_version = segments[1], segments[3]
            if not self._ensure_contract(contract_id, contract_version):
                return httpx.Response(status_code=404, json={"detail": "Unknown contract"})
            contract = self._contracts[contract_version]
            return httpx.Response(status_code=200, json=_contract_payload(contract))
        if len(segments) == 3 and segments[2] == "versions" and method == "GET":
            contract_id = segments[1]
            if not self._ensure_contract(contract_id):
                return httpx.Response(status_code=404, json={"detail": "Unknown contract"})
            return httpx.Response(status_code=200, json=list(self._contracts.keys()))
        if len(segments) == 3 and segments[2] == "latest" and method == "GET":
            contract_id = segments[1]
            if not self._ensure_contract(contract_id):
                return httpx.Response(status_code=404, json={"detail": "Unknown contract"})
            return httpx.Response(status_code=200, json=_contract_payload(self._contract))
        if path == "/contracts/link" and method == "POST":
            payload = self._read_json(request)
            required = {"dataset_id", "dataset_version", "contract_id", "contract_version"}
            if not required.issubset(payload):
                return httpx.Response(status_code=400, json={"detail": "Missing linkage fields"})
            contract_id = str(payload["contract_id"])
            contract_version = str(payload["contract_version"])
            if not self._ensure_contract(contract_id, contract_version):
                return httpx.Response(status_code=404, json={"detail": "Unknown contract"})
            dataset_key = (str(payload["dataset_id"]), str(payload["dataset_version"]))
            self._dataset_links[dataset_key] = f"{contract_id}:{contract_version}"
            return httpx.Response(status_code=204)
        if len(segments) == 4 and segments[2] == "datasets" and method == "GET":
            dataset_id = segments[3]
            dataset_version = request.url.params.get("dataset_version")
            if dataset_version is None:
                return httpx.Response(status_code=404, json={"detail": "Missing dataset version"})
            key = (dataset_id, dataset_version)
            if key not in self._dataset_links:
                return httpx.Response(status_code=404, json={"detail": "Dataset not linked"})
            contract_reference = self._dataset_links[key]
            _, _, version = contract_reference.partition(":")
            return httpx.Response(status_code=200, json={"contract_version": version})
        return httpx.Response(status_code=404, json={"detail": "Unknown contract endpoint"})

    def _handle_data_quality(self, request: "httpx.Request", method: str, path: str) -> "httpx.Response":
        if path == "/data-quality/evaluate" and method == "POST":
            payload = self._read_json(request)
            dq_payload = payload.get("payload") if isinstance(payload, Mapping) else None
            result = _build_validation(dq_payload if isinstance(dq_payload, Mapping) else None)
            return httpx.Response(status_code=200, json=encode_validation_result(result) or {})
        if path == "/data-quality/expectations" and method == "POST":
            expectations = [
                {"expectation": "row_count", "description": "Row count must be non-negative"},
                {"expectation": "not_null", "description": "order_id should be present"},
            ]
            return httpx.Response(status_code=200, json=expectations)
        return httpx.Response(status_code=404, json={"detail": "Unknown data-quality endpoint"})

    def _handle_data_products(self, request: "httpx.Request", method: str, path: str) -> "httpx.Response":
        segments = path.strip("/").split("/")
        if len(segments) < 2:
            return httpx.Response(status_code=404, json={"detail": "Unknown data-product endpoint"})
        product_id = segments[1]
        backend = self._data_products

        if len(segments) >= 4 and segments[2] == "versions" and method == "GET":
            version = segments[3]
            try:
                product = backend.get(product_id, version)
            except FileNotFoundError:
                return httpx.Response(status_code=404, json={"detail": "Unknown data product"})
            return httpx.Response(status_code=200, json=product.to_dict())

        if len(segments) == 3 and segments[2] == "latest" and method == "GET":
            product = backend.latest(product_id)
            if product is None:
                return httpx.Response(status_code=404, json={"detail": "Unknown data product"})
            return httpx.Response(status_code=200, json=product.to_dict())

        if len(segments) == 3 and segments[2] == "versions" and method == "GET":
            versions = backend.list_versions(product_id)
            return httpx.Response(status_code=200, json=list(versions))

        if len(segments) == 3 and segments[2] == "input-ports" and method == "POST":
            payload = self._read_json(request)
            port = DataProductInputPort(
                name=str(payload.get("port_name")),
                version=str(payload.get("contract_version")),
                contract_id=str(payload.get("contract_id")),
            )
            result = backend.register_input_port(
                data_product_id=product_id,
                port=port,
                bump=str(payload.get("bump", "minor")),
                custom_properties=payload.get("custom_properties"),
                source_data_product=payload.get("source_data_product"),
                source_output_port=payload.get("source_output_port"),
            )
            return httpx.Response(
                status_code=200,
                json={
                    "product": result.product.to_dict(),
                    "changed": result.changed,
                },
            )

        if len(segments) == 3 and segments[2] == "output-ports" and method == "POST":
            payload = self._read_json(request)
            port = DataProductOutputPort(
                name=str(payload.get("port_name")),
                version=str(payload.get("contract_version")),
                contract_id=str(payload.get("contract_id")),
            )
            result = backend.register_output_port(
                data_product_id=product_id,
                port=port,
                bump=str(payload.get("bump", "minor")),
                custom_properties=payload.get("custom_properties"),
            )
            return httpx.Response(
                status_code=200,
                json={
                    "product": result.product.to_dict(),
                    "changed": result.changed,
                },
            )

        if len(segments) == 5 and segments[2] == "output-ports" and segments[4] == "contract" and method == "GET":
            port_name = segments[3]
            resolved = backend.resolve_output_contract(
                data_product_id=product_id,
                port_name=port_name,
            )
            if resolved is None:
                return httpx.Response(status_code=404, json={"detail": "Unknown output port"})
            contract_id, contract_version = resolved
            return httpx.Response(
                status_code=200,
                json={"contract_id": contract_id, "contract_version": contract_version},
            )

        return httpx.Response(status_code=404, json={"detail": "Unknown data-product endpoint"})

    def _handle_governance(self, request: "httpx.Request", method: str, path: str) -> "httpx.Response":
        if path == "/governance/evaluate" and method == "POST":
            payload = self._read_json(request)
            dataset_id = payload.get("dataset_id")
            dataset_version = payload.get("dataset_version")
            if dataset_id is None or dataset_version is None:
                return httpx.Response(status_code=400, json={"detail": "Missing dataset identifiers"})
            observations = payload.get("observations") if isinstance(payload, Mapping) else None
            validation_result = _build_validation(observations if isinstance(observations, Mapping) else None)
            key = (str(dataset_id), str(dataset_version))
            self._governance_status[key] = validation_result
            self._pipeline_activity.append(
                {"event": "governance.evaluate", "dataset_id": str(dataset_id), "dataset_version": str(dataset_version)}
            )
            reused = bool(observations.get("reused", False)) if isinstance(observations, Mapping) else False
            return httpx.Response(
                status_code=200,
                json={
                    "status": encode_validation_result(validation_result),
                    "draft": None,
                    "observations_reused": reused,
                },
            )
        if path == "/governance/status" and method == "GET":
            params = request.url.params
            contract_id = params.get("contract_id")
            contract_version = params.get("contract_version")
            dataset_id = params.get("dataset_id")
            dataset_version = params.get("dataset_version")
            if not self._ensure_contract(contract_id or "", contract_version or ""):
                return httpx.Response(status_code=404, json={"detail": "Unknown contract"})
            key = (dataset_id, dataset_version)
            if None in key or key not in self._governance_status:
                return httpx.Response(status_code=404, json={"detail": "No status recorded"})
            return httpx.Response(status_code=200, json=encode_validation_result(self._governance_status[key]) or {})
        if path == "/governance/link" and method == "POST":
            payload = self._read_json(request)
            required = {"dataset_id", "dataset_version", "contract_id", "contract_version"}
            if not required.issubset(payload):
                return httpx.Response(status_code=400, json={"detail": "Missing linkage fields"})
            contract_id = str(payload["contract_id"])
            contract_version = str(payload["contract_version"])
            if not self._ensure_contract(contract_id, contract_version):
                return httpx.Response(status_code=404, json={"detail": "Unknown contract"})
            dataset_key = (str(payload["dataset_id"]), str(payload["dataset_version"]))
            self._dataset_links[dataset_key] = f"{contract_id}:{contract_version}"
            self._pipeline_activity.append(
                {
                    "event": "governance.link",
                    "dataset_id": dataset_key[0],
                    "dataset_version": dataset_key[1],
                }
            )
            return httpx.Response(status_code=204)
        if path == "/governance/linked" and method == "GET":
            dataset_id = request.url.params.get("dataset_id")
            dataset_version = request.url.params.get("dataset_version")
            if dataset_id is None or dataset_version is None:
                return httpx.Response(status_code=404, json={"detail": "Missing dataset version"})
            key = (dataset_id, dataset_version)
            if key not in self._dataset_links:
                return httpx.Response(status_code=404, json={"detail": "Dataset not linked"})
            return httpx.Response(status_code=200, json={"contract_version": self._dataset_links[key]})
        if path == "/governance/activity" and method == "GET":
            dataset_id = request.url.params.get("dataset_id")
            dataset_version = request.url.params.get("dataset_version")
            activities = [
                item
                for item in self._pipeline_activity
                if item["dataset_id"] == dataset_id
                and (dataset_version is None or item["dataset_version"] == dataset_version)
            ]
            return httpx.Response(status_code=200, json=activities)
        if path == "/governance/auth" and method == "POST":  # pragma: no cover - smoke path
            return httpx.Response(status_code=204)
        return httpx.Response(status_code=404, json={"detail": "Unknown governance endpoint"})

    def _ensure_contract(self, contract_id: str, contract_version: str | None = None) -> bool:
        if contract_id != self._contract.id:
            return False
        if contract_version is not None and contract_version not in self._contracts:
            return False
        return True

    def _read_json(self, request: "httpx.Request") -> Mapping[str, object]:
        if not request.content:
            return {}
        try:
            return json.loads(request.content.decode())
        except json.JSONDecodeError:  # pragma: no cover - guard path
            return {}


@pytest.fixture()
def service_backend():
    contract = _sample_contract()
    token = "super-secret"
    backend = _ServiceBackendMock(contract, token=token)
    return {"backend": backend, "contract": contract, "token": token}


@pytest.fixture()
def http_clients(service_backend):
    backend = service_backend["backend"]
    contract = service_backend["contract"]
    token = service_backend["token"]

    contract_client = RemoteContractServiceClient(
        base_url="http://dc43-services",
        transport=httpx.MockTransport(backend),
        token=token,
    )
    dq_client = RemoteDataQualityServiceClient(
        base_url="http://dc43-services",
        transport=httpx.MockTransport(backend),
        token=token,
    )
    governance_client = RemoteGovernanceServiceClient(
        base_url="http://dc43-services",
        transport=httpx.MockTransport(backend),
        token=token,
    )
    data_product_client = RemoteDataProductServiceClient(
        base_url="http://dc43-services",
        transport=httpx.MockTransport(backend),
        token=token,
    )

    clients = {
        "contract": contract_client,
        "dq": dq_client,
        "governance": governance_client,
        "data_product": data_product_client,
        "contract_model": contract,
        "backend": backend,
        "token": token,
    }
    try:
        yield clients
    finally:
        contract_client.close()
        dq_client.close()
        governance_client.close()
        data_product_client.close()


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


def test_remote_data_product_client_registers_ports(http_clients):
    dp_client: RemoteDataProductServiceClient = http_clients["data_product"]

    registration = dp_client.register_input_port(
        data_product_id="dp.analytics",
        port_name="orders-input",
        contract_id="sales.orders",
        contract_version="1.0.0",
        source_data_product="dp.source",
        source_output_port="gold",
    )
    assert registration.changed is True
    assert any(port.name == "orders-input" for port in registration.product.input_ports)

    updated = dp_client.register_output_port(
        data_product_id="dp.analytics",
        port_name="forecast",
        contract_id="sales.forecast",
        contract_version="2.0.0",
    )
    assert updated.changed is True
    assert any(port.name == "forecast" for port in updated.product.output_ports)


def test_remote_data_product_resolve_output_contract(http_clients):
    dp_client: RemoteDataProductServiceClient = http_clients["data_product"]
    contract: OpenDataContractStandard = http_clients["contract_model"]

    contract_ref = dp_client.resolve_output_contract(
        data_product_id="dp.analytics",
        port_name="primary",
    )
    assert contract_ref == (contract.id, contract.version)


def test_http_clients_require_authentication(service_backend):
    backend = service_backend["backend"]
    contract = service_backend["contract"]

    client = RemoteContractServiceClient(
        base_url="http://dc43-services",
        transport=httpx.MockTransport(backend),
    )
    try:
        with pytest.raises(httpx.HTTPStatusError):
            client.list_versions(contract.id)
    finally:
        client.close()
