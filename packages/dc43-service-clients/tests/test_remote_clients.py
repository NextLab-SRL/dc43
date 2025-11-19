from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime, timezone

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
from dc43_service_clients.odps import (
    DataProductInputPort,
    DataProductOutputPort,
    OpenDataProductStandard,
    to_model as to_data_product_model,
)
from dc43_service_clients.contracts.client.remote import RemoteContractServiceClient
from dc43_service_clients.data_quality import ObservationPayload
from dc43_service_clients.data_products.client.remote import RemoteDataProductServiceClient
from dc43_service_clients.data_products.models import DataProductOutputBinding
from dc43_service_clients.data_quality.client.remote import RemoteDataQualityServiceClient
from dc43_service_clients.data_quality.models import ValidationResult
from dc43_service_clients.data_quality.transport import encode_validation_result
from dc43_service_clients.governance.client.remote import RemoteGovernanceServiceClient
from dc43_service_clients.governance.lineage import (
    decode_lineage_event,
    encode_lineage_event,
)
from dc43_service_clients.governance import (
    ContractReference,
    GovernanceReadContext,
    GovernanceWriteContext,
)


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
        self._metrics: list[dict[str, object]] = []
        self._lineage_events: list[Mapping[str, object]] = []
        self._data_products = LocalDataProductServiceBackend()
        self._data_products.register_output_port(
            data_product_id="dp.analytics",
            port=DataProductOutputPort(
                name="primary",
                version=contract.version,
                contract_id=contract.id,
            ),
        )
        self._last_read_registration: Mapping[str, object] | None = None
        self._last_write_registration: Mapping[str, object] | None = None

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
        if len(segments) >= 4 and segments[2] == "versions" and method == "PUT":
            contract_id, contract_version = segments[1], segments[3]
            payload = self._read_json(request)
            try:
                contract = OpenDataContractStandard.model_validate(payload)
            except Exception as exc:  # pragma: no cover - malformed payload guard
                return httpx.Response(status_code=400, json={"detail": str(exc)})
            if contract.id and contract.id != contract_id:
                return httpx.Response(status_code=400, json={"detail": "contract id mismatch"})
            if contract.version and contract.version != contract_version:
                return httpx.Response(status_code=400, json={"detail": "contract version mismatch"})
            contract.id = contract_id
            contract.version = contract_version
            self._contracts[contract_version] = contract
            self._contract = contract
            return httpx.Response(status_code=204)
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
            return httpx.Response(status_code=200, json=sorted(self._contracts.keys()))
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

        if len(segments) >= 4 and segments[2] == "versions" and method == "PUT":
            version = segments[3]
            payload = self._read_json(request)
            try:
                product = to_data_product_model(payload)
            except Exception as exc:  # pragma: no cover - malformed payload guard
                return httpx.Response(status_code=400, json={"detail": str(exc)})
            if product.id and product.id != product_id:
                return httpx.Response(status_code=400, json={"detail": "product id mismatch"})
            if product.version and product.version != version:
                return httpx.Response(status_code=400, json={"detail": "product version mismatch"})
            product.id = product_id
            product.version = version
            backend.put(product)
            return httpx.Response(status_code=204)

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
            return httpx.Response(status_code=200, json=sorted(versions))

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
        def _record(metric_result: ValidationResult, *, contract_id: str | None, contract_version: str | None, dataset_id: str, dataset_version: str) -> None:
            metrics = metric_result.metrics if hasattr(metric_result, "metrics") else None
            if not metrics:
                return
            recorded_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            for key, value in metrics.items():
                if isinstance(value, (int, float)):
                    numeric = float(value)
                    serialized = str(value)
                elif value is None:
                    numeric = None
                    serialized = None
                else:
                    try:
                        serialized = json.dumps(value)
                    except TypeError:
                        serialized = str(value)
                    numeric = None
                self._metrics.append(
                    {
                        "dataset_id": dataset_id,
                        "dataset_version": dataset_version,
                        "contract_id": contract_id,
                        "contract_version": contract_version,
                        "status_recorded_at": recorded_at,
                        "metric_key": str(key),
                        "metric_value": serialized,
                        "metric_numeric_value": numeric,
                    }
                )

        if path == "/governance/read/resolve" and method == "POST":
            context = self._read_json(request).get("context", {})
            try:
                plan = self._build_plan_from_context(context, operation="read")
            except ValueError as exc:
                return httpx.Response(status_code=400, json={"detail": str(exc)})
            return httpx.Response(status_code=200, json=plan)
        if path == "/governance/write/resolve" and method == "POST":
            context = self._read_json(request).get("context", {})
            try:
                plan = self._build_plan_from_context(context, operation="write")
            except ValueError as exc:
                return httpx.Response(status_code=400, json={"detail": str(exc)})
            return httpx.Response(status_code=200, json=plan)
        if path == "/governance/read/evaluate" and method == "POST":
            payload = self._read_json(request)
            try:
                plan = self._build_plan_from_context(payload.get("plan", {}), operation="read")
            except ValueError as exc:
                return httpx.Response(status_code=400, json={"detail": str(exc)})
            observations = payload.get("observations")
            validation_result = _build_validation(
                observations if isinstance(observations, Mapping) else None
            )
            dataset_id = str(plan["dataset_id"])
            dataset_version = str(plan["dataset_version"])
            self._governance_status[(dataset_id, dataset_version)] = validation_result
            self._pipeline_activity.append(
                {"event": "governance.read.evaluate", "dataset_id": dataset_id, "dataset_version": dataset_version}
            )
            contract_ref = plan.get("contract_id"), plan.get("contract_version")
            if all(contract_ref):
                self._dataset_links[(dataset_id, dataset_version)] = f"{contract_ref[0]}:{contract_ref[1]}"
            _record(
                validation_result,
                contract_id=str(plan.get("contract_id")) if plan.get("contract_id") else None,
                contract_version=str(plan.get("contract_version")) if plan.get("contract_version") else None,
                dataset_id=dataset_id,
                dataset_version=dataset_version,
            )
            reused = bool(observations.get("reused", False)) if isinstance(observations, Mapping) else False
            return httpx.Response(
                status_code=200,
                json={
                    "status": encode_validation_result(validation_result),
                    "validation": encode_validation_result(validation_result),
                    "draft": None,
                    "observations_reused": reused,
                },
            )
        if path == "/governance/write/evaluate" and method == "POST":
            payload = self._read_json(request)
            try:
                plan = self._build_plan_from_context(payload.get("plan", {}), operation="write")
            except ValueError as exc:
                return httpx.Response(status_code=400, json={"detail": str(exc)})
            observations = payload.get("observations")
            validation_result = _build_validation(
                observations if isinstance(observations, Mapping) else None
            )
            dataset_id = str(plan["dataset_id"])
            dataset_version = str(plan["dataset_version"])
            self._governance_status[(dataset_id, dataset_version)] = validation_result
            self._pipeline_activity.append(
                {"event": "governance.write.evaluate", "dataset_id": dataset_id, "dataset_version": dataset_version}
            )
            contract_ref = plan.get("contract_id"), plan.get("contract_version")
            if all(contract_ref):
                self._dataset_links[(dataset_id, dataset_version)] = f"{contract_ref[0]}:{contract_ref[1]}"
            _record(
                validation_result,
                contract_id=str(plan.get("contract_id")) if plan.get("contract_id") else None,
                contract_version=str(plan.get("contract_version")) if plan.get("contract_version") else None,
                dataset_id=dataset_id,
                dataset_version=dataset_version,
            )
            reused = bool(observations.get("reused", False)) if isinstance(observations, Mapping) else False
            return httpx.Response(
                status_code=200,
                json={
                    "status": encode_validation_result(validation_result),
                    "validation": encode_validation_result(validation_result),
                    "draft": None,
                    "observations_reused": reused,
                },
            )
        if path == "/governance/read/register" and method == "POST":
            payload = self._read_json(request)
            self._last_read_registration = payload
            plan = payload.get("plan", {})
            dataset_id = str(plan.get("dataset_id", ""))
            dataset_version = str(plan.get("dataset_version", ""))
            if dataset_id and dataset_version:
                self._pipeline_activity.append(
                    {
                        "event": "governance.read.register",
                        "dataset_id": dataset_id,
                        "dataset_version": dataset_version,
                    }
                )
            return httpx.Response(status_code=204)
        if path == "/governance/write/register" and method == "POST":
            payload = self._read_json(request)
            self._last_write_registration = payload
            plan = payload.get("plan", {})
            contract_id = plan.get("contract_id")
            contract_version = plan.get("contract_version")
            dataset_id = plan.get("dataset_id")
            dataset_version = plan.get("dataset_version")
            if all([contract_id, contract_version, dataset_id, dataset_version]):
                key = (str(dataset_id), str(dataset_version))
                self._dataset_links[key] = f"{contract_id}:{contract_version}"
                self._pipeline_activity.append(
                    {
                        "event": "governance.write.register",
                        "dataset_id": key[0],
                        "dataset_version": key[1],
                    }
                )
            return httpx.Response(status_code=204)
        if path == "/governance/lineage" and method == "POST":
            payload = self._read_json(request)
            event_payload = payload.get("event") if isinstance(payload, Mapping) else None
            if not isinstance(event_payload, Mapping):
                return httpx.Response(status_code=400, json={"detail": "Missing lineage event"})
            self._lineage_events.append(dict(event_payload))
            return httpx.Response(status_code=204)
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
            contract_ref = payload.get("contract_id"), payload.get("contract_version")
            if all(contract_ref):
                self._dataset_links[key] = f"{contract_ref[0]}:{contract_ref[1]}"
            _record(
                validation_result,
                contract_id=str(payload.get("contract_id")) if payload.get("contract_id") else None,
                contract_version=str(payload.get("contract_version")) if payload.get("contract_version") else None,
                dataset_id=str(dataset_id),
                dataset_version=str(dataset_version),
            )
            reused = bool(observations.get("reused", False)) if isinstance(observations, Mapping) else False
            return httpx.Response(
                status_code=200,
                json={
                    "status": encode_validation_result(validation_result),
                    "validation": encode_validation_result(validation_result),
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
        if path == "/governance/status-matrix" and method == "GET":
            params = request.url.params
            dataset_id = params.get("dataset_id")
            if not dataset_id:
                return httpx.Response(status_code=400, json={"detail": "dataset_id required"})
            contract_filters = (
                params.getlist("contract_id") if hasattr(params, "getlist") else []
            )
            dataset_filters = (
                params.getlist("dataset_version") if hasattr(params, "getlist") else []
            )
            entries: list[dict[str, object]] = []
            for (ds_id, ds_version), status in self._governance_status.items():
                if ds_id != dataset_id:
                    continue
                if dataset_filters and ds_version not in dataset_filters:
                    continue
                contract_ref = self._dataset_links.get((ds_id, ds_version))
                if not contract_ref:
                    continue
                contract_id, _, contract_version = contract_ref.partition(":")
                if contract_filters and contract_id not in contract_filters:
                    continue
                entries.append(
                    {
                        "dataset_id": ds_id,
                        "dataset_version": ds_version,
                        "contract_id": contract_id,
                        "contract_version": contract_version,
                        "status": encode_validation_result(status),
                    }
                )
            return httpx.Response(
                status_code=200,
                json={"dataset_id": dataset_id, "entries": entries},
            )
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
        if path == "/governance/metrics" and method == "GET":
            dataset_id = request.url.params.get("dataset_id")
            if dataset_id is None:
                return httpx.Response(status_code=400, json={"detail": "Missing dataset identifier"})
            dataset_version = request.url.params.get("dataset_version")
            contract_id = request.url.params.get("contract_id")
            contract_version = request.url.params.get("contract_version")
            entries = [
                entry
                for entry in self._metrics
                if entry.get("dataset_id") == dataset_id
                and (dataset_version is None or entry.get("dataset_version") == dataset_version)
                and (contract_id is None or entry.get("contract_id") == contract_id)
                and (contract_version is None or entry.get("contract_version") == contract_version)
            ]
            return httpx.Response(status_code=200, json=entries)
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
        if path == "/governance/dataset-records" and method == "GET":
            dataset_id = request.url.params.get("dataset_id")
            dataset_version = request.url.params.get("dataset_version")
            entries: list[dict[str, object]] = []
            for (ds_id, ds_version), status in self._governance_status.items():
                if dataset_id and ds_id != dataset_id:
                    continue
                if dataset_version and ds_version != dataset_version:
                    continue
                contract_id = ""
                contract_version = ""
                contract_ref = self._dataset_links.get((ds_id, ds_version))
                if contract_ref:
                    contract_id, _, contract_version = contract_ref.partition(":")
                entries.append(
                    {
                        "dataset_name": ds_id,
                        "dataset_version": ds_version,
                        "contract_id": contract_id,
                        "contract_version": contract_version,
                        "status": getattr(status, "status", "unknown"),
                        "dq_details": getattr(status, "details", {}),
                        "run_type": "infer",
                        "violations": 0,
                    }
                )
            return httpx.Response(status_code=200, json=entries)
        if path == "/governance/auth" and method == "POST":  # pragma: no cover - smoke path
            return httpx.Response(status_code=204)
        return httpx.Response(status_code=404, json={"detail": "Unknown governance endpoint"})

    def _build_plan_from_context(self, context: Mapping[str, object], *, operation: str) -> dict[str, object]:
        contract_spec = context.get("contract") if isinstance(context, Mapping) else None
        contract_id = None
        contract_version = None
        if isinstance(contract_spec, Mapping):
            contract_id = contract_spec.get("contract_id")
            contract_version = contract_spec.get("contract_version")
        if contract_id is None or contract_version is None:
            contract_id = self._contract.id
            contract_version = self._contract.version
        if not self._ensure_contract(str(contract_id), str(contract_version)):
            raise ValueError("unknown contract reference")
        contract = self._contracts.get(str(contract_version), self._contract)
        dataset_id = context.get("dataset_id") if isinstance(context, Mapping) else None
        dataset_version = context.get("dataset_version") if isinstance(context, Mapping) else None
        plan: dict[str, object] = {
            "contract": _contract_payload(contract),
            "contract_id": str(contract_id),
            "contract_version": str(contract_version),
            "dataset_id": dataset_id or contract.id,
            "dataset_version": dataset_version or contract.version,
            "dataset_format": context.get("dataset_format") if isinstance(context, Mapping) else None,
            "pipeline_context": context.get("pipeline_context") if isinstance(context, Mapping) else None,
            "bump": context.get("bump", "minor") if isinstance(context, Mapping) else "minor",
            "draft_on_violation": bool(context.get("draft_on_violation", False))
            if isinstance(context, Mapping)
            else False,
        }
        if operation == "read":
            plan["input_binding"] = context.get("input_binding") if isinstance(context, Mapping) else None
        else:
            plan["output_binding"] = context.get("output_binding") if isinstance(context, Mapping) else None
        return plan

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

    updated_payload = contract.model_dump(by_alias=True, exclude_none=True)
    updated_payload["version"] = "1.1.0"
    updated_contract = OpenDataContractStandard.model_validate(updated_payload)
    contract_client.put(updated_contract)

    versions = contract_client.list_versions(contract.id)
    assert versions == [contract.version, "1.1.0"]

    latest = contract_client.latest(contract.id)
    assert latest is not None and latest.version == "1.1.0"
    stored = contract_client.get(contract.id, "1.1.0")
    assert stored.version == "1.1.0"

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
    fetched = governance_client.get_contract(
        contract_id=contract.id,
        contract_version=contract.version,
    )
    assert fetched.version == contract.version

    latest = governance_client.latest_contract(contract_id=contract.id)
    assert latest is not None and latest.version == contract.version

    versions = governance_client.list_contract_versions(contract_id=contract.id)
    assert contract.version in versions

    expectations = governance_client.describe_expectations(
        contract_id=contract.id,
        contract_version=contract.version,
    )
    assert isinstance(expectations, list)
    assert expectations

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

    matrix = governance_client.get_status_matrix(dataset_id="orders")
    assert matrix
    matrix_entry = next(
        (
            entry
            for entry in matrix
            if entry.contract_id == contract.id
            and entry.contract_version == contract.version
            and entry.dataset_version == "2024-01-01"
        ),
        None,
    )
    assert matrix_entry is not None and matrix_entry.status is not None

    filtered = governance_client.get_status_matrix(
        dataset_id="orders",
        contract_ids=["does-not-exist"],
    )
    assert filtered == ()

    metrics = governance_client.get_metrics(
        dataset_id="orders",
        dataset_version="2024-01-01",
    )
    assert metrics
    assert any(entry.get("metric_key") == "row_count" for entry in metrics)

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
    dataset_records = governance_client.get_dataset_records(dataset_id="orders")
    assert dataset_records
    assert dataset_records[0]["dataset_name"] == "orders"

    read_plan = governance_client.resolve_read_context(
        context=GovernanceReadContext(
            contract=ContractReference(
                contract_id=contract.id,
                contract_version=contract.version,
            ),
            dataset_id="orders",
            dataset_version="2024-02-02",
        )
    )
    assert read_plan.contract_version == contract.version
    read_assessment = governance_client.evaluate_read_plan(
        plan=read_plan,
        validation=ValidationResult(ok=True, status="ok"),
        observations=lambda: payload,
    )
    governance_client.register_read_activity(plan=read_plan, assessment=read_assessment)

    write_plan = governance_client.resolve_write_context(
        context=GovernanceWriteContext(
            contract=ContractReference(
                contract_id=contract.id,
                contract_version=contract.version,
            ),
            output_binding=DataProductOutputBinding(
                data_product="dp.analytics",
                port_name="staging",
            ),
            dataset_id="orders_staging",
            dataset_version="v1",
        )
    )
    write_assessment = governance_client.evaluate_write_plan(
        plan=write_plan,
        validation=ValidationResult(ok=True, status="ok"),
        observations=lambda: payload,
    )
    governance_client.register_write_activity(plan=write_plan, assessment=write_assessment)


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


def test_remote_data_product_client_persists_products(http_clients):
    dp_client: RemoteDataProductServiceClient = http_clients["data_product"]

    product = OpenDataProductStandard(
        id="dp.catalog",
        status="draft",
        version="0.1.0",
        name="Catalog",  # optional metadata to ensure serialization
    )
    dp_client.put(product)

    retrieved = dp_client.get("dp.catalog", "0.1.0")
    assert retrieved.version == "0.1.0"

    versions = dp_client.list_versions("dp.catalog")
    assert versions == ["0.1.0"]

    latest = dp_client.latest("dp.catalog")
    assert latest is not None and latest.version == "0.1.0"


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


def test_lineage_event_encode_decode_round_trip() -> None:
    pytest.importorskip(
        "openlineage.client.run", reason="openlineage-python is required for lineage client tests"
    )
    payload = {
        "eventType": "COMPLETE",
        "eventTime": "2024-01-01T00:00:00Z",
        "producer": "https://dc43.example/producer",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#",
        "run": {"runId": "44695653-fc1a-4ec6-8c2a-6c6a44ec5ad9", "facets": {"custom": {"value": "x"}}},
        "job": {"namespace": "dc43", "name": "orders"},
        "inputs": [
            {
                "namespace": "dc43",
                "name": "orders",
                "facets": {"dc43Dataset": {"datasetId": "orders", "datasetVersion": "v1"}},
            }
        ],
        "outputs": [],
    }
    event = decode_lineage_event(payload)
    assert event is not None
    encoded = encode_lineage_event(event)
    assert encoded == payload


def test_remote_governance_client_publishes_lineage(http_clients) -> None:
    pytest.importorskip(
        "openlineage.client.run", reason="openlineage-python is required for lineage client tests"
    )
    governance_client: RemoteGovernanceServiceClient = http_clients["governance"]
    backend: _ServiceBackendMock = http_clients["backend"]

    run_id = "44695653-fc1a-4ec6-8c2a-6c6a44ec5ad9"
    lineage_payload = {
        "eventType": "COMPLETE",
        "eventTime": "2024-01-02T12:00:00Z",
        "producer": "https://dc43.example/integrations",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#",
        "run": {"runId": run_id, "facets": {"dc43PipelineContext": {"context": {"job": "orders"}}}},
        "job": {"namespace": "dc43", "name": "orders-job"},
        "inputs": [
            {
                "namespace": "dc43",
                "name": "orders-dataset",
                "facets": {
                    "dc43Dataset": {
                        "datasetId": "orders",
                        "datasetVersion": "2024-01-02",
                        "operation": "read",
                    }
                },
            }
        ],
        "outputs": [],
    }

    event = decode_lineage_event(lineage_payload)
    assert event is not None

    governance_client.publish_lineage_event(event=event)

    assert backend._lineage_events, "lineage event should be forwarded to backend"
    stored = backend._lineage_events[-1]
    assert stored.get("run", {}).get("runId") == run_id
