"""HTTP application exposing service backend capabilities."""

from __future__ import annotations

from typing import Any, List, Mapping, Optional, Sequence

try:  # pragma: no cover - import guard exercised in packaging contexts
    from fastapi import APIRouter, FastAPI, HTTPException, Query, Response
    from fastapi.responses import RedirectResponse
    from fastapi.encoders import jsonable_encoder
except ModuleNotFoundError as exc:  # pragma: no cover - raised when optional deps missing
    raise ModuleNotFoundError(
        "FastAPI is required to use the HTTP server utilities. Install "
        "'dc43-service-backends[http]' to enable them."
    ) from exc
from pydantic import BaseModel
from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43_service_clients.odps import (
    DataProductInputPort,
    DataProductOutputPort,
    as_odps_dict as as_odps_product_dict,
    to_model as to_data_product_model,
)
from dc43_service_clients.data_quality import ValidationResult
from dc43_service_clients.data_quality.transport import (
    decode_observation_payload,
    decode_validation_result,
    encode_validation_result,
)
from dc43_service_clients.governance.lineage import decode_lineage_event
from dc43_service_clients.governance.transport import (
    decode_contract,
    decode_credentials,
    decode_draft_context,
    decode_quality_assessment,
    decode_read_context,
    decode_read_plan,
    decode_write_context,
    decode_write_plan,
    encode_contract,
    encode_quality_assessment,
    encode_read_plan,
    encode_write_plan,
)
from dc43_service_clients.governance.models import QualityAssessment

from .contracts import ContractServiceBackend
from .data_products import DataProductServiceBackend
from .data_quality import DataQualityServiceBackend
from .governance.backend import GovernanceServiceBackend
from .core.odcs import to_model as to_contract_model


class _LinkDatasetPayload(BaseModel):
    dataset_id: str
    dataset_version: str
    contract_id: str
    contract_version: str


class _DataProductInputPayload(BaseModel):
    port_name: str
    contract_id: str
    contract_version: str
    bump: str = "minor"
    custom_properties: Optional[Mapping[str, Any]] = None
    source_data_product: Optional[str] = None
    source_output_port: Optional[str] = None


class _DataProductOutputPayload(BaseModel):
    port_name: str
    contract_id: str
    contract_version: str
    bump: str = "minor"
    custom_properties: Optional[Mapping[str, Any]] = None


class _EvaluateDQPayload(BaseModel):
    contract: Mapping[str, Any]
    payload: Mapping[str, Any]


class _ExpectationsPayload(BaseModel):
    contract: Mapping[str, Any]


class _GovernanceEvaluatePayload(BaseModel):
    contract_id: str
    contract_version: str
    dataset_id: str
    dataset_version: str
    validation: Optional[Mapping[str, Any]] = None
    observations: Mapping[str, Any]
    bump: str = "minor"
    context: Optional[Mapping[str, Any]] = None
    pipeline_context: Optional[Mapping[str, Any]] = None
    operation: str = "read"
    draft_on_violation: bool = False


class _GovernanceReviewPayload(BaseModel):
    validation: Mapping[str, Any]
    base_contract: Mapping[str, Any]
    bump: str = "minor"
    dataset_id: Optional[str] = None
    dataset_version: Optional[str] = None
    data_format: Optional[str] = None
    dq_status: Optional[Mapping[str, Any]] = None
    dq_feedback: Optional[Mapping[str, Any]] = None
    context: Optional[Mapping[str, Any]] = None
    pipeline_context: Optional[Mapping[str, Any]] = None
    draft_requested: bool = False
    operation: Optional[str] = None


class _GovernanceDraftPayload(BaseModel):
    validation: Mapping[str, Any]
    base_contract: Mapping[str, Any]
    bump: str = "minor"
    context: Optional[Mapping[str, Any]] = None
    pipeline_context: Optional[Mapping[str, Any]] = None


class _AuthPayload(BaseModel):
    credentials: Optional[Mapping[str, Any]] = None


class _GovernanceResolvePayload(BaseModel):
    context: Mapping[str, Any]


class _GovernancePlanEvaluatePayload(BaseModel):
    plan: Mapping[str, Any]
    validation: Optional[Mapping[str, Any]] = None
    observations: Optional[Mapping[str, Any]] = None


class _GovernanceRegisterPayload(BaseModel):
    plan: Mapping[str, Any]
    assessment: Mapping[str, Any]


class _GovernanceLineagePayload(BaseModel):
    event: Mapping[str, Any]


def _encode_assessment(assessment: QualityAssessment) -> Mapping[str, Any]:
    return encode_quality_assessment(assessment)


def build_app(
    *,
    contract_backend: ContractServiceBackend,
    dq_backend: DataQualityServiceBackend,
    governance_backend: GovernanceServiceBackend,
    data_product_backend: DataProductServiceBackend,
    dependencies: Sequence[object] | None = None,
) -> FastAPI:
    """Create a FastAPI app exposing the provided backend implementations."""

    app = FastAPI(title="dc43 service backends")
    router_dependencies = list(dependencies) if dependencies else None
    router = APIRouter(dependencies=router_dependencies)

    @app.get("/", include_in_schema=False)
    def docs_redirect() -> Response:
        """Expose the interactive API documentation at the application root."""

        if app.docs_url:
            return RedirectResponse(url=app.docs_url)
        if app.openapi_url:
            return RedirectResponse(url=app.openapi_url)
        return Response(status_code=204)

    # ------------------------------------------------------------------
    # Contract service endpoints
    # ------------------------------------------------------------------
    @router.put("/contracts/{contract_id}/versions/{contract_version}", status_code=204)
    def put_contract(contract_id: str, contract_version: str, payload: Mapping[str, Any]) -> None:
        try:
            contract = to_contract_model(dict(payload))
        except Exception as exc:  # pragma: no cover - invalid payload handling
            raise HTTPException(status_code=400, detail=f"invalid contract payload: {exc}") from exc

        payload_id = str(contract.id) if contract.id else None
        if payload_id and payload_id != contract_id:
            raise HTTPException(status_code=400, detail="contract.id does not match request path")

        payload_version = str(contract.version) if contract.version else None
        if payload_version and payload_version != contract_version:
            raise HTTPException(status_code=400, detail="contract.version does not match request path")

        contract.id = contract_id
        contract.version = contract_version
        contract_backend.put(contract)

    @router.get("/contracts")
    def list_contracts(
        limit: int | None = Query(None, ge=0),
        offset: int = Query(0, ge=0),
    ) -> Mapping[str, Any]:
        listing = contract_backend.list_contracts(limit=limit, offset=offset)
        return {
            "items": [str(value) for value in listing.items],
            "total": int(listing.total),
            "limit": listing.limit,
            "offset": listing.offset,
        }

    @router.get("/contracts/{contract_id}/versions/{contract_version}")
    def get_contract(contract_id: str, contract_version: str) -> Mapping[str, Any]:
        try:
            contract = contract_backend.get(contract_id, contract_version)
        except FileNotFoundError as exc:  # pragma: no cover - backend signals missing contract
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return contract.model_dump(by_alias=True, exclude_none=True)

    @router.get("/contracts/{contract_id}/latest")
    def latest_contract(contract_id: str) -> Mapping[str, Any]:
        contract = contract_backend.latest(contract_id)
        if contract is None:
            raise HTTPException(status_code=404, detail="contract not found")
        return contract.model_dump(by_alias=True, exclude_none=True)

    @router.get("/contracts/{contract_id}/versions")
    def list_contract_versions(contract_id: str) -> list[str]:
        versions = contract_backend.list_versions(contract_id)
        return [str(value) for value in versions]

    @router.post("/contracts/link")
    def link_contract(payload: _LinkDatasetPayload) -> None:
        contract_backend.link_dataset_contract(
            dataset_id=payload.dataset_id,
            dataset_version=payload.dataset_version,
            contract_id=payload.contract_id,
            contract_version=payload.contract_version,
        )

    @router.get("/contracts/datasets/{dataset_id}/linked")
    def get_linked_contract(dataset_id: str, dataset_version: str | None = None) -> Mapping[str, Any]:
        version = contract_backend.get_linked_contract_version(
            dataset_id=dataset_id,
            dataset_version=dataset_version,
        )
        if version is None:
            raise HTTPException(status_code=404, detail="no contract linked")
        return {"contract_version": version}

    # ------------------------------------------------------------------
    # Data product endpoints
    # ------------------------------------------------------------------
    @router.put("/data-products/{data_product_id}/versions/{version}", status_code=204)
    def put_data_product(data_product_id: str, version: str, payload: Mapping[str, Any]) -> None:
        try:
            product = to_data_product_model(dict(payload))
        except Exception as exc:  # pragma: no cover - invalid payload handling
            raise HTTPException(status_code=400, detail=f"invalid data product payload: {exc}") from exc

        payload_id = str(product.id) if product.id else None
        if payload_id and payload_id != data_product_id:
            raise HTTPException(status_code=400, detail="data_product.id does not match request path")

        payload_version = str(product.version) if product.version else None
        if payload_version and payload_version != version:
            raise HTTPException(status_code=400, detail="data_product.version does not match request path")

        product.id = data_product_id
        product.version = version
        data_product_backend.put(product)

    @router.get("/data-products")
    def list_data_products(
        limit: int | None = Query(None, ge=0),
        offset: int = Query(0, ge=0),
    ) -> Mapping[str, Any]:
        try:
            listing = data_product_backend.list_data_products(limit=limit, offset=offset)
        except NotImplementedError as exc:  # pragma: no cover - backend does not support listings
            raise HTTPException(status_code=501, detail=str(exc)) from exc
        return {
            "items": [str(value) for value in listing.items],
            "total": int(listing.total),
            "limit": listing.limit,
            "offset": listing.offset,
        }

    @router.get("/data-products/{data_product_id}/versions/{version}")
    def get_data_product(data_product_id: str, version: str) -> Mapping[str, Any]:
        try:
            product = data_product_backend.get(data_product_id, version)
        except FileNotFoundError as exc:  # pragma: no cover - backend signals missing product
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return as_odps_product_dict(product)

    @router.get("/data-products/{data_product_id}/latest")
    def latest_data_product(data_product_id: str) -> Mapping[str, Any]:
        product = data_product_backend.latest(data_product_id)
        if product is None:
            raise HTTPException(status_code=404, detail="data product not found")
        return as_odps_product_dict(product)

    @router.get("/data-products/{data_product_id}/versions")
    def list_data_product_versions(data_product_id: str) -> list[str]:
        versions = data_product_backend.list_versions(data_product_id)
        return [str(value) for value in versions]

    @router.post("/data-products/{data_product_id}/input-ports")
    def register_data_product_input(
        data_product_id: str, payload: _DataProductInputPayload
    ) -> Mapping[str, Any]:
        result = data_product_backend.register_input_port(
            data_product_id=data_product_id,
            port=DataProductInputPort(
                name=payload.port_name,
                version=payload.contract_version,
                contract_id=payload.contract_id,
            ),
            bump=payload.bump,
            custom_properties=payload.custom_properties,
            source_data_product=payload.source_data_product,
            source_output_port=payload.source_output_port,
        )
        return {
            "product": as_odps_product_dict(result.product),
            "changed": result.changed,
        }

    @router.post("/data-products/{data_product_id}/output-ports")
    def register_data_product_output(
        data_product_id: str, payload: _DataProductOutputPayload
    ) -> Mapping[str, Any]:
        result = data_product_backend.register_output_port(
            data_product_id=data_product_id,
            port=DataProductOutputPort(
                name=payload.port_name,
                version=payload.contract_version,
                contract_id=payload.contract_id,
            ),
            bump=payload.bump,
            custom_properties=payload.custom_properties,
        )
        return {
            "product": as_odps_product_dict(result.product),
            "changed": result.changed,
        }

    @router.get("/data-products/{data_product_id}/output-ports/{port_name}/contract")
    def resolve_data_product_output_contract(
        data_product_id: str, port_name: str
    ) -> Mapping[str, Any]:
        contract = data_product_backend.resolve_output_contract(
            data_product_id=data_product_id,
            port_name=port_name,
        )
        if contract is None:
            raise HTTPException(status_code=404, detail="output port not found")
        contract_id, contract_version = contract
        return {
            "contract_id": contract_id,
            "contract_version": contract_version,
        }

    # ------------------------------------------------------------------
    # Data-quality endpoints
    # ------------------------------------------------------------------
    @router.post("/data-quality/evaluate")
    def evaluate_quality(payload: _EvaluateDQPayload) -> Mapping[str, Any]:
        contract = OpenDataContractStandard.model_validate(dict(payload.contract))
        observations = decode_observation_payload(payload.payload)
        result = dq_backend.evaluate(contract=contract, payload=observations)
        return encode_validation_result(result) or {}

    @router.post("/data-quality/expectations")
    def describe_expectations(payload: _ExpectationsPayload) -> list[Mapping[str, Any]]:
        contract = OpenDataContractStandard.model_validate(dict(payload.contract))
        descriptors = dq_backend.describe_expectations(contract=contract)
        return [dict(item) for item in descriptors]

    # ------------------------------------------------------------------
    # Governance endpoints
    # ------------------------------------------------------------------
    @router.post("/governance/auth")
    def configure_auth(payload: _AuthPayload) -> None:
        credentials = decode_credentials(payload.credentials)
        governance_backend.configure_auth(credentials)

    @router.post("/governance/read/resolve")
    def resolve_read_context(payload: _GovernanceResolvePayload) -> Mapping[str, Any]:
        context = decode_read_context(payload.context)
        if context is None:
            raise HTTPException(status_code=400, detail="invalid read context")
        plan = governance_backend.resolve_read_context(context=context)
        return encode_read_plan(plan)

    @router.post("/governance/write/resolve")
    def resolve_write_context(payload: _GovernanceResolvePayload) -> Mapping[str, Any]:
        context = decode_write_context(payload.context)
        if context is None:
            raise HTTPException(status_code=400, detail="invalid write context")
        plan = governance_backend.resolve_write_context(context=context)
        return encode_write_plan(plan)

    @router.post("/governance/read/evaluate")
    def evaluate_read_plan(payload: _GovernancePlanEvaluatePayload) -> Mapping[str, Any]:
        try:
            plan = decode_read_plan(payload.plan)
        except ValueError as exc:  # pragma: no cover - invalid payload handling
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        validation = decode_validation_result(payload.validation)
        observations = decode_observation_payload(payload.observations or {})
        assessment = governance_backend.evaluate_read_plan(
            plan=plan,
            validation=validation,
            observations=lambda: observations,
        )
        return _encode_assessment(assessment)

    @router.post("/governance/write/evaluate")
    def evaluate_write_plan(payload: _GovernancePlanEvaluatePayload) -> Mapping[str, Any]:
        try:
            plan = decode_write_plan(payload.plan)
        except ValueError as exc:  # pragma: no cover - invalid payload handling
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        validation = decode_validation_result(payload.validation)
        observations = decode_observation_payload(payload.observations or {})
        assessment = governance_backend.evaluate_write_plan(
            plan=plan,
            validation=validation,
            observations=lambda: observations,
        )
        return _encode_assessment(assessment)

    @router.post("/governance/read/register", status_code=204)
    def register_read_activity(payload: _GovernanceRegisterPayload) -> None:
        try:
            plan = decode_read_plan(payload.plan)
        except ValueError as exc:  # pragma: no cover - invalid payload handling
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        assessment = decode_quality_assessment(payload.assessment)
        governance_backend.register_read_activity(plan=plan, assessment=assessment)

    @router.post("/governance/write/register", status_code=204)
    def register_write_activity(payload: _GovernanceRegisterPayload) -> None:
        try:
            plan = decode_write_plan(payload.plan)
        except ValueError as exc:  # pragma: no cover - invalid payload handling
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        assessment = decode_quality_assessment(payload.assessment)
        governance_backend.register_write_activity(plan=plan, assessment=assessment)

    @router.post("/governance/lineage", status_code=204)
    def publish_lineage(payload: _GovernanceLineagePayload) -> None:
        try:
            event = decode_lineage_event(payload.event)
        except ValueError as exc:  # pragma: no cover - invalid payload handling
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if event is None:
            raise HTTPException(status_code=400, detail="lineage event payload is required")
        governance_backend.publish_lineage_event(event=event)

    @router.post("/governance/evaluate")
    def evaluate_dataset(payload: _GovernanceEvaluatePayload) -> Mapping[str, Any]:
        validation = decode_validation_result(payload.validation)
        observations = decode_observation_payload(payload.observations)
        context = decode_draft_context(payload.context)
        assessment = governance_backend.evaluate_dataset(
            contract_id=payload.contract_id,
            contract_version=payload.contract_version,
            dataset_id=payload.dataset_id,
            dataset_version=payload.dataset_version,
            validation=validation,
            observations=lambda: observations,
            bump=payload.bump,
            context=context,
            pipeline_context=payload.pipeline_context,
            operation=payload.operation,
            draft_on_violation=payload.draft_on_violation,
        )
        return _encode_assessment(assessment)

    @router.post("/governance/review")
    def review_validation(payload: _GovernanceReviewPayload) -> Mapping[str, Any] | None:
        validation = decode_validation_result(payload.validation)
        base_contract = OpenDataContractStandard.model_validate(dict(payload.base_contract))
        dq_status = decode_validation_result(payload.dq_status)
        context = decode_draft_context(payload.context)
        draft = governance_backend.review_validation_outcome(
            validation=validation,
            base_contract=base_contract,
            bump=payload.bump,
            dataset_id=payload.dataset_id,
            dataset_version=payload.dataset_version,
            data_format=payload.data_format,
            dq_status=dq_status,
            dq_feedback=payload.dq_feedback,
            context=context,
            pipeline_context=payload.pipeline_context,
            draft_requested=payload.draft_requested,
            operation=payload.operation,
        )
        return encode_contract(draft)

    @router.post("/governance/draft")
    def propose_draft(payload: _GovernanceDraftPayload) -> Mapping[str, Any]:
        validation = decode_validation_result(payload.validation)
        base_contract = OpenDataContractStandard.model_validate(dict(payload.base_contract))
        context = decode_draft_context(payload.context)
        draft = governance_backend.propose_draft(
            validation=validation,
            base_contract=base_contract,
            bump=payload.bump,
            context=context,
            pipeline_context=payload.pipeline_context,
        )
        return encode_contract(draft) or {}

    @router.get("/governance/status")
    def get_status(
        contract_id: str,
        contract_version: str,
        dataset_id: str,
        dataset_version: str,
    ) -> Mapping[str, Any]:
        status = governance_backend.get_status(
            contract_id=contract_id,
            contract_version=contract_version,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
        )
        if status is None:
            raise HTTPException(status_code=404, detail="status unavailable")
        return encode_validation_result(status) or {}

    @router.get("/governance/status-matrix")
    def get_status_matrix(
        dataset_id: str,
        contract_id: List[str] | None = None,
        dataset_version: List[str] | None = None,
    ) -> Mapping[str, Any]:
        contract_ids: Sequence[str] | None
        if contract_id is None:
            contract_ids = None
        elif isinstance(contract_id, (list, tuple, set)):
            contract_ids = [str(item) for item in contract_id if str(item)]
        else:
            value = str(contract_id)
            contract_ids = [value] if value else None

        dataset_versions: Sequence[str] | None
        if dataset_version is None:
            dataset_versions = None
        elif isinstance(dataset_version, (list, tuple, set)):
            dataset_versions = [str(item) for item in dataset_version if str(item)]
        else:
            value = str(dataset_version)
            dataset_versions = [value] if value else None

        records = governance_backend.get_status_matrix(
            dataset_id=dataset_id,
            contract_ids=contract_ids,
            dataset_versions=dataset_versions,
        )
        entries: List[Mapping[str, Any]] = []
        for record in records:
            if not isinstance(record, Mapping):
                continue
            status_payload = record.get("status")
            encoded = None
            if isinstance(status_payload, ValidationResult):
                encoded = encode_validation_result(status_payload)
            elif isinstance(status_payload, Mapping):
                encoded = dict(status_payload)
            elif status_payload is not None:
                try:
                    encoded = encode_validation_result(status_payload)  # type: ignore[arg-type]
                except Exception:  # pragma: no cover - defensive guard for unexpected payloads
                    encoded = None
            entries.append(
                {
                    "dataset_id": record.get("dataset_id"),
                    "dataset_version": record.get("dataset_version"),
                    "contract_id": record.get("contract_id"),
                    "contract_version": record.get("contract_version"),
                    "status": encoded,
                }
            )
        return {"dataset_id": dataset_id, "entries": entries}

    @router.post("/governance/link", status_code=204)
    def link_dataset(payload: _LinkDatasetPayload) -> None:
        governance_backend.link_dataset_contract(
            dataset_id=payload.dataset_id,
            dataset_version=payload.dataset_version,
            contract_id=payload.contract_id,
            contract_version=payload.contract_version,
        )

    @router.get("/governance/linked")
    def get_link(dataset_id: str, dataset_version: str | None = None) -> Mapping[str, Any]:
        version = governance_backend.get_linked_contract_version(
            dataset_id=dataset_id,
            dataset_version=dataset_version,
        )
        if version is None:
            raise HTTPException(status_code=404, detail="no contract linked")
        return {"contract_version": version}

    @router.get("/governance/metrics")
    def dataset_metrics(
        dataset_id: str,
        dataset_version: str | None = None,
        contract_id: str | None = None,
        contract_version: str | None = None,
    ) -> list[Mapping[str, Any]]:
        records = governance_backend.get_metrics(
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            contract_id=contract_id,
            contract_version=contract_version,
        )
        return list(jsonable_encoder(records))

    @router.get("/governance/datasets")
    def dataset_ids() -> list[str]:
        return list(governance_backend.list_datasets())

    @router.get("/governance/activity")
    def pipeline_activity(
        dataset_id: str,
        dataset_version: str | None = None,
        include_status: bool = False,
    ) -> list[Mapping[str, Any]]:
        records = governance_backend.get_pipeline_activity(
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            include_status=include_status,
        )
        if not include_status:
            return list(jsonable_encoder(records))

        entries: list[Mapping[str, Any]] = []
        for record in records:
            if not isinstance(record, Mapping):
                entries.append(record)
                continue
            entry = dict(record)
            status_payload = entry.get("validation_status")
            encoded_status = None
            if isinstance(status_payload, ValidationResult):
                encoded_status = encode_validation_result(status_payload)
            elif isinstance(status_payload, Mapping):
                encoded_status = dict(status_payload)
            elif status_payload is not None:
                try:
                    encoded_status = encode_validation_result(status_payload)  # type: ignore[arg-type]
                except Exception:  # pragma: no cover - defensive guard
                    encoded_status = None
            if encoded_status is not None:
                entry["validation_status"] = encoded_status
            entries.append(entry)
        return list(jsonable_encoder(entries))

    @router.get("/governance/dataset-records")
    def dataset_records(
        dataset_id: str | None = None,
        dataset_version: str | None = None,
    ) -> list[Mapping[str, Any]]:
        records = governance_backend.get_dataset_records(
            dataset_id=dataset_id,
            dataset_version=dataset_version,
        )
        return [dict(record) for record in records]

    app.include_router(router)
    return app


__all__ = ["build_app"]
