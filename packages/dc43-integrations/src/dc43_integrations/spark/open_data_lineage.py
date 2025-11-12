from __future__ import annotations

"""Helpers for producing Open Data Lineage run events."""

from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Mapping, MutableMapping, Optional, Sequence, Union
from uuid import NAMESPACE_DNS, UUID, uuid4, uuid5

from dc43_service_clients.data_products import (
    DataProductInputBinding,
    DataProductOutputBinding,
)
from dc43_service_clients.data_quality import ValidationResult
from dc43_service_clients.governance import PipelineContext, normalise_pipeline_context
from dc43_service_clients.governance.models import ResolvedReadPlan, ResolvedWritePlan

PipelineContextLike = Union[
    PipelineContext,
    Mapping[str, object],
    Sequence[tuple[str, object]],
    str,
]

DEFAULT_PRODUCER = "https://github.com/datacontractsolutions/dc43-integrations"
DEFAULT_SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json#"

_LineagePlan = Union[ResolvedReadPlan, ResolvedWritePlan]
_Binding = Union[DataProductInputBinding, DataProductOutputBinding]


def _normalise_value(value: Any) -> Any:
    if isinstance(value, ValidationResult):
        return _serialise_validation(value)
    if is_dataclass(value):
        return {key: _normalise_value(item) for key, item in asdict(value).items()}
    if isinstance(value, Mapping):
        return {str(key): _normalise_value(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_normalise_value(item) for item in value]
    return value


def _drop_empty(value: Any) -> Any:
    if isinstance(value, Mapping):
        cleaned: MutableMapping[str, Any] = {}
        for key, item in value.items():
            normalised = _drop_empty(item)
            if normalised is not None:
                cleaned[str(key)] = normalised
        return dict(cleaned) or None
    if isinstance(value, list):
        cleaned_list = [item for item in (_drop_empty(item) for item in value) if item is not None]
        return cleaned_list or None
    if isinstance(value, tuple):
        cleaned_tuple = tuple(
            item for item in (_drop_empty(item) for item in value) if item is not None
        )
        return cleaned_tuple or None
    return value if value is not None else None


def _serialise_validation(validation: ValidationResult) -> Mapping[str, Any]:
    payload = {
        "ok": bool(validation.ok),
        "status": validation.status,
        "reason": validation.reason,
        "errors": list(validation.errors or []),
        "warnings": list(validation.warnings or []),
        "metrics": _normalise_value(validation.metrics),
        "schema": _normalise_value(validation.schema),
        "details": _normalise_value(validation.details),
    }
    return _drop_empty(payload) or {}


def _merge_contexts(
    plan: Optional[_LineagePlan],
    extra: Optional[PipelineContextLike],
) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    plan_ctx = normalise_pipeline_context(getattr(plan, "pipeline_context", None))
    extra_ctx = normalise_pipeline_context(extra)
    if plan_ctx:
        merged.update(plan_ctx)
    if extra_ctx:
        merged.update(extra_ctx)
    return merged


def _binding_to_dict(binding: Optional[_Binding]) -> Optional[Mapping[str, Any]]:
    if binding is None:
        return None
    data: dict[str, Any] = {
        "dataProduct": getattr(binding, "data_product", None),
        "portName": getattr(binding, "port_name", None),
        "bump": getattr(binding, "bump", None),
        "customProperties": _normalise_value(getattr(binding, "custom_properties", None)),
        "dataProductVersion": getattr(binding, "data_product_version", None),
    }
    if isinstance(binding, DataProductInputBinding):
        data.update(
            {
                "sourceDataProduct": getattr(binding, "source_data_product", None),
                "sourceOutputPort": getattr(binding, "source_output_port", None),
                "sourceDataProductVersion": getattr(binding, "source_data_product_version", None),
                "sourceContractVersion": getattr(binding, "source_contract_version", None),
            }
        )
    return _drop_empty(data)


def _resolve_binding(
    plan: Optional[_LineagePlan],
    binding: Optional[_Binding],
) -> Optional[_Binding]:
    if binding is not None:
        return binding
    if isinstance(plan, ResolvedReadPlan):
        return plan.input_binding
    if isinstance(plan, ResolvedWritePlan):
        return plan.output_binding
    return None


def _resolve_dataset_id(
    plan: Optional[_LineagePlan],
    dataset_id: Optional[str],
) -> Optional[str]:
    if dataset_id:
        return dataset_id
    if plan is not None:
        return getattr(plan, "dataset_id", None)
    return None


def _resolve_dataset_version(
    plan: Optional[_LineagePlan],
    dataset_version: Optional[str],
) -> Optional[str]:
    if dataset_version:
        return dataset_version
    if plan is not None:
        return getattr(plan, "dataset_version", None)
    return None


def _resolve_format(plan: Optional[_LineagePlan], dataset_format: Optional[str]) -> Optional[str]:
    if dataset_format:
        return dataset_format
    if plan is not None:
        return getattr(plan, "dataset_format", None)
    return None


def _resolve_contract_id(plan: Optional[_LineagePlan], contract_id: Optional[str]) -> Optional[str]:
    if contract_id:
        return contract_id
    if plan is not None:
        return getattr(plan, "contract_id", None)
    return None


def _resolve_contract_version(
    plan: Optional[_LineagePlan],
    contract_version: Optional[str],
) -> Optional[str]:
    if contract_version:
        return contract_version
    if plan is not None:
        return getattr(plan, "contract_version", None)
    return None


def _resolve_expectation_plan(
    plan_expectations: Optional[Sequence[Mapping[str, Any]]],
    validation: Optional[ValidationResult],
) -> Optional[Sequence[Mapping[str, Any]]]:
    if plan_expectations:
        return list(plan_expectations)
    if validation is None:
        return None
    details = validation.details
    plan = details.get("expectation_plan")
    if isinstance(plan, Sequence):
        return [item for item in plan if isinstance(item, Mapping)]
    return None


def _extract_metrics(
    metrics: Optional[Mapping[str, Any]],
    validation: Optional[ValidationResult],
) -> Optional[Mapping[str, Any]]:
    if metrics:
        return _drop_empty(_normalise_value(metrics))
    if validation is None:
        return None
    details_metrics = validation.details.get("metrics")
    if isinstance(details_metrics, Mapping) and details_metrics:
        return _drop_empty(_normalise_value(details_metrics))
    if validation.metrics:
        return _drop_empty(_normalise_value(validation.metrics))
    return None


def _extract_schema(validation: Optional[ValidationResult]) -> Optional[Mapping[str, Any]]:
    if validation is None:
        return None
    details_schema = validation.details.get("schema")
    if isinstance(details_schema, Mapping) and details_schema:
        return _drop_empty(_normalise_value(details_schema))
    if validation.schema:
        return _drop_empty(_normalise_value(validation.schema))
    return None


def _resolve_job_identity(context: Mapping[str, Any], contract_id: Optional[str]) -> tuple[str, str]:
    namespace = context.get("job_namespace") or context.get("namespace") or "dc43"
    name = (
        context.get("job_name")
        or context.get("job")
        or contract_id
        or context.get("asset")
        or "dc43-job"
    )
    return str(namespace), str(name)


def _resolve_run_id(context: Mapping[str, Any]) -> str:
    candidates = (
        context.get("run_id"),
        context.get("runId"),
        context.get("execution_id"),
        context.get("executionId"),
    )
    for value in candidates:
        if isinstance(value, str) and value.strip():
            return _ensure_uuid(value)
    return str(uuid4())


def _ensure_uuid(value: str) -> str:
    text = value.strip()
    try:
        return str(UUID(text))
    except (ValueError, AttributeError):
        return str(uuid5(NAMESPACE_DNS, text))


if TYPE_CHECKING:  # pragma: no cover - import used for static analysis only
    from openlineage.client.run import Dataset, Job, Run, RunEvent, RunState


@lru_cache(maxsize=1)
def _openlineage_models():
    from openlineage.client.run import Dataset, Job, Run, RunEvent, RunState
    return Dataset, Job, Run, RunEvent, RunState


def build_lineage_run_event(
    *,
    operation: str,
    plan: Optional[_LineagePlan],
    pipeline_context: Optional[PipelineContextLike],
    contract_id: Optional[str],
    contract_version: Optional[str],
    dataset_id: Optional[str],
    dataset_version: Optional[str],
    dataset_format: Optional[str] = None,
    table: Optional[str] = None,
    path: Optional[str] = None,
    binding: Optional[_Binding] = None,
    validation: Optional[ValidationResult] = None,
    status: Optional[ValidationResult] = None,
    expectation_plan: Optional[Sequence[Mapping[str, Any]]] = None,
    metrics: Optional[Mapping[str, Any]] = None,
    producer: str = DEFAULT_PRODUCER,
    schema_url: str = DEFAULT_SCHEMA_URL,
) -> "RunEvent":
    Dataset, Job, Run, RunEvent, RunState = _openlineage_models()
    operation = operation.lower()
    context = _merge_contexts(plan, pipeline_context)
    resolved_contract_id = _resolve_contract_id(plan, contract_id)
    resolved_contract_version = _resolve_contract_version(plan, contract_version)
    resolved_dataset_id = _resolve_dataset_id(plan, dataset_id)
    resolved_dataset_version = _resolve_dataset_version(plan, dataset_version)
    resolved_format = _resolve_format(plan, dataset_format)
    resolved_binding = _resolve_binding(plan, binding)
    resolved_expectations = _resolve_expectation_plan(expectation_plan, validation)
    resolved_metrics = _extract_metrics(metrics, validation)
    resolved_schema = _extract_schema(validation)
    namespace, job_name = _resolve_job_identity(context, resolved_contract_id)
    run_id = _resolve_run_id(context)

    job_facets: dict[str, Any] = {}
    dataset_facets: dict[str, Any] = {}
    run_facets: dict[str, Any] = {}

    if resolved_contract_id:
        dataset_facets["dc43Contract"] = _drop_empty(
            {
                "contractId": resolved_contract_id,
                "contractVersion": resolved_contract_version,
            }
        )
    if resolved_binding is not None:
        dataset_facets["dc43DataProduct"] = _binding_to_dict(resolved_binding)
    if resolved_dataset_version:
        dataset_facets["version"] = {"datasetVersion": resolved_dataset_version}
    if resolved_format:
        dataset_facets["dc43Format"] = {"format": resolved_format}
    dataset_facets["dc43Dataset"] = _drop_empty(
        {
            "datasetId": resolved_dataset_id,
            "datasetVersion": resolved_dataset_version,
            "operation": operation,
        }
    )
    if table or path:
        dataset_facets["dc43DatasetRef"] = _drop_empty({"table": table, "path": path})
    if resolved_metrics:
        dataset_facets.setdefault("dc43DataQuality", {})["metrics"] = resolved_metrics
    if resolved_schema:
        dataset_facets.setdefault("dc43DataQuality", {}).setdefault("schema", resolved_schema)
    if resolved_expectations:
        dataset_facets["dc43Expectations"] = {"plan": _normalise_value(resolved_expectations)}

    if context:
        run_facets["dc43PipelineContext"] = {"context": _drop_empty(context)}
    validation_result = status or validation
    if validation_result is not None:
        run_facets["dc43Validation"] = _serialise_validation(validation_result)

    dataset_namespace = context.get("dataset_namespace") or namespace
    dataset_name = resolved_dataset_id or job_name
    dataset_entry = Dataset(
        namespace=str(dataset_namespace),
        name=str(dataset_name),
        facets=_drop_empty(dataset_facets) or {},
    )

    event = RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=run_id, facets=_drop_empty(run_facets) or {}),
        job=Job(namespace=str(namespace), name=str(job_name), facets=_drop_empty(job_facets) or {}),
        producer=producer,
        inputs=[dataset_entry] if operation == "read" else [],
        outputs=[dataset_entry] if operation == "write" else [],
        schemaURL=schema_url,
    )
    return event


__all__ = ["build_lineage_run_event"]
