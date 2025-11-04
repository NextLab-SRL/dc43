"""Serialisation helpers shared by governance clients and servers."""

from __future__ import annotations

from typing import Any, Mapping, Optional, Sequence

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43_service_clients.data_quality.transport import (
    decode_validation_result,
    encode_validation_result,
)
from dc43_service_clients.data_products.models import (
    DataProductInputBinding,
    DataProductOutputBinding,
    normalise_input_binding,
    normalise_output_binding,
)

from .models import (
    ContractReference,
    GovernanceCredentials,
    GovernanceReadContext,
    GovernanceWriteContext,
    PipelineContextSpec,
    QualityAssessment,
    QualityDraftContext,
    ResolvedReadPlan,
    ResolvedWritePlan,
    merge_pipeline_context,
    normalise_pipeline_context,
)


def _coerce_optional_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "y"}:
            return True
        if lowered in {"false", "0", "no", "n"}:
            return False
        if not lowered:
            return None
    if isinstance(value, (int, float)):
        return bool(value)
    return bool(value)


def _decode_allowed_statuses(raw: Mapping[str, Any]) -> Optional[tuple[str, ...]]:
    allowed_statuses_raw = raw.get("allowed_data_product_statuses") or raw.get(
        "allowedDataProductStatuses"
    )
    if isinstance(allowed_statuses_raw, str):
        values = [value.strip() for value in allowed_statuses_raw.split(",")]
        filtered = tuple(filter(None, values))
        return filtered or None
    if isinstance(allowed_statuses_raw, Sequence):
        prepared = [
            str(value).strip()
            for value in allowed_statuses_raw
            if str(value).strip()
        ]
        result = tuple(prepared)
        return result or None
    return None


def _extract_status_policy_fields(
    raw: Mapping[str, Any],
) -> tuple[Any, Any, Any, Any]:
    allow_missing_raw = raw.get("allow_missing_data_product_status")
    if allow_missing_raw is None:
        allow_missing_raw = raw.get("allowMissingDataProductStatus")
    case_insensitive_raw = raw.get("data_product_status_case_insensitive")
    if case_insensitive_raw is None:
        case_insensitive_raw = raw.get("dataProductStatusCaseInsensitive")
    failure_message_raw = raw.get("data_product_status_failure_message")
    if failure_message_raw is None:
        failure_message_raw = raw.get("dataProductStatusFailureMessage")
    enforce_raw = raw.get("enforce_data_product_status")
    if enforce_raw is None:
        enforce_raw = raw.get("enforceDataProductStatus")
    return allow_missing_raw, case_insensitive_raw, failure_message_raw, enforce_raw


def encode_credentials(credentials: GovernanceCredentials | None) -> dict[str, Any] | None:
    if credentials is None:
        return None
    payload: dict[str, Any] = {}
    if credentials.token is not None:
        payload["token"] = credentials.token
    if credentials.headers is not None:
        payload["headers"] = dict(credentials.headers)
    if credentials.extra is not None:
        payload.update(dict(credentials.extra))
    return payload


def decode_credentials(raw: Mapping[str, Any] | None) -> GovernanceCredentials | None:
    if raw is None:
        return None
    token = raw.get("token")
    headers = raw.get("headers")
    extra = {key: value for key, value in raw.items() if key not in {"token", "headers"}}
    return GovernanceCredentials(
        token=str(token) if token is not None else None,
        headers=dict(headers) if isinstance(headers, Mapping) else None,
        extra=extra or None,
    )


def encode_contract_reference(reference: ContractReference | None) -> dict[str, Any] | None:
    if reference is None:
        return None
    return {
        "contract_id": reference.contract_id,
        "contract_version": reference.contract_version,
        "version_selector": reference.version_selector,
    }


def decode_contract_reference(raw: Mapping[str, Any] | None) -> ContractReference | None:
    if raw is None:
        return None
    contract_id = raw.get("contract_id") or raw.get("contractId")
    if not contract_id:
        return None
    return ContractReference(
        contract_id=str(contract_id),
        contract_version=str(raw.get("contract_version") or raw.get("contractVersion") or "").strip()
        or None,
        version_selector=str(raw.get("version_selector") or raw.get("versionSelector") or "").strip()
        or None,
    )


def encode_input_binding(binding: DataProductInputBinding | None) -> dict[str, Any] | None:
    if binding is None:
        return None
    payload: dict[str, Any] = {
        "data_product": binding.data_product,
        "port_name": binding.port_name,
        "data_product_version": binding.data_product_version,
        "source_data_product": binding.source_data_product,
        "source_data_product_version": binding.source_data_product_version,
        "source_output_port": binding.source_output_port,
        "source_contract_version": binding.source_contract_version,
        "bump": binding.bump,
    }
    if binding.custom_properties is not None:
        payload["custom_properties"] = dict(binding.custom_properties)
    return payload


def decode_input_binding(raw: Mapping[str, Any] | None) -> DataProductInputBinding | None:
    if raw is None:
        return None
    return normalise_input_binding(raw)


def encode_output_binding(binding: DataProductOutputBinding | None) -> dict[str, Any] | None:
    if binding is None:
        return None
    payload: dict[str, Any] = {
        "data_product": binding.data_product,
        "port_name": binding.port_name,
        "data_product_version": binding.data_product_version,
        "bump": binding.bump,
    }
    if binding.custom_properties is not None:
        payload["custom_properties"] = dict(binding.custom_properties)
    return payload


def decode_output_binding(raw: Mapping[str, Any] | None) -> DataProductOutputBinding | None:
    if raw is None:
        return None
    return normalise_output_binding(raw)


def encode_draft_context(context: QualityDraftContext | None) -> dict[str, Any] | None:
    if context is None:
        return None
    payload: dict[str, Any] = {
        "dataset_id": context.dataset_id,
        "dataset_version": context.dataset_version,
        "data_format": context.data_format,
        "dq_feedback": dict(context.dq_feedback) if isinstance(context.dq_feedback, Mapping) else context.dq_feedback,
        "draft_context": dict(context.draft_context) if isinstance(context.draft_context, Mapping) else context.draft_context,
        "pipeline_context": dict(context.pipeline_context)
        if isinstance(context.pipeline_context, Mapping)
        else context.pipeline_context,
    }
    return payload


def decode_draft_context(raw: Mapping[str, Any] | None) -> QualityDraftContext | None:
    if raw is None:
        return None
    dq_feedback = raw.get("dq_feedback")
    draft_context = raw.get("draft_context")
    pipeline_context = raw.get("pipeline_context")
    return QualityDraftContext(
        dataset_id=raw.get("dataset_id"),
        dataset_version=raw.get("dataset_version"),
        data_format=raw.get("data_format"),
        dq_feedback=dict(dq_feedback) if isinstance(dq_feedback, Mapping) else dq_feedback,
        draft_context=dict(draft_context) if isinstance(draft_context, Mapping) else draft_context,
        pipeline_context=dict(pipeline_context) if isinstance(pipeline_context, Mapping) else pipeline_context,
    )


def encode_pipeline_context(context: PipelineContextSpec | None) -> Mapping[str, Any] | None:
    resolved = normalise_pipeline_context(context)
    return resolved or None


def decode_pipeline_context(raw: Mapping[str, Any] | Sequence[tuple[str, Any]] | str | None) -> Optional[dict[str, Any]]:
    if raw is None:
        return None
    if isinstance(raw, str):
        return normalise_pipeline_context(raw)
    if isinstance(raw, Sequence):
        return normalise_pipeline_context(raw)
    if isinstance(raw, Mapping):
        return dict(raw)
    return None


def encode_read_context(context: GovernanceReadContext) -> dict[str, Any]:
    return {
        "contract": encode_contract_reference(context.contract),
        "input_binding": encode_input_binding(context.input_binding),
        "dataset_id": context.dataset_id,
        "dataset_version": context.dataset_version,
        "dataset_format": context.dataset_format,
        "pipeline_context": encode_pipeline_context(context.pipeline_context),
        "bump": context.bump,
        "draft_on_violation": context.draft_on_violation,
        "allowed_data_product_statuses": list(context.allowed_data_product_statuses)
        if context.allowed_data_product_statuses is not None
        else None,
        "allow_missing_data_product_status": context.allow_missing_data_product_status,
        "data_product_status_case_insensitive": context.data_product_status_case_insensitive,
        "data_product_status_failure_message": context.data_product_status_failure_message,
        "enforce_data_product_status": context.enforce_data_product_status,
    }


def decode_read_context(raw: Mapping[str, Any]) -> GovernanceReadContext:
    allowed_statuses = _decode_allowed_statuses(raw)
    (
        allow_missing_raw,
        case_insensitive_raw,
        failure_message_raw,
        enforce_raw,
    ) = _extract_status_policy_fields(raw)
    return GovernanceReadContext(
        contract=decode_contract_reference(raw.get("contract")),
        input_binding=decode_input_binding(raw.get("input_binding")),
        dataset_id=raw.get("dataset_id"),
        dataset_version=raw.get("dataset_version"),
        dataset_format=raw.get("dataset_format"),
        pipeline_context=raw.get("pipeline_context"),
        bump=str(raw.get("bump") or "minor"),
        draft_on_violation=bool(raw.get("draft_on_violation", False)),
        allowed_data_product_statuses=allowed_statuses,
        allow_missing_data_product_status=_coerce_optional_bool(allow_missing_raw),
        data_product_status_case_insensitive=_coerce_optional_bool(
            case_insensitive_raw
        ),
        data_product_status_failure_message=str(failure_message_raw)
        if failure_message_raw is not None
        else None,
        enforce_data_product_status=_coerce_optional_bool(enforce_raw),
    )


def encode_write_context(context: GovernanceWriteContext) -> dict[str, Any]:
    return {
        "contract": encode_contract_reference(context.contract),
        "output_binding": encode_output_binding(context.output_binding),
        "dataset_id": context.dataset_id,
        "dataset_version": context.dataset_version,
        "dataset_format": context.dataset_format,
        "pipeline_context": encode_pipeline_context(context.pipeline_context),
        "bump": context.bump,
        "draft_on_violation": context.draft_on_violation,
        "allowed_data_product_statuses": list(context.allowed_data_product_statuses)
        if context.allowed_data_product_statuses is not None
        else None,
        "allow_missing_data_product_status": context.allow_missing_data_product_status,
        "data_product_status_case_insensitive": context.data_product_status_case_insensitive,
        "data_product_status_failure_message": context.data_product_status_failure_message,
        "enforce_data_product_status": context.enforce_data_product_status,
    }


def decode_write_context(raw: Mapping[str, Any]) -> GovernanceWriteContext:
    allowed_statuses = _decode_allowed_statuses(raw)
    (
        allow_missing_raw,
        case_insensitive_raw,
        failure_message_raw,
        enforce_raw,
    ) = _extract_status_policy_fields(raw)
    return GovernanceWriteContext(
        contract=decode_contract_reference(raw.get("contract")),
        output_binding=decode_output_binding(raw.get("output_binding")),
        dataset_id=raw.get("dataset_id"),
        dataset_version=raw.get("dataset_version"),
        dataset_format=raw.get("dataset_format"),
        pipeline_context=raw.get("pipeline_context"),
        bump=str(raw.get("bump") or "minor"),
        draft_on_violation=bool(raw.get("draft_on_violation", False)),
        allowed_data_product_statuses=allowed_statuses,
        allow_missing_data_product_status=_coerce_optional_bool(allow_missing_raw),
        data_product_status_case_insensitive=_coerce_optional_bool(
            case_insensitive_raw
        ),
        data_product_status_failure_message=str(failure_message_raw)
        if failure_message_raw is not None
        else None,
        enforce_data_product_status=_coerce_optional_bool(enforce_raw),
    )


def encode_read_plan(plan: ResolvedReadPlan) -> dict[str, Any]:
    return {
        "contract": encode_contract(plan.contract),
        "contract_id": plan.contract_id,
        "contract_version": plan.contract_version,
        "dataset_id": plan.dataset_id,
        "dataset_version": plan.dataset_version,
        "dataset_format": plan.dataset_format,
        "input_binding": encode_input_binding(plan.input_binding),
        "pipeline_context": dict(plan.pipeline_context) if isinstance(plan.pipeline_context, Mapping) else plan.pipeline_context,
        "bump": plan.bump,
        "draft_on_violation": plan.draft_on_violation,
        "allowed_data_product_statuses": list(plan.allowed_data_product_statuses)
        if plan.allowed_data_product_statuses is not None
        else None,
        "allow_missing_data_product_status": plan.allow_missing_data_product_status,
        "data_product_status_case_insensitive": plan.data_product_status_case_insensitive,
        "data_product_status_failure_message": plan.data_product_status_failure_message,
        "enforce_data_product_status": plan.enforce_data_product_status,
    }


def decode_read_plan(raw: Mapping[str, Any]) -> ResolvedReadPlan:
    contract = decode_contract(raw.get("contract"))
    if contract is None:
        raise ValueError("Resolved plan payload missing contract definition")
    allowed_statuses = _decode_allowed_statuses(raw)
    (
        allow_missing_raw,
        case_insensitive_raw,
        failure_message_raw,
        enforce_raw,
    ) = _extract_status_policy_fields(raw)
    return ResolvedReadPlan(
        contract=contract,
        contract_id=str(raw.get("contract_id")),
        contract_version=str(raw.get("contract_version")),
        dataset_id=str(raw.get("dataset_id")),
        dataset_version=str(raw.get("dataset_version")),
        dataset_format=raw.get("dataset_format"),
        input_binding=decode_input_binding(raw.get("input_binding")),
        pipeline_context=decode_pipeline_context(raw.get("pipeline_context")),
        bump=str(raw.get("bump") or "minor"),
        draft_on_violation=bool(raw.get("draft_on_violation", False)),
        allowed_data_product_statuses=allowed_statuses,
        allow_missing_data_product_status=_coerce_optional_bool(allow_missing_raw),
        data_product_status_case_insensitive=_coerce_optional_bool(
            case_insensitive_raw
        ),
        data_product_status_failure_message=str(failure_message_raw)
        if failure_message_raw is not None
        else None,
        enforce_data_product_status=_coerce_optional_bool(enforce_raw),
    )


def encode_write_plan(plan: ResolvedWritePlan) -> dict[str, Any]:
    return {
        "contract": encode_contract(plan.contract),
        "contract_id": plan.contract_id,
        "contract_version": plan.contract_version,
        "dataset_id": plan.dataset_id,
        "dataset_version": plan.dataset_version,
        "dataset_format": plan.dataset_format,
        "output_binding": encode_output_binding(plan.output_binding),
        "pipeline_context": dict(plan.pipeline_context) if isinstance(plan.pipeline_context, Mapping) else plan.pipeline_context,
        "bump": plan.bump,
        "draft_on_violation": plan.draft_on_violation,
        "allowed_data_product_statuses": list(plan.allowed_data_product_statuses)
        if plan.allowed_data_product_statuses is not None
        else None,
        "allow_missing_data_product_status": plan.allow_missing_data_product_status,
        "data_product_status_case_insensitive": plan.data_product_status_case_insensitive,
        "data_product_status_failure_message": plan.data_product_status_failure_message,
        "enforce_data_product_status": plan.enforce_data_product_status,
    }


def decode_write_plan(raw: Mapping[str, Any]) -> ResolvedWritePlan:
    contract = decode_contract(raw.get("contract"))
    if contract is None:
        raise ValueError("Resolved plan payload missing contract definition")
    allowed_statuses = _decode_allowed_statuses(raw)
    (
        allow_missing_raw,
        case_insensitive_raw,
        failure_message_raw,
        enforce_raw,
    ) = _extract_status_policy_fields(raw)
    return ResolvedWritePlan(
        contract=contract,
        contract_id=str(raw.get("contract_id")),
        contract_version=str(raw.get("contract_version")),
        dataset_id=str(raw.get("dataset_id")),
        dataset_version=str(raw.get("dataset_version")),
        dataset_format=raw.get("dataset_format"),
        output_binding=decode_output_binding(raw.get("output_binding")),
        pipeline_context=decode_pipeline_context(raw.get("pipeline_context")),
        bump=str(raw.get("bump") or "minor"),
        draft_on_violation=bool(raw.get("draft_on_violation", False)),
        allowed_data_product_statuses=allowed_statuses,
        allow_missing_data_product_status=_coerce_optional_bool(allow_missing_raw),
        data_product_status_case_insensitive=_coerce_optional_bool(
            case_insensitive_raw
        ),
        data_product_status_failure_message=str(failure_message_raw)
        if failure_message_raw is not None
        else None,
        enforce_data_product_status=_coerce_optional_bool(enforce_raw),
    )


def encode_quality_assessment(assessment: QualityAssessment) -> dict[str, Any]:
    draft = assessment.draft
    return {
        "status": encode_validation_result(assessment.status),
        "validation": encode_validation_result(assessment.validation),
        "draft": draft.model_dump(by_alias=True, exclude_none=True) if isinstance(draft, OpenDataContractStandard) else None,
        "observations_reused": bool(assessment.observations_reused),
    }


def decode_quality_assessment(raw: Mapping[str, Any]) -> QualityAssessment:
    status = decode_validation_result(raw.get("status"))
    validation = decode_validation_result(raw.get("validation"))
    draft_raw = raw.get("draft")
    draft = None
    if isinstance(draft_raw, Mapping):
        draft = OpenDataContractStandard.model_validate(dict(draft_raw))
    return QualityAssessment(
        status=status,
        validation=validation,
        draft=draft,
        observations_reused=bool(raw.get("observations_reused", False)),
    )


def encode_contract(contract: OpenDataContractStandard | None) -> dict[str, Any] | None:
    if contract is None:
        return None
    return contract.model_dump(by_alias=True, exclude_none=True)


def decode_contract(raw: Mapping[str, Any] | None) -> OpenDataContractStandard | None:
    if raw is None:
        return None
    return OpenDataContractStandard.model_validate(dict(raw))


def merge_pipeline_specs(*values: PipelineContextSpec | None) -> Optional[dict[str, Any]]:
    return merge_pipeline_context(*values)


__all__ = [
    "encode_credentials",
    "decode_credentials",
    "encode_contract_reference",
    "decode_contract_reference",
    "encode_input_binding",
    "decode_input_binding",
    "encode_output_binding",
    "decode_output_binding",
    "encode_draft_context",
    "decode_draft_context",
    "encode_pipeline_context",
    "decode_pipeline_context",
    "encode_read_context",
    "decode_read_context",
    "encode_write_context",
    "decode_write_context",
    "encode_read_plan",
    "decode_read_plan",
    "encode_write_plan",
    "decode_write_plan",
    "encode_quality_assessment",
    "decode_quality_assessment",
    "encode_contract",
    "decode_contract",
    "merge_pipeline_specs",
]
