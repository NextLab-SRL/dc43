"""Serialisation helpers for governance service payloads."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Optional

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43_service_clients.data_quality.transport import encode_validation_result, decode_validation_result
from .models import (
    GovernanceCredentials,
    PipelineContextSpec,
    QualityAssessment,
    QualityDraftContext,
    merge_pipeline_context,
    normalise_pipeline_context,
)


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


def encode_draft_context(context: QualityDraftContext | None) -> dict[str, Any] | None:
    if context is None:
        return None
    payload: dict[str, Any] = {
        "dataset_id": context.dataset_id,
        "dataset_version": context.dataset_version,
        "data_format": context.data_format,
        "dq_feedback": dict(context.dq_feedback) if isinstance(context.dq_feedback, Mapping) else context.dq_feedback,
        "draft_context": dict(context.draft_context) if isinstance(context.draft_context, Mapping) else context.draft_context,
        "pipeline_context": dict(context.pipeline_context) if isinstance(context.pipeline_context, Mapping) else context.pipeline_context,
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


def encode_quality_assessment(assessment: QualityAssessment) -> dict[str, Any]:
    draft = assessment.draft
    return {
        "status": encode_validation_result(assessment.status),
        "draft": draft.model_dump(by_alias=True, exclude_none=True) if isinstance(draft, OpenDataContractStandard) else None,
        "observations_reused": bool(assessment.observations_reused),
    }


def decode_quality_assessment(raw: Mapping[str, Any]) -> QualityAssessment:
    status = decode_validation_result(raw.get("status"))
    draft_raw = raw.get("draft")
    draft = None
    if isinstance(draft_raw, Mapping):
        draft = OpenDataContractStandard.model_validate(dict(draft_raw))
    return QualityAssessment(
        status=status,
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
    "encode_draft_context",
    "decode_draft_context",
    "encode_pipeline_context",
    "decode_pipeline_context",
    "encode_quality_assessment",
    "decode_quality_assessment",
    "encode_contract",
    "decode_contract",
    "merge_pipeline_specs",
]
