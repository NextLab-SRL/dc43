"""Shared models leveraged by governance clients and backends."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple, Union

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43_service_clients.data_quality import ValidationResult
from dc43_service_clients.data_products.models import (
    DataProductInputBinding,
    DataProductOutputBinding,
    normalise_input_binding,
    normalise_output_binding,
)

PipelineContextSpec = Union[
    "PipelineContext",
    Mapping[str, object],
    Sequence[tuple[str, object]],
    str,
]


@dataclass(slots=True)
class GovernanceCredentials:
    """Authentication payload cached by the governance service."""

    token: Optional[str] = None
    headers: Optional[Mapping[str, str]] = None
    extra: Optional[Mapping[str, object]] = None


@dataclass(slots=True)
class PipelineContext:
    """Descriptor describing the pipeline triggering a governance interaction."""

    pipeline: Optional[str] = None
    label: Optional[str] = None
    metadata: Optional[Mapping[str, object]] = None

    def as_dict(self) -> Dict[str, object]:
        payload: Dict[str, object] = {}
        if self.metadata:
            payload.update(self.metadata)
        if self.label:
            payload.setdefault("label", self.label)
        if self.pipeline:
            payload.setdefault("pipeline", self.pipeline)
        return payload


@dataclass(slots=True)
class QualityDraftContext:
    """Context forwarded when proposing a draft to governance."""

    dataset_id: Optional[str]
    dataset_version: Optional[str]
    data_format: Optional[str]
    dq_feedback: Optional[Mapping[str, object]]
    draft_context: Optional[Mapping[str, object]] = None
    pipeline_context: Optional[Mapping[str, object]] = None


@dataclass(slots=True)
class QualityAssessment:
    """Outcome returned after consulting the governance service."""

    status: Optional[ValidationResult]
    validation: Optional[ValidationResult] = None
    draft: Optional[OpenDataContractStandard] = None
    observations_reused: bool = False


@dataclass(slots=True)
class DatasetContractStatus:
    """Snapshot of governance status for a dataset/contract pairing."""

    dataset_id: str
    dataset_version: str
    contract_id: str
    contract_version: str
    status: Optional[ValidationResult] = None


@dataclass(slots=True)
class ContractReference:
    """Describe a contract that should participate in a governance interaction."""

    contract_id: str
    contract_version: Optional[str] = None
    version_selector: Optional[str] = None

    def resolved_version(self) -> Optional[str]:
        """Return an explicit version embedded in the reference when present."""

        if self.contract_version:
            return self.contract_version
        if self.version_selector:
            selector = self.version_selector.strip()
            if selector.lower() in {"latest", "newest"}:
                return None
            if selector.startswith("=="):
                candidate = selector[2:].strip()
                return candidate or None
        return None


@dataclass(slots=True)
class GovernancePolicy:
    """Descriptor for behavioral enforcement and policy rules."""

    allowed_data_product_statuses: Optional[tuple[str, ...]] = None
    allow_missing_data_product_status: Optional[bool] = None
    data_product_status_case_insensitive: Optional[bool] = None
    data_product_status_failure_message: Optional[str] = None
    enforce_data_product_status: Optional[bool] = None
    bump: str = "minor"
    draft_on_violation: bool = False

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any]) -> "GovernancePolicy":
        """Build a policy from a mapping of its attributes."""
        return cls(**{k: v for k, v in mapping.items() if hasattr(cls, k)})


@dataclass(slots=True)
class GovernanceReadContext:
    """Specification forwarded by callers before a governed read."""

    contract: Optional[ContractReference] = None
    input_binding: Optional[DataProductInputBinding] = None
    dataset_id: Optional[str] = None
    dataset_version: Optional[str] = None
    dataset_format: Optional[str] = None
    pipeline_context: Optional[PipelineContextSpec] = None
    policy: Optional[GovernancePolicy] = None

    # Legacy fields for backward compatibility
    allowed_data_product_statuses: Optional[tuple[str, ...]] = None
    allow_missing_data_product_status: Optional[bool] = None
    data_product_status_case_insensitive: Optional[bool] = None
    data_product_status_failure_message: Optional[str] = None
    enforce_data_product_status: Optional[bool] = None

    @classmethod
    def from_port(
        cls,
        product: str,
        port: str,
        product_version: str = "latest",
    ) -> "GovernanceReadContext":
        """Primary entrance for port-priority reading."""
        return cls(
            input_binding=DataProductInputBinding(
                source_data_product=product,
                source_output_port=port,
                source_data_product_version=product_version,
            )
        )

    @classmethod
    def from_contract(cls, id: str, version: str = None) -> "GovernanceReadContext":
        """Fallback entrance for direct contract reading."""
        return cls(contract=ContractReference(contract_id=id, contract_version=version))

    def __post_init__(self) -> None:
        if self.contract is not None and not isinstance(self.contract, ContractReference):
            if isinstance(self.contract, Mapping):
                self.contract = ContractReference(**dict(self.contract))  # type: ignore[arg-type]
            else:
                raise TypeError("contract must be a ContractReference or mapping")
        if self.input_binding is not None and not isinstance(
            self.input_binding, DataProductInputBinding
        ):
            resolved = normalise_input_binding(self.input_binding)  # type: ignore[arg-type]
            if resolved is None:
                raise ValueError("input_binding specification is invalid")
            self.input_binding = resolved
        if self.policy is not None and not isinstance(self.policy, GovernancePolicy):
            if isinstance(self.policy, Mapping):
                self.policy = GovernancePolicy.from_mapping(self.policy)
            else:
                raise TypeError("policy must be a GovernancePolicy or mapping")

        # Migrate legacy fields to policy if policy is None or missing them
        if any(
            v is not None
            for v in (
                self.allowed_data_product_statuses,
                self.allow_missing_data_product_status,
                self.data_product_status_case_insensitive,
                self.data_product_status_failure_message,
                self.enforce_data_product_status,
            )
        ):
            if self.policy is None:
                self.policy = GovernancePolicy()
            if self.policy.allowed_data_product_statuses is None:
                self.policy.allowed_data_product_statuses = self.allowed_data_product_statuses
            if self.policy.allow_missing_data_product_status is None:
                self.policy.allow_missing_data_product_status = self.allow_missing_data_product_status
            if self.policy.data_product_status_case_insensitive is None:
                self.policy.data_product_status_case_insensitive = (
                    self.data_product_status_case_insensitive
                )
            if self.policy.data_product_status_failure_message is None:
                self.policy.data_product_status_failure_message = self.data_product_status_failure_message
            if self.policy.enforce_data_product_status is None:
                self.policy.enforce_data_product_status = self.enforce_data_product_status


@dataclass(slots=True)
class GovernanceWriteContext:
    """Specification describing a governed write interaction."""

    contract: Optional[ContractReference] = None
    output_binding: Optional[DataProductOutputBinding] = None
    dataset_id: Optional[str] = None
    dataset_version: Optional[str] = None
    dataset_format: Optional[str] = None
    pipeline_context: Optional[PipelineContextSpec] = None
    policy: Optional[GovernancePolicy] = None

    # Legacy fields for backward compatibility
    allowed_data_product_statuses: Optional[tuple[str, ...]] = None
    allow_missing_data_product_status: Optional[bool] = None
    data_product_status_case_insensitive: Optional[bool] = None
    data_product_status_failure_message: Optional[str] = None
    enforce_data_product_status: Optional[bool] = None
    bump: Optional[str] = None
    draft_on_violation: Optional[bool] = None

    @classmethod
    def from_port(
        cls,
        product: str,
        port: str,
        product_version: str = "latest",
        bump: str = "minor",
    ) -> "GovernanceWriteContext":
        """Primary entrance for port-priority writing."""
        return cls(
            output_binding=DataProductOutputBinding(
                data_product=product,
                port_name=port,
                data_product_version=product_version,
                bump=bump,
            )
        )

    @classmethod
    def from_contract(cls, id: str, version: str = None) -> "GovernanceWriteContext":
        """Fallback entrance for direct contract writing."""
        return cls(contract=ContractReference(contract_id=id, contract_version=version))

    def __post_init__(self) -> None:
        if self.contract is not None and not isinstance(self.contract, ContractReference):
            if isinstance(self.contract, Mapping):
                self.contract = ContractReference(**dict(self.contract))  # type: ignore[arg-type]
            else:
                raise TypeError("contract must be a ContractReference or mapping")
        if self.output_binding is not None and not isinstance(
            self.output_binding, DataProductOutputBinding
        ):
            resolved = normalise_output_binding(self.output_binding)  # type: ignore[arg-type]
            if resolved is None:
                raise ValueError("output_binding specification is invalid")
            self.output_binding = resolved
        if self.policy is not None and not isinstance(self.policy, GovernancePolicy):
            if isinstance(self.policy, Mapping):
                self.policy = GovernancePolicy.from_mapping(self.policy)
            else:
                raise TypeError("policy must be a GovernancePolicy or mapping")

        # Migrate legacy fields to policy if policy is None or missing them
        if any(
            v is not None
            for v in (
                self.allowed_data_product_statuses,
                self.allow_missing_data_product_status,
                self.data_product_status_case_insensitive,
                self.data_product_status_failure_message,
                self.enforce_data_product_status,
                self.bump,
                self.draft_on_violation,
            )
        ):
            if self.policy is None:
                self.policy = GovernancePolicy()
            if self.policy.allowed_data_product_statuses is None:
                self.policy.allowed_data_product_statuses = self.allowed_data_product_statuses
            if self.policy.allow_missing_data_product_status is None:
                self.policy.allow_missing_data_product_status = self.allow_missing_data_product_status
            if self.policy.data_product_status_case_insensitive is None:
                self.policy.data_product_status_case_insensitive = (
                    self.data_product_status_case_insensitive
                )
            if self.policy.data_product_status_failure_message is None:
                self.policy.data_product_status_failure_message = self.data_product_status_failure_message
            if self.policy.enforce_data_product_status is None:
                self.policy.enforce_data_product_status = self.enforce_data_product_status
            if self.bump is not None:
                self.policy.bump = self.bump
            if self.draft_on_violation is not None:
                self.policy.draft_on_violation = self.draft_on_violation


@dataclass(slots=True)
class ResolvedReadPlan:
    """Outcome returned by governance when resolving read contexts."""

    contract: OpenDataContractStandard
    contract_id: str
    contract_version: str
    dataset_id: str
    dataset_version: str
    dataset_format: Optional[str] = None
    dataset_path: Optional[str] = None
    dataset_table: Optional[str] = None
    input_binding: Optional[DataProductInputBinding] = None
    pipeline_context: Optional[Mapping[str, object]] = None
    policy: Optional[GovernancePolicy] = None


@dataclass(slots=True)
class ResolvedWritePlan:
    """Resolved specification for governed writes."""

    contract: OpenDataContractStandard
    contract_id: str
    contract_version: str
    dataset_id: str
    dataset_version: str
    dataset_format: Optional[str] = None
    dataset_path: Optional[str] = None
    dataset_table: Optional[str] = None
    output_binding: Optional[DataProductOutputBinding] = None
    pipeline_context: Optional[Mapping[str, object]] = None
    policy: Optional[GovernancePolicy] = None


def normalise_pipeline_context(
    context: PipelineContextSpec | Mapping[str, object] | None,
) -> Optional[Dict[str, Any]]:
    if context is None:
        return None
    if isinstance(context, PipelineContext):
        return context.as_dict()
    if isinstance(context, str):
        value = context.strip()
        return {"pipeline": value} if value else None
    if isinstance(context, Mapping):
        return dict(context)
    if isinstance(context, Sequence):
        payload: Dict[str, Any] = {}
        for item in context:
            if isinstance(item, tuple) and len(item) == 2:
                key, value = item
                payload[str(key)] = value
        return payload or None
    return None


def merge_pipeline_context(
    *candidates: PipelineContextSpec | Mapping[str, object] | None,
) -> Optional[Dict[str, Any]]:
    merged: Dict[str, Any] = {}
    for candidate in candidates:
        resolved = normalise_pipeline_context(candidate)
        if resolved:
            merged.update(resolved)
    return merged or None


def merge_draft_context(
    base: Optional[Mapping[str, Any]],
    *,
    dataset_id: Optional[str],
    dataset_version: Optional[str],
    data_format: Optional[str],
    pipeline_context: Optional[Mapping[str, Any]],
) -> Optional[Dict[str, Any]]:
    context: Dict[str, Any] = {}
    if pipeline_context:
        context.update(pipeline_context)
    if base:
        context.update(base)
    pipeline_name = context.get("pipeline")
    if isinstance(pipeline_name, str):
        module, _, function = pipeline_name.rpartition(".")
        if module:
            context.setdefault("module", module)
        if function:
            context.setdefault("function", function)
    if dataset_id and "dataset_id" not in context:
        context["dataset_id"] = dataset_id
    if dataset_version and "dataset_version" not in context:
        context["dataset_version"] = dataset_version
    if data_format and "data_format" not in context:
        context["data_format"] = data_format
    return context or None


def derive_feedback(
    status: ValidationResult | None,
    override: Mapping[str, object] | None,
) -> Optional[Mapping[str, object]]:
    if override is not None:
        return override
    if status is None:
        return None
    payload: Dict[str, object] = dict(status.details)
    payload.setdefault("status", status.status)
    if status.reason:
        payload.setdefault("reason", status.reason)
    return payload or None


def build_quality_context(
    context: QualityDraftContext | None,
    *,
    dataset_id: Optional[str],
    dataset_version: Optional[str],
    data_format: Optional[str],
    dq_feedback: Optional[Mapping[str, object]],
    pipeline_context: Optional[PipelineContextSpec] = None,
) -> QualityDraftContext:
    resolved_dataset_id = context.dataset_id if context and context.dataset_id else dataset_id
    resolved_dataset_version = (
        context.dataset_version if context and context.dataset_version else dataset_version
    )
    resolved_data_format = context.data_format if context and context.data_format else data_format
    resolved_feedback = context.dq_feedback if context and context.dq_feedback else dq_feedback

    pipeline_metadata = merge_pipeline_context(
        context.pipeline_context if context else None,
        pipeline_context,
    )

    merged_context = merge_draft_context(
        context.draft_context if context else None,
        dataset_id=resolved_dataset_id,
        dataset_version=resolved_dataset_version,
        data_format=resolved_data_format,
        pipeline_context=pipeline_metadata,
    )

    return QualityDraftContext(
        dataset_id=resolved_dataset_id,
        dataset_version=resolved_dataset_version,
        data_format=resolved_data_format,
        dq_feedback=resolved_feedback,
        draft_context=merged_context,
        pipeline_context=pipeline_metadata,
    )


__all__ = [
    "GovernanceCredentials",
    "PipelineContext",
    "PipelineContextSpec",
    "QualityDraftContext",
    "QualityAssessment",
    "DatasetContractStatus",
    "build_quality_context",
    "derive_feedback",
    "merge_draft_context",
    "merge_pipeline_context",
    "normalise_pipeline_context",
    "ContractReference",
    "GovernanceReadContext",
    "GovernanceWriteContext",
    "GovernancePolicy",
    "ResolvedReadPlan",
    "ResolvedWritePlan",
]
