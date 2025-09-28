"""Governance orchestration service abstractions and local stubs."""

from .backend import GovernanceServiceBackend, LocalGovernanceServiceBackend
from .client import (
    GovernanceServiceClient,
    LocalGovernanceServiceClient,
    build_local_governance_service,
)
from .models import (
    GovernanceCredentials,
    PipelineContext,
    PipelineContextSpec,
    QualityAssessment,
    QualityDraftContext,
    normalise_pipeline_context,
)

LocalGovernanceService = LocalGovernanceServiceBackend

__all__ = [
    "GovernanceServiceBackend",
    "LocalGovernanceServiceBackend",
    "GovernanceServiceClient",
    "GovernanceCredentials",
    "PipelineContext",
    "PipelineContextSpec",
    "QualityAssessment",
    "QualityDraftContext",
    "normalise_pipeline_context",
    "LocalGovernanceServiceClient",
    "LocalGovernanceService",
    "build_local_governance_service",
]
