"""Backwards-compatible exports for the governance orchestration service."""

from dc43.services.contracts import ContractServiceClient as ContractManagerClient
from dc43.services.contracts import LocalContractServiceClient as LocalContractManager
from dc43.services.governance.client import GovernanceServiceClient
from dc43.services.governance.local import build_local_governance_service
from dc43.services.governance.models import (
    GovernanceCredentials,
    PipelineContext,
    PipelineContextSpec,
    QualityAssessment,
    QualityDraftContext,
    normalise_pipeline_context,
)

__all__ = [
    "ContractManagerClient",
    "GovernanceCredentials",
    "GovernanceServiceClient",
    "LocalContractManager",
    "PipelineContext",
    "PipelineContextSpec",
    "QualityAssessment",
    "QualityDraftContext",
    "build_local_governance_service",
    "normalise_pipeline_context",
]
