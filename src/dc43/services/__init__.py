"""Service runtimes and compatibility imports for the dc43 platform.

The package now distinguishes between three layers:

* :mod:`dc43.lib` for pure library helpers that can run without any services.
* :mod:`dc43.clients` for HTTP or in-process client abstractions.
* :mod:`dc43.services` for running service backends or local orchestration.

Existing imports continue to work but new code should prefer the dedicated
sub-packages to make the layering explicit.
"""

from dc43.lib.data_quality import ObservationPayload

from .contracts import ContractServiceClient, LocalContractServiceClient
from .data_quality import DataQualityServiceClient, LocalDataQualityServiceClient
from .governance import (
    GovernanceCredentials,
    GovernanceServiceClient,
    LocalGovernanceService,
    PipelineContext,
    PipelineContextSpec,
    QualityAssessment,
    QualityDraftContext,
    build_local_governance_service,
    normalise_pipeline_context,
)

__all__ = [
    "DataQualityServiceClient",
    "LocalDataQualityServiceClient",
    "ObservationPayload",
    "GovernanceCredentials",
    "GovernanceServiceClient",
    "LocalGovernanceService",
    "PipelineContext",
    "PipelineContextSpec",
    "QualityAssessment",
    "QualityDraftContext",
    "build_local_governance_service",
    "normalise_pipeline_context",
    "ContractServiceClient",
    "LocalContractServiceClient",
]
