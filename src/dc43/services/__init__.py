"""Service runtimes and compatibility imports for the dc43 platform.

The package now distinguishes between several layers:

* :mod:`dc43.components` for pure library helpers that can run without any
  services.
* :mod:`dc43.integration` for execution-engine adapters.
* :mod:`dc43.clients` for HTTP or in-process client abstractions.
* :mod:`dc43.services` for running service backends or local orchestration.

Existing imports continue to work but new code should prefer the dedicated
sub-packages to make the layering explicit.
"""

from .data_quality import ObservationPayload

from .contracts import (
    ContractServiceBackend,
    ContractServiceClient,
    LocalContractServiceBackend,
    LocalContractServiceClient,
)
from .data_quality import (
    DataQualityServiceBackend,
    DataQualityServiceClient,
    LocalDataQualityServiceBackend,
    LocalDataQualityServiceClient,
)
from .governance import (
    GovernanceServiceBackend,
    GovernanceCredentials,
    GovernanceServiceClient,
    LocalGovernanceServiceBackend,
    LocalGovernanceServiceClient,
    LocalGovernanceService,
    PipelineContext,
    PipelineContextSpec,
    QualityAssessment,
    QualityDraftContext,
    build_local_governance_service,
    normalise_pipeline_context,
)

__all__ = [
    "DataQualityServiceBackend",
    "LocalDataQualityServiceBackend",
    "DataQualityServiceClient",
    "LocalDataQualityServiceClient",
    "ObservationPayload",
    "ContractServiceBackend",
    "LocalContractServiceBackend",
    "GovernanceCredentials",
    "GovernanceServiceBackend",
    "GovernanceServiceClient",
    "LocalGovernanceServiceBackend",
    "LocalGovernanceServiceClient",
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
