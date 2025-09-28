"""Compatibility facade exposing local governance helpers."""

from dc43_service_backends.governance import (  # noqa: F401 re-export
    LocalGovernanceServiceBackend as LocalGovernanceService,
)
from dc43_service_clients.governance import (
    LocalGovernanceServiceClient,
    build_local_governance_service,
)

__all__ = [
    "LocalGovernanceService",
    "LocalGovernanceServiceClient",
    "build_local_governance_service",
]
