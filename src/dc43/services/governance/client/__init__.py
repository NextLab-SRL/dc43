"""Client interfaces and local helpers for governance services."""

from .interface import GovernanceServiceClient
from .local import LocalGovernanceServiceClient, build_local_governance_service

__all__ = [
    "GovernanceServiceClient",
    "LocalGovernanceServiceClient",
    "build_local_governance_service",
]
