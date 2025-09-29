"""Client interfaces and local helpers for governance services."""

from .interface import GovernanceServiceClient
from .local import LocalGovernanceServiceClient, build_local_governance_service
from .remote import RemoteGovernanceServiceClient

__all__ = [
    "GovernanceServiceClient",
    "LocalGovernanceServiceClient",
    "RemoteGovernanceServiceClient",
    "build_local_governance_service",
]
