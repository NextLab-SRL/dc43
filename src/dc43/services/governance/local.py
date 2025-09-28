"""Compatibility facade exposing local governance helpers."""

from .backend import LocalGovernanceServiceBackend as LocalGovernanceService
from .client import LocalGovernanceServiceClient, build_local_governance_service

__all__ = [
    "LocalGovernanceService",
    "LocalGovernanceServiceClient",
    "build_local_governance_service",
]
