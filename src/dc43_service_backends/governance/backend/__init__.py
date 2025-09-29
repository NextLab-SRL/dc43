"""Backend contracts and stubs for governance services."""

from .interface import GovernanceServiceBackend
from .local import LocalGovernanceServiceBackend

__all__ = ["GovernanceServiceBackend", "LocalGovernanceServiceBackend"]
