"""Backend contracts and stubs for the data-quality service."""

from .interface import DataQualityServiceBackend
from .local import LocalDataQualityServiceBackend

__all__ = ["DataQualityServiceBackend", "LocalDataQualityServiceBackend"]
