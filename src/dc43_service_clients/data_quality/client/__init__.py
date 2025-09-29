"""Client interfaces and local implementations for data-quality services."""

from .interface import DataQualityServiceClient
from .local import LocalDataQualityServiceClient

__all__ = [
    "DataQualityServiceClient",
    "LocalDataQualityServiceClient",
]
