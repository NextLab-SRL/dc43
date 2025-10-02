"""Client implementations for data product services."""

from .interface import DataProductServiceClient
from .local import LocalDataProductServiceClient
from .remote import RemoteDataProductServiceClient

__all__ = [
    "DataProductServiceClient",
    "LocalDataProductServiceClient",
    "RemoteDataProductServiceClient",
]

