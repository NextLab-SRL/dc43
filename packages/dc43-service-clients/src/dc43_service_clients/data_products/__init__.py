"""Client-facing data product service helpers."""

from .client.interface import DataProductServiceClient
from .client.local import LocalDataProductServiceClient
from .client.remote import RemoteDataProductServiceClient
from .models import (
    DataProductInputBinding,
    DataProductOutputBinding,
    normalise_input_binding,
    normalise_output_binding,
)
from dc43_service_backends.data_products import DataProductRegistrationResult

__all__ = [
    "DataProductInputBinding",
    "DataProductOutputBinding",
    "DataProductRegistrationResult",
    "DataProductServiceClient",
    "LocalDataProductServiceClient",
    "RemoteDataProductServiceClient",
    "normalise_input_binding",
    "normalise_output_binding",
]

