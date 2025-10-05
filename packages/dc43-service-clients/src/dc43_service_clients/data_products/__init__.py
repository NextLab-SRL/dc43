"""Client-facing data product service helpers."""

from ._compat import DataProductRegistrationResult
from .client.interface import DataProductServiceClient
from .client.local import LocalDataProductServiceClient
from .client.remote import RemoteDataProductServiceClient
from .models import (
    DataProductInputBinding,
    DataProductOutputBinding,
    normalise_input_binding,
    normalise_output_binding,
)

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

