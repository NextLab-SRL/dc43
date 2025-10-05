"""Data product backend implementations."""

from .collibra import (
    CollibraDataProductAdapter,
    CollibraDataProductServiceBackend,
    HttpCollibraDataProductAdapter,
    StubCollibraDataProductAdapter,
)
from .interface import DataProductRegistrationResult, DataProductServiceBackend
from .local import FilesystemDataProductServiceBackend, LocalDataProductServiceBackend

__all__ = [
    "DataProductRegistrationResult",
    "DataProductServiceBackend",
    "FilesystemDataProductServiceBackend",
    "LocalDataProductServiceBackend",
    "CollibraDataProductAdapter",
    "CollibraDataProductServiceBackend",
    "HttpCollibraDataProductAdapter",
    "StubCollibraDataProductAdapter",
]
