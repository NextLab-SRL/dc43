"""Client interfaces and local implementations for contract services."""

from .interface import ContractServiceClient
from .local import LocalContractServiceClient

__all__ = ["ContractServiceClient", "LocalContractServiceClient"]
