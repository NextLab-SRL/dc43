from __future__ import annotations

from typing import (
    Protocol,
    runtime_checkable,
    Sequence,
)

from open_data_contract_standard.model import OpenDataContractStandard
from dc43_service_clients.odps import OpenDataProductStandard

# Import from new modules to maintain backward compatibility
from dc43_integrations.spark.io.resolution import (
    DatasetResolution,
    DatasetLocatorStrategy,
)
from dc43_integrations.spark.io.locators import (
    ContractFirstDatasetLocator,
    StaticDatasetLocator,
    ContractVersionLocator,
    _ref_from_contract,
    _timestamp,
)
from dc43_integrations.spark.io.status import (
    ReadStatusContext,
    ReadStatusStrategy,
    DefaultReadStatusStrategy,
)


@runtime_checkable
class SupportsContractStatusValidation(Protocol):
    """Expose a contract-status validation hook."""

    def validate_contract_status(
        self,
        *,
        contract: OpenDataContractStandard,
        enforce: bool,
        operation: str,
    ) -> None:
        ...


@runtime_checkable
class SupportsDataProductStatusValidation(Protocol):
    """Expose a data-product status validation hook."""

    def validate_data_product_status(
        self,
        *,
        data_product: OpenDataProductStandard,
        enforce: bool,
        operation: str,
    ) -> None:
        ...


@runtime_checkable
class SupportsDataProductStatusPolicy(Protocol):
    """Expose data product status policy attributes."""

    allowed_data_product_statuses: Sequence[str]
    allow_missing_data_product_status: bool
    data_product_status_case_insensitive: bool
    data_product_status_failure_message: str | None


__all__ = [
    "SupportsContractStatusValidation",
    "SupportsDataProductStatusValidation",
    "SupportsDataProductStatusPolicy",
    "DatasetResolution",
    "DatasetLocatorStrategy",
    "ContractFirstDatasetLocator",
    "StaticDatasetLocator",
    "ContractVersionLocator",
    "_ref_from_contract",
    "_timestamp",
    "ReadStatusContext",
    "ReadStatusStrategy",
    "DefaultReadStatusStrategy",
]
