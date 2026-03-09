from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Protocol, runtime_checkable

from pyspark.sql import DataFrame
from open_data_contract_standard.model import OpenDataContractStandard
from dc43_service_clients.odps import OpenDataProductStandard
from dc43_service_clients.data_quality import ValidationResult

from dc43_integrations.spark.io.validation import (
    _validate_contract_status,
    _validate_data_product_status,
)


@dataclass(slots=True)
class ReadStatusContext:
    """Information exposed to read status strategies."""

    contract: Optional[OpenDataContractStandard]
    dataset_id: Optional[str]
    dataset_version: Optional[str]


@runtime_checkable
class ReadStatusStrategy(Protocol):
    """Allow callers to react to DQ statuses before returning a dataframe."""

    def apply(
        self,
        *,
        dataframe: DataFrame,
        status: Optional[ValidationResult],
        enforce: bool,
        context: ReadStatusContext,
    ) -> DataFrame:
        ...


@dataclass(slots=True)
class DefaultReadStatusStrategy:
    """Default behaviour preserving enforcement semantics."""

    allowed_contract_statuses: tuple[str, ...] = ("active",)
    allow_missing_contract_status: bool = True
    allowed_data_product_statuses: tuple[str, ...] = ("active",)
    allow_missing_data_product_status: bool = True
    data_product_status_case_insensitive: bool = True
    data_product_status_failure_message: str | None = None

    def validate_contract_status(
        self,
        *,
        contract: OpenDataContractStandard,
        enforce: bool,
        operation: str,
    ) -> None:
        """Apply contract status rules."""
        _validate_contract_status(
            contract=contract,
            allowed_statuses=self.allowed_contract_statuses,
            allow_missing=self.allow_missing_contract_status,
            enforce=enforce,
            operation=operation,
        )

    def apply(
        self,
        *,
        dataframe: DataFrame,
        status: Optional[ValidationResult],
        enforce: bool,
        context: ReadStatusContext,
    ) -> DataFrame:
        """Apply the validation result to the dataframe."""
        if status is not None and enforce:
            # Note: base executor handles basic quality enforcement.
            # This hook allows strategies to perform custom interventions.
            pass
        return dataframe

    def validate_data_product_status(
        self,
        *,
        data_product: OpenDataProductStandard,
        enforce: bool,
        operation: str,
    ) -> None:
        """Apply data product status rules."""
        _validate_data_product_status(
            data_product=data_product,
            allowed_statuses=self.allowed_data_product_statuses,
            allow_missing=self.allow_missing_data_product_status,
            case_insensitive=self.data_product_status_case_insensitive,
            failure_message=self.data_product_status_failure_message,
            enforce=enforce,
            operation=operation,
        )
