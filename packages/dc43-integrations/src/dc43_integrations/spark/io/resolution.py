from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable

from pyspark.sql import DataFrame, SparkSession
from open_data_contract_standard.model import OpenDataContractStandard


@dataclass(slots=True)
class DatasetResolution:
    """Resolved location and governance identifiers for a dataset."""

    path: Optional[str]
    table: Optional[str]
    format: Optional[str]
    dataset_id: Optional[str]
    dataset_version: Optional[str]
    read_options: Optional[Dict[str, str]] = None
    write_options: Optional[Dict[str, str]] = None
    custom_properties: Optional[Dict[str, Any]] = None
    load_paths: Optional[List[str]] = None


@runtime_checkable
class DatasetLocatorStrategy(Protocol):
    """Resolve IO coordinates and identifiers for read/write operations."""

    def for_read(
        self,
        *,
        contract: Optional[OpenDataContractStandard],
        spark: SparkSession,
        format: Optional[str],
        path: Optional[str],
        table: Optional[str],
    ) -> DatasetResolution:
        ...

    def for_write(
        self,
        *,
        contract: Optional[OpenDataContractStandard],
        df: DataFrame,
        format: Optional[str],
        path: Optional[str],
        table: Optional[str],
    ) -> DatasetResolution:
        ...
