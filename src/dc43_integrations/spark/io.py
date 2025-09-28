"""I/O helpers orchestrating Spark data-quality and governance flows."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

try:  # pragma: no cover - optional dependency
    from pyspark.sql import DataFrame
except Exception:  # pragma: no cover
    DataFrame = object  # type: ignore

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43.odcs import as_odcs_dict
from dc43_service_clients.contracts import ContractServiceClient
from dc43_service_clients.data_quality import (
    DataQualityServiceClient,
    ObservationPayload,
    ValidationResult,
)
from dc43_service_clients.governance import (
    GovernanceServiceClient,
    PipelineContext,
    normalise_pipeline_context,
)

from .data_quality import build_metrics_payload, collect_observations
from .validation import apply_contract


@contextmanager
def temporary_view(df: DataFrame, name: str) -> Iterator[None]:
    """Context manager that registers and cleans up a temporary Spark view."""

    df.createOrReplaceTempView(name)
    try:
        yield
    finally:
        df.sparkSession.catalog.dropTempView(name)


__all__ = [
    "ContractServiceClient",
    "DataQualityServiceClient",
    "GovernanceServiceClient",
    "ObservationPayload",
    "OpenDataContractStandard",
    "PipelineContext",
    "ValidationResult",
    "apply_contract",
    "as_odcs_dict",
    "build_metrics_payload",
    "collect_observations",
    "normalise_pipeline_context",
    "temporary_view",
]
