from __future__ import annotations

import os
import inspect
import importlib
import logging
from typing import Callable, Sequence, Protocol, Any, Optional

from pyspark.sql import SparkSession, DataFrame
from open_data_contract_standard.model import OpenDataContractStandard

from dc43_service_clients.governance.models import PipelineContext


logger = logging.getLogger(__name__)


class InterceptorContext:
    """Contextual execution data injected into Governance Interceptors."""
    def __init__(
        self, 
        spark: SparkSession, 
        contract: OpenDataContractStandard, 
        operation: str, 
        dataset_id: str, 
        dataset_version: str, 
        dataset_format: str, 
        pipeline_context: Optional[PipelineContext] = None, 
        dataset_locator: Optional[Any] = None, 
        **kwargs
    ):
        self.spark = spark
        self.contract = contract
        self.operation = operation
        self.dataset_id = dataset_id
        self.dataset_version = dataset_version
        self.dataset_format = dataset_format
        self.pipeline_context = pipeline_context
        self.dataset_locator = dataset_locator
        self.kwargs = kwargs


class GovernanceInterceptor(Protocol):
    """Protocol for active data governance interceptors handling the IO lifecycle."""

    def pre_read(self, context: InterceptorContext) -> None:
        """Executed before the physical read is initiated."""
        ...

    def post_read(self, context: InterceptorContext, df: DataFrame) -> DataFrame:
        """Executed after the physical read. Can mutate and return a new DataFrame."""
        ...

    def pre_write(self, context: InterceptorContext, df: DataFrame) -> DataFrame:
        """Executed before the physical write. Can mutate and return a new DataFrame."""
        ...

    def post_write(self, context: InterceptorContext, result: Any) -> None:
        """Executed after the physical write. Receives the `WriteExecutionResult`."""
        ...


class BaseGovernanceInterceptor:
    """Convenience base class representing a no-op interceptor. 
    Subclasses only need to override the lifecycle hooks they care about.
    """

    def pre_read(self, context: InterceptorContext) -> None:
        pass

    def post_read(self, context: InterceptorContext, df: DataFrame) -> DataFrame:
        return df

    def pre_write(self, context: InterceptorContext, df: DataFrame) -> DataFrame:
        return df

    def post_write(self, context: InterceptorContext, result: Any) -> None:
        pass


def resolve_interceptor(ref: str | GovernanceInterceptor) -> GovernanceInterceptor:
    """Resolve an interceptor reference to an instance."""

    # If it already implements the interface or provides the base methods
    if hasattr(ref, 'pre_write') and hasattr(ref, 'post_read'):
        return ref

    if isinstance(ref, str):
        module_name, _, class_name = ref.replace(":", ".").rpartition(".")
        if not module_name or not class_name:
            raise ValueError(f"Invalid interceptor reference format: {ref!r}")

        try:
            module = importlib.import_module(module_name)
        except ImportError as e:
            raise ValueError(f"Could not import module {module_name!r} for interceptor {ref!r}") from e

        interceptor_class = getattr(module, class_name, None)
        if not interceptor_class:
            raise ValueError(f"Interceptor class {class_name!r} not found in module {module_name!r}")
        
        # Instantiate it if it's a class
        if isinstance(interceptor_class, type):
            return interceptor_class()
        return interceptor_class

    raise TypeError(f"Interceptor must be a class path string or an active instance, got {type(ref)}")


def get_global_interceptors(spark: SparkSession | None = None, operation: str | None = None) -> tuple[str, ...]:
    """Return the list of globally configured governance interceptors.
    
    1. Spark conf: `dc43.governance.interceptors.{operation}` (e.g., read, write)
    2. Spark conf: `dc43.governance.interceptors`
    3. OS Environment: `DC43_GOVERNANCE_INTERCEPTORS`
    """
    interceptors_str = ""

    if spark and operation:
        try:
            interceptors_str = spark.conf.get(f"dc43.governance.interceptors.{operation}", "")
        except Exception:
            pass

    if spark and not interceptors_str:
        try:
            interceptors_str = spark.conf.get("dc43.governance.interceptors", "")
        except Exception:
            pass

    if not interceptors_str:
        interceptors_str = os.environ.get("DC43_GOVERNANCE_INTERCEPTORS", "")

    if interceptors_str:
        return tuple(t.strip() for t in interceptors_str.split(",") if t.strip())

    return ()
