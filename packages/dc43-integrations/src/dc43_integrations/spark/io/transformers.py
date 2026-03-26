from __future__ import annotations

import os
import inspect
import importlib
import logging
from typing import Callable, Sequence, Protocol, Any

from pyspark.sql import SparkSession, DataFrame
from open_data_contract_standard.model import OpenDataContractStandard


logger = logging.getLogger(__name__)


class ContractBasedTransformer(Protocol):
    """Protocol for callables that apply DataFrame transformations based on an ODCS contract."""

    def __call__(self, df: DataFrame, contract: OpenDataContractStandard, **kwargs: Any) -> DataFrame: ...


def resolve_transformer(ref: str | ContractBasedTransformer) -> ContractBasedTransformer:
    """Resolve a transformer reference to a callable.

    If ``ref`` is already a callable, it is returned directly. If it is a string,
    it is imported dynamically (e.g. ``"my_module.my_func"`` or ``"my_module:my_func"``).
    """

    if callable(ref):
        return ref

    if not isinstance(ref, str):
        raise TypeError(f"Transformer reference must be a callable or string, got {type(ref)}")

    module_name, _, func_name = ref.replace(":", ".").rpartition(".")
    if not module_name or not func_name:
        raise ValueError(f"Invalid transformer reference format: {ref!r}")

    try:
        module = importlib.import_module(module_name)
    except ImportError as e:
        raise ValueError(f"Could not import module {module_name!r} for transformer {ref!r}") from e

    func = getattr(module, func_name, None)
    if not func:
        raise ValueError(f"Transformer function {func_name!r} not found in module {module_name!r}")
    
    if not callable(func):
        raise TypeError(f"Resolved transformer {ref!r} is not callable")

    return func


def get_global_transformers(spark: SparkSession | None = None, operation: str | None = None) -> tuple[str, ...]:
    """Return the list of globally configured contract transformers.

    This resolves configurations in the following order of precedence:
    1. Spark conf: `dc43.governance.transformers.{operation}` (e.g., read, write)
    2. Spark conf: `dc43.governance.transformers`
    3. OS Environment: `DC43_GOVERNANCE_CONTRACT_TRANSFORMERS`
    4. Service backends configuration: `[governance] contract_transformers`
    """
    transformers_str = ""

    if spark and operation:
        try:
            transformers_str = spark.conf.get(f"dc43.governance.transformers.{operation}", "")
        except Exception:
            pass

    if spark and not transformers_str:
        try:
            transformers_str = spark.conf.get("dc43.governance.transformers", "")
        except Exception:
            pass

    if not transformers_str:
        transformers_str = os.environ.get("DC43_GOVERNANCE_CONTRACT_TRANSFORMERS", "")

    if transformers_str:
        return tuple(t.strip() for t in transformers_str.split(",") if t.strip())

    return ()


def apply_contract_transformers(
    df: DataFrame,
    contract: OpenDataContractStandard,
    transformers: Sequence[ContractBasedTransformer | str] | None = None,
    **kwargs: Any
) -> DataFrame:
    """Apply an ordered sequence of transformers to the DataFrame based on the contract."""

    if not transformers:
        return df

    for ref in transformers:
        func = resolve_transformer(ref)
        try:
            sig = inspect.signature(func)
            bound_kwargs = {}
            for k, v in kwargs.items():
                if k in sig.parameters or any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()):
                    bound_kwargs[k] = v
            df = func(df, contract, **bound_kwargs)
        except Exception as e:
            logger.error("Data contract transformer '%s' failed: %s", getattr(func, "__name__", str(ref)), e)
            raise RuntimeError(f"Data contract transformer failed: {e}") from e

    return df
