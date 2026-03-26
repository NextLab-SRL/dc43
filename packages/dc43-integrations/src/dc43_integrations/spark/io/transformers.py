from __future__ import annotations

import importlib
import logging
from typing import Callable, Sequence, Protocol

from pyspark.sql import DataFrame
from open_data_contract_standard.model import OpenDataContractStandard


logger = logging.getLogger(__name__)


class ContractBasedTransformer(Protocol):
    """Protocol for callables that apply DataFrame transformations based on an ODCS contract."""

    def __call__(self, df: DataFrame, contract: OpenDataContractStandard) -> DataFrame: ...


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


def get_global_transformers() -> tuple[str, ...]:
    """Return the list of globally configured contract transformers.

    This lazily loads the service backends configuration and extracts any transformers
    defined in the ``[governance]`` section.
    """

    try:
        from dc43_service_backends.config import load_config
    except ImportError:
        logger.debug("dc43-service-backends is not installed; skipping global transformer lookup.")
        return ()

    config = load_config()
    return config.governance.contract_transformers


def apply_contract_transformers(
    df: DataFrame,
    contract: OpenDataContractStandard,
    transformers: Sequence[ContractBasedTransformer | str] | None = None,
) -> DataFrame:
    """Apply an ordered sequence of transformers to the DataFrame based on the contract."""

    if not transformers:
        return df

    for ref in transformers:
        func = resolve_transformer(ref)
        try:
            df = func(df, contract)
        except Exception as e:
            logger.error("Data contract transformer '%s' failed: %s", getattr(func, "__name__", str(ref)), e)
            raise RuntimeError(f"Data contract transformer failed: {e}") from e

    return df
