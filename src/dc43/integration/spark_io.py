"""Compatibility shim for Spark IO helpers."""

from dc43.components.integration.spark_io import (
    read_with_contract,
    write_with_contract,
)

__all__ = ["read_with_contract", "write_with_contract"]
