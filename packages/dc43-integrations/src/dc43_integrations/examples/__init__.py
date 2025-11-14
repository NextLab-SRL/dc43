"""Example utilities for Databricks governance demos."""

from .databricks_delta_versioning_support import (  # noqa: F401
    VersionedWriteSpec,
    build_contract,
    collect_status_matrix,
    contract_has_discount,
    describe_delta_history,
    drain_streaming_queries,
    ensure_active_data_product,
    extract_streaming_queries,
    make_dataframe,
    make_streaming_dataframe,
    register_contracts,
    render_markdown_matrix,
    write_dataset_version,
    write_streaming_dataset_version,
)

__all__ = [
    "VersionedWriteSpec",
    "build_contract",
    "collect_status_matrix",
    "contract_has_discount",
    "describe_delta_history",
    "drain_streaming_queries",
    "ensure_active_data_product",
    "extract_streaming_queries",
    "make_dataframe",
    "make_streaming_dataframe",
    "register_contracts",
    "render_markdown_matrix",
    "write_dataset_version",
    "write_streaming_dataset_version",
]
