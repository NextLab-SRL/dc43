"""Interfaces for persisting Open Data Product Standard documents."""

from __future__ import annotations

from typing import Protocol, Sequence

from dc43_service_clients.odps import OpenDataProductStandard


class DataProductStore(Protocol):
    """Persistence contract used by data product service backends."""

    def put(self, product: OpenDataProductStandard) -> None:
        """Persist ``product`` making it available for subsequent lookups."""

    def get(self, data_product_id: str, version: str) -> OpenDataProductStandard:
        """Return the stored model for ``data_product_id`` at ``version``."""

    def latest(self, data_product_id: str) -> OpenDataProductStandard | None:
        """Return the latest known version for ``data_product_id`` when available."""

    def list_versions(self, data_product_id: str) -> Sequence[str]:
        """Return the known versions for ``data_product_id`` in ascending order."""

    def list_data_product_ids(self) -> Sequence[str]:
        """Return the identifiers tracked by the store."""


__all__ = ["DataProductStore"]
