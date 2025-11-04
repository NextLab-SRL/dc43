"""Store-backed implementations of the data product backend."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from dc43_service_clients.odps import OpenDataProductStandard

from ._shared import MutableDataProductBackendMixin
from .interface import DataProductListing, DataProductServiceBackend
from .stores import (
    DataProductStore,
    FilesystemDataProductStore,
    InMemoryDataProductStore,
)


class _StoreBackedDataProductServiceBackend(MutableDataProductBackendMixin, DataProductServiceBackend):
    """Delegate persistence operations to the configured data product store."""

    def __init__(self, store: DataProductStore) -> None:
        self._store = store

    # ------------------------------------------------------------------
    # DataProductServiceBackend implementation
    # ------------------------------------------------------------------
    def put(self, product: OpenDataProductStandard) -> None:  # noqa: D401 - short docstring
        self._store.put(product)

    def list_data_products(
        self, *, limit: int | None = None, offset: int = 0
    ) -> DataProductListing:  # noqa: D401
        product_ids = list(self._store.list_data_product_ids())
        total = len(product_ids)
        start = max(int(offset), 0)
        end = total
        if limit is not None:
            span = max(int(limit), 0)
            end = min(start + span, total)
        return DataProductListing(
            items=product_ids[start:end],
            total=total,
            limit=limit,
            offset=start,
        )

    def get(self, data_product_id: str, version: str) -> OpenDataProductStandard:  # noqa: D401
        return self._store.get(data_product_id, version)

    def latest(self, data_product_id: str) -> Optional[OpenDataProductStandard]:  # noqa: D401
        return self._store.latest(data_product_id)

    def list_versions(self, data_product_id: str):  # noqa: D401
        return self._store.list_versions(data_product_id)


class LocalDataProductServiceBackend(_StoreBackedDataProductServiceBackend):
    """Store ODPS documents in memory while providing port registration helpers."""

    def __init__(self, store: DataProductStore | None = None) -> None:
        super().__init__(store or InMemoryDataProductStore())


class FilesystemDataProductServiceBackend(_StoreBackedDataProductServiceBackend):
    """Persist ODPS documents as JSON files following the ODPS schema."""

    def __init__(self, root: str | Path, *, store: DataProductStore | None = None) -> None:
        super().__init__(store or FilesystemDataProductStore(root))


__all__ = ["LocalDataProductServiceBackend", "FilesystemDataProductServiceBackend"]
