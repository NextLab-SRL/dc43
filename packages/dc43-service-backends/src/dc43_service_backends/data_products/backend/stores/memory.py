"""In-memory store for Open Data Product Standard documents."""

from __future__ import annotations

from collections import defaultdict
from typing import Dict, Iterable, Mapping, Optional, Sequence

from dc43_service_clients.odps import OpenDataProductStandard, to_model

from .interface import DataProductStore


def _as_mapping(obj: object, seen: set[int]) -> Optional[Mapping[str, object]]:
    identity = id(obj)
    if identity in seen:
        return None
    seen.add(identity)

    if isinstance(obj, OpenDataProductStandard):
        return obj.to_dict()

    if isinstance(obj, Mapping):
        return dict(obj)

    to_dict = getattr(obj, "to_dict", None)
    if callable(to_dict):
        try:
            payload = to_dict()
        except TypeError:
            payload = None
        if isinstance(payload, Mapping):
            return dict(payload)

    for attr in ("model_dump", "dict"):
        serializer = getattr(obj, attr, None)
        if not callable(serializer):
            continue
        for kwargs in ({"by_alias": True, "exclude_none": True}, {}):
            try:
                payload = serializer(**kwargs)
            except TypeError:
                continue
            if isinstance(payload, Mapping):
                return dict(payload)

    clone = getattr(obj, "clone", None)
    if callable(clone):
        return _as_mapping(clone(), seen)

    return None


def _clone_product(product: OpenDataProductStandard | object) -> OpenDataProductStandard:
    payload = _as_mapping(product, set())
    if payload is None:
        raise AttributeError(
            "OpenDataProductStandard clone helpers unavailable for object of type "
            f"{type(product)!r}"
        )

    try:
        return to_model(payload)
    except Exception as exc:  # pragma: no cover - defensive guard
        raise ValueError("Failed to coerce object into OpenDataProductStandard") from exc


class InMemoryDataProductStore(DataProductStore):
    """Persist ODPS documents in memory for the lifetime of the process."""

    def __init__(self) -> None:
        self._products: Dict[str, Dict[str, OpenDataProductStandard]] = defaultdict(dict)
        self._latest: Dict[str, str] = {}

    # ------------------------------------------------------------------
    # Core helpers
    # ------------------------------------------------------------------
    def _existing_versions(self, data_product_id: str) -> Iterable[str]:
        return self._products.get(data_product_id, {}).keys()

    def _store(self, data_product_id: str) -> Dict[str, OpenDataProductStandard]:
        return self._products.setdefault(data_product_id, {})

    # ------------------------------------------------------------------
    # DataProductStore implementation
    # ------------------------------------------------------------------
    def put(self, product: OpenDataProductStandard) -> None:  # noqa: D401 - short docstring
        if not product.version:
            raise ValueError("Data product version is required")
        store = self._store(product.id)
        store[product.version] = _clone_product(product)
        self._latest[product.id] = product.version

    def get(self, data_product_id: str, version: str) -> OpenDataProductStandard:  # noqa: D401
        versions = self._products.get(data_product_id)
        if not versions or version not in versions:
            raise FileNotFoundError(f"data product {data_product_id}:{version} not found")
        return _clone_product(versions[version])

    def latest(self, data_product_id: str) -> Optional[OpenDataProductStandard]:  # noqa: D401
        version = self._latest.get(data_product_id)
        if version is None:
            return None
        return self.get(data_product_id, version)

    def list_versions(self, data_product_id: str) -> Sequence[str]:  # noqa: D401
        versions = self._products.get(data_product_id, {})
        return sorted(versions.keys())

    def list_data_product_ids(self) -> Sequence[str]:  # noqa: D401
        return sorted(self._products.keys())


__all__ = ["InMemoryDataProductStore"]
