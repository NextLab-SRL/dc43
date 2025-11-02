"""In-memory data product backend used by the service stack."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, Mapping, Optional
import json
import logging

from dc43_service_clients.odps import (
    OpenDataProductStandard,
    as_odps_dict,
    to_model,
)

from .interface import (
    DataProductListing,
    DataProductServiceBackend,
)
from ._shared import MutableDataProductBackendMixin, _version_key


logger = logging.getLogger(__name__)


class LocalDataProductServiceBackend(MutableDataProductBackendMixin, DataProductServiceBackend):
    """Store ODPS documents in memory while providing port registration helpers."""

    def __init__(self) -> None:
        self._products: Dict[str, Dict[str, OpenDataProductStandard]] = defaultdict(dict)
        self._latest: Dict[str, str] = {}

    def _existing_versions(self, data_product_id: str) -> Iterable[str]:
        return self._products.get(data_product_id, {}).keys()

    def _clone_product(self, product: OpenDataProductStandard) -> OpenDataProductStandard:
        """Return a detached :class:`OpenDataProductStandard` instance.

        Consumers occasionally hand over rich objects (for example Pydantic models)
        instead of the dataclass implementation provided by the backend.  Earlier
        implementations attempted to call ``clone`` on those payloads and accepted
        whatever the helper returned which allowed non-ODPS objects (such as ODCS
        documents) to be stored silently.  This routine now normalises any
        compatible representation into an ODPS mapping before re-hydrating it into
        the canonical model, guaranteeing type safety while preserving the
        backwards compatibility with alternative serialisation hooks.
        """

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

        payload = _as_mapping(product, set())
        if payload is None:
            raise AttributeError(
                "OpenDataProductStandard clone helpers unavailable for object of type "
                f"{type(product)!r}"
            )

        try:
            return to_model(payload)
        except Exception as exc:  # pragma: no cover - defensive guard
            raise ValueError(
                "Failed to coerce object into OpenDataProductStandard"
            ) from exc

    def put(self, product: OpenDataProductStandard) -> None:  # noqa: D401 - short docstring
        if not product.version:
            raise ValueError("Data product version is required")
        store = self._products.setdefault(product.id, {})
        store[product.version] = self._clone_product(product)
        self._latest[product.id] = product.version

    def list_data_products(
        self, *, limit: int | None = None, offset: int = 0
    ) -> DataProductListing:  # noqa: D401
        product_ids = sorted(self._products.keys())
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
        versions = self._products.get(data_product_id)
        if not versions or version not in versions:
            raise FileNotFoundError(f"data product {data_product_id}:{version} not found")
        return self._clone_product(versions[version])

    def latest(self, data_product_id: str) -> Optional[OpenDataProductStandard]:  # noqa: D401
        version = self._latest.get(data_product_id)
        if version is None:
            return None
        return self.get(data_product_id, version)

    def list_versions(self, data_product_id: str) -> list[str]:  # noqa: D401
        versions = self._products.get(data_product_id, {})
        return sorted(versions.keys())

    def _ensure_product(self, data_product_id: str) -> OpenDataProductStandard:
        latest = self.latest(data_product_id)
        if latest is not None:
            return latest.clone()
        # Seed a minimal draft when the product does not yet exist
        product = OpenDataProductStandard(id=data_product_id, status="draft")
        product.version = None
        return product

class FilesystemDataProductServiceBackend(MutableDataProductBackendMixin, DataProductServiceBackend):
    """Persist ODPS documents as JSON files following the ODPS schema."""

    def __init__(self, root: str | Path) -> None:
        self._root_path = Path(root)
        self._root_path.mkdir(parents=True, exist_ok=True)

    def _product_dir(self, data_product_id: str) -> Path:
        safe_id = data_product_id.replace("/", "__")
        return self._root_path / safe_id

    def _product_path(self, data_product_id: str, version: str) -> Path:
        return self._product_dir(data_product_id) / f"{version}.json"

    def _load_model(self, path: Path) -> Optional[OpenDataProductStandard]:
        try:
            with path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except Exception:  # pragma: no cover - defensive best effort
            logger.warning("Failed to read data product definition from %s", path, exc_info=True)
            return None
        try:
            return to_model(payload)
        except Exception:  # pragma: no cover - defensive best effort
            logger.warning("Invalid data product payload stored at %s", path, exc_info=True)
            return None

    def put(self, product: OpenDataProductStandard) -> None:  # noqa: D401
        if not product.version:
            raise ValueError("Data product version is required")
        path = self._product_path(product.id, product.version)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            json.dump(as_odps_dict(product), handle, indent=2, sort_keys=True)

    def list_data_products(
        self, *, limit: int | None = None, offset: int = 0
    ) -> DataProductListing:  # noqa: D401
        product_ids: list[str] = []
        for candidate in sorted(self._root_path.iterdir()):
            if not candidate.is_dir():
                continue
            for json_path in sorted(candidate.glob("*.json")):
                model = self._load_model(json_path)
                if model is None:
                    continue
                product_ids.append(model.id)
                break
        unique_ids = sorted(set(product_ids))
        total = len(unique_ids)
        start = max(int(offset), 0)
        end = total
        if limit is not None:
            span = max(int(limit), 0)
            end = min(start + span, total)
        return DataProductListing(
            items=unique_ids[start:end],
            total=total,
            limit=limit,
            offset=start,
        )

    def get(self, data_product_id: str, version: str) -> OpenDataProductStandard:  # noqa: D401
        path = self._product_path(data_product_id, version)
        if not path.exists():
            raise FileNotFoundError(f"data product {data_product_id}:{version} not found")
        model = self._load_model(path)
        if model is None:
            raise FileNotFoundError(f"data product {data_product_id}:{version} not found")
        return model

    def latest(self, data_product_id: str) -> Optional[OpenDataProductStandard]:  # noqa: D401
        versions = self.list_versions(data_product_id)
        if not versions:
            return None
        latest_version = sorted(versions, key=_version_key, reverse=True)[0]
        return self.get(data_product_id, latest_version)

    def list_versions(self, data_product_id: str) -> list[str]:  # noqa: D401
        directory = self._product_dir(data_product_id)
        if not directory.exists():
            return []
        versions = [path.stem for path in directory.glob("*.json") if path.is_file()]
        return sorted(versions, key=_version_key)

    def _existing_versions(self, data_product_id: str) -> Iterable[str]:
        return self.list_versions(data_product_id)


__all__ = ["LocalDataProductServiceBackend", "FilesystemDataProductServiceBackend"]

