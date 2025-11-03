"""Filesystem-backed persistence for ODPS documents."""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Sequence
import json
import logging

from dc43_service_clients.odps import OpenDataProductStandard, as_odps_dict, to_model

from .interface import DataProductStore
from .._shared import _version_key


logger = logging.getLogger(__name__)


class FilesystemDataProductStore(DataProductStore):
    """Persist ODPS documents as JSON files on disk."""

    def __init__(self, root: str | Path) -> None:
        self._root_path = Path(root)
        self._root_path.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # DataProductStore implementation
    # ------------------------------------------------------------------
    def put(self, product: OpenDataProductStandard) -> None:  # noqa: D401 - short docstring
        if not product.version:
            raise ValueError("Data product version is required")
        path = self._product_path(product.id, product.version)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            json.dump(as_odps_dict(product), handle, indent=2, sort_keys=True)

    def get(self, data_product_id: str, version: str) -> OpenDataProductStandard:  # noqa: D401
        path = self._product_path(data_product_id, version)
        model = self._load_model(path)
        if model is None:
            raise FileNotFoundError(f"data product {data_product_id}:{version} not found")
        return model

    def latest(self, data_product_id: str) -> Optional[OpenDataProductStandard]:  # noqa: D401
        versions = self.list_versions(data_product_id)
        if not versions:
            return None
        return self.get(data_product_id, versions[-1])

    def list_versions(self, data_product_id: str) -> Sequence[str]:  # noqa: D401
        product_dir = self._product_dir(data_product_id)
        if not product_dir.exists():
            return []
        versions: list[str] = []
        for candidate in sorted(product_dir.glob("*.json")):
            model = self._load_model(candidate)
            if model is None:
                continue
            if model.id != data_product_id:
                continue
            if not model.version:
                continue
            versions.append(model.version)
        return sorted(set(versions), key=_version_key)

    def list_data_product_ids(self) -> Sequence[str]:  # noqa: D401
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
        return sorted(set(product_ids))


__all__ = ["FilesystemDataProductStore"]
