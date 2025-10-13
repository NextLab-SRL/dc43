from __future__ import annotations

"""Filesystem-based contract store (DBFS/UC Volumes/local).

Stores ODCS documents under ``{base}/{contract_id}/{version}.json``.
"""

import logging
import os
from typing import List

from .interface import ContractStore
from dc43_service_backends.core.odcs import as_odcs_dict, ensure_version, contract_identity, to_model
from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

logger = logging.getLogger(__name__)


class FSContractStore(ContractStore):
    """Filesystem-based store (works with DBFS, UC Volumes, local FS)."""

    def __init__(self, base_path: str):
        self.base_path = base_path.rstrip("/")

    def _dir(self, contract_id: str) -> str:
        return os.path.join(self.base_path, contract_id)

    def _path(self, contract_id: str, version: str) -> str:
        return os.path.join(self._dir(contract_id), f"{version}.json")

    def put(self, contract: OpenDataContractStandard) -> None:
        """Write an ODCS document model to the filesystem."""
        ensure_version(contract)
        odcs = as_odcs_dict(contract)
        cid, ver = contract_identity(contract)
        d = self._dir(cid)
        os.makedirs(d, exist_ok=True)
        p = self._path(cid, ver)
        logger.info("Storing contract %s:%s at %s", cid, ver, p)
        with open(p, "w", encoding="utf-8") as f:
            import json

            f.write(json.dumps(odcs, indent=2, sort_keys=True))

    def get(self, contract_id: str, version: str) -> OpenDataContractStandard:
        """Read an ODCS document as a model from the filesystem."""
        p = self._path(contract_id, version)
        logger.info("Loading contract %s:%s from %s", contract_id, version, p)
        with open(p, "r", encoding="utf-8") as f:
            import json
            return to_model(json.loads(f.read()))

    def list_contracts(self) -> List[str]:
        """List all contract identifiers stored under the base path."""
        if not os.path.isdir(self.base_path):
            return []
        ids: List[str] = []
        for name in os.listdir(self.base_path):
            d = os.path.join(self.base_path, name)
            if os.path.isdir(d) and any(f.endswith(".json") for f in os.listdir(d)):
                ids.append(name)
        return ids

    def list_versions(self, contract_id: str) -> List[str]:
        """List versions present on disk for ``contract_id``."""
        d = self._dir(contract_id)
        if not os.path.isdir(d):
            return []
        versions: List[str] = []
        for name in os.listdir(d):
            if name.endswith(".json"):
                versions.append(name[:-5])
        return versions


__all__ = ["FSContractStore"]
