"""Filesystem-backed stub for governance-facing data-quality clients."""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Optional

from ..interface import DQClient, DQStatus
from dc43.odcs import contract_identity
from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

logger = logging.getLogger(__name__)


class StubDQClient(DQClient):
    """Filesystem-backed stub for a DQ/DO service."""

    def __init__(self, base_path: str, *, block_on_violation: bool = True):
        self.base_path = base_path.rstrip("/")
        self.block_on_violation = block_on_violation
        logger.info("Initialized StubDQClient at %s", self.base_path)

    def _safe(self, s: str) -> str:
        """Return a filesystem-safe version of ``s``."""

        return "".join(ch if ch.isalnum() or ch in ("_", "-", ".") else "_" for ch in s)

    def _links_path(self, dataset_id: str) -> str:
        d = os.path.join(self.base_path, "links")
        os.makedirs(d, exist_ok=True)
        return os.path.join(d, f"{self._safe(dataset_id)}.json")

    def _status_path(self, dataset_id: str, dataset_version: str) -> str:
        d = os.path.join(self.base_path, "status", self._safe(dataset_id))
        os.makedirs(d, exist_ok=True)
        return os.path.join(d, f"{self._safe(str(dataset_version))}.json")

    def get_status(
        self,
        *,
        contract_id: str,
        contract_version: str,
        dataset_id: str,
        dataset_version: str,
    ) -> DQStatus:
        path = self._status_path(dataset_id, dataset_version)
        logger.debug("Fetching DQ status from %s", path)
        if not os.path.exists(path):
            return DQStatus(status="unknown", reason="no-status-for-version")
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        link = self.get_linked_contract_version(dataset_id=dataset_id)
        if link and link != f"{contract_id}:{contract_version}":
            return DQStatus(status="block", reason=f"dataset linked to contract {link}", details=data)
        return DQStatus(status=data.get("status", "warn"), reason=data.get("reason"), details=data.get("details", {}))

    def submit_metrics(
        self,
        *,
        contract: OpenDataContractStandard,
        dataset_id: str,
        dataset_version: str,
        metrics: Dict[str, Any],
    ) -> DQStatus:
        blocking = self.block_on_violation
        violations = 0
        for k, v in metrics.items():
            if k.startswith("violations.") or k.startswith("query."):
                if isinstance(v, (int, float)):
                    violations += int(v)
        status = "ok" if violations == 0 else ("block" if blocking else "warn")
        details = {"violations": violations, "metrics": metrics}

        path = self._status_path(dataset_id, dataset_version)
        logger.info("Persisting DQ status %s for %s@%s to %s", status, dataset_id, dataset_version, path)
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"status": status, "details": details}, f)

        self.link_dataset_contract(
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            contract_id=contract_identity(contract)[0],
            contract_version=contract_identity(contract)[1],
        )
        return DQStatus(status=status, details=details)

    def link_dataset_contract(
        self,
        *,
        dataset_id: str,
        dataset_version: str,
        contract_id: str,
        contract_version: str,
    ) -> None:
        path = self._links_path(dataset_id)
        logger.info(
            "Linking dataset %s@%s to contract %s:%s at %s",
            dataset_id,
            dataset_version,
            contract_id,
            contract_version,
            path,
        )
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"contract_id": contract_id, "contract_version": contract_version, "dataset_version": dataset_version}, f)

    def get_linked_contract_version(self, *, dataset_id: str) -> Optional[str]:
        path = self._links_path(dataset_id)
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f)
        link = f"{d.get('contract_id')}:{d.get('contract_version')}"
        logger.debug("Found contract link for %s -> %s", dataset_id, link)
        return link


__all__ = ["StubDQClient"]
