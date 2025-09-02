from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

from .base import DQClient, DQStatus
from ..odcs import list_properties, contract_identity
from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore


class StubDQClient(DQClient):
    """Filesystem-backed stub for a DQ/DO service.

    Lightweight local implementation to simulate a DQ service while integrating
    the library. Useful in dev/POC contexts.

    Layout under ``base_path``:
      - ``links/{dataset_id}.json`` -> ``{"contract_id": "x", "contract_version": "y"}``
      - ``status/{dataset_id}/{dataset_version}.json`` -> ``{status, reason, details}``
    """

    def __init__(self, base_path: str, *, block_on_violation: bool = True):
        self.base_path = base_path.rstrip("/")
        self.block_on_violation = block_on_violation

    def _safe(self, s: str) -> str:
        return "".join(ch if ch.isalnum() or ch in ("_", "-", ".") else "_" for ch in s)

    def _links_path(self, dataset_id: str) -> str:
        d = os.path.join(self.base_path, "links")
        os.makedirs(d, exist_ok=True)
        return os.path.join(d, f"{self._safe(dataset_id)}.json")

    def _status_path(self, dataset_id: str, dataset_version: str) -> str:
        d = os.path.join(self.base_path, "status", self._safe(dataset_id))
        os.makedirs(d, exist_ok=True)
        return os.path.join(d, f"{self._safe(str(dataset_version))}.json")

    def expected_metrics(self, contract: OpenDataContractStandard) -> Dict[str, Any]:
        """Return a simple metric spec derived from constraints.

        Always includes ``row_count`` and per-field ``null_count`` then
        adds metrics for common constraints (min/max/regex/enum/unique).
        """
        fields: Dict[str, Any] = {}
        for f in list_properties(contract):
            metrics = ["null_count"]
            # Quality-based metrics
            if f.quality:
                for q in f.quality:
                    if any(v is not None for v in [q.mustBeGreaterThan, q.mustBeGreaterOrEqualTo]):
                        metrics.append("min")
                    if any(v is not None for v in [q.mustBeLessThan, q.mustBeLessOrEqualTo]):
                        metrics.append("max")
            # Logical options
            lto = f.logicalTypeOptions or {}
            if isinstance(lto, dict):
                if "pattern" in lto:
                    metrics.append({"regex_violations": lto["pattern"]})
                if "enum" in lto:
                    metrics.append({"enum_violations": lto["enum"]})
            if f.unique:
                metrics.append("distinct_count")
            fields[f.name] = metrics
        return {"row_count": True, "fields": fields}

    def get_status(
        self,
        *,
        contract_id: str,
        contract_version: str,
        dataset_id: str,
        dataset_version: str,
    ) -> DQStatus:
        path = self._status_path(dataset_id, dataset_version)
        if not os.path.exists(path):
            return DQStatus(status="unknown", reason="no-status-for-version")
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        # naive consistency check
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
        # Very simple rules:
        # - Any expectation violation metrics > 0 -> warn/block depending on
        #   contract expectations ("blocking").
        blocking = self.block_on_violation
        violations = 0
        for k, v in metrics.items():
            if k.startswith("violations.") and isinstance(v, (int, float)):
                violations += int(v)
        status = "ok" if violations == 0 else ("block" if blocking else "warn")
        details = {"violations": violations, "metrics": metrics}

        # Persist status
        path = self._status_path(dataset_id, dataset_version)
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"status": status, "details": details}, f)
        # Ensure link exists
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
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"contract_id": contract_id, "contract_version": contract_version, "dataset_version": dataset_version}, f)

    def get_linked_contract_version(self, *, dataset_id: str) -> Optional[str]:
        """Return the linked ``"id:version"`` string for a dataset if present."""
        path = self._links_path(dataset_id)
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f)
        return f"{d.get('contract_id')}:{d.get('contract_version')}"
