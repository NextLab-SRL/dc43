"""Utilities to integrate dc43 contracts with Collibra Data Products.

This module exposes a small gateway abstraction that hides the details of
Collibra's REST APIs.  dc43 components can rely on the gateway to fetch the
latest validated contract, upsert draft proposals, and list available
versions.  A lightweight in-memory stub is provided for tests or local
development while :class:`HttpCollibraContractGateway` performs real HTTP
calls.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from typing import Dict, List, Mapping, Optional, Protocol, Tuple

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from ..odcs import as_odcs_dict, contract_identity, ensure_version
from ..versioning import SemVer


def _semver_key(version: str) -> Tuple[int, int, int, str]:
    semver = SemVer.parse(version)
    return (semver.major, semver.minor, semver.patch, semver.prerelease or "")


@dataclass(frozen=True)
class ContractSummary:
    """Small DTO describing a contract version stored in Collibra."""

    contract_id: str
    version: str
    status: str
    updated_at: Optional[datetime] = None


class CollibraContractGateway(Protocol):
    """Abstraction over Collibra operations used by dc43."""

    def list_contracts(self) -> List[str]:
        """Return all contract identifiers known to the gateway."""

    def list_versions(self, contract_id: str) -> List[ContractSummary]:
        """Return version summaries for ``contract_id``."""

    def get_contract(self, contract_id: str, version: str) -> Mapping[str, object]:
        """Return the raw ODCS JSON document for ``contract_id``/``version``."""

    def upsert_contract(
        self,
        contract: OpenDataContractStandard,
        *,
        status: str = "Draft",
    ) -> None:
        """Create or update a Collibra contract version."""

    def submit_draft(self, contract: OpenDataContractStandard) -> None:
        """Convenience wrapper used when persisting draft proposals."""

    def update_status(self, contract_id: str, version: str, status: str) -> None:
        """Update the lifecycle state for a stored contract version."""

    def get_validated_contract(self, contract_id: str) -> Mapping[str, object]:
        """Return the latest contract marked as ``Validated`` for ``contract_id``."""


def _now() -> datetime:
    return datetime.utcnow()


def _parse_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


class StubCollibraContractGateway(CollibraContractGateway):
    """In-memory gateway used for tests and demos."""

    def __init__(self, *, catalog: Optional[Mapping[str, Tuple[str, str]]] = None):
        self._catalog: Dict[str, Tuple[str, str]] = dict(catalog or {})
        self._contracts: Dict[str, Dict[str, Dict[str, object]]] = {}

    def _register_if_missing(self, contract_id: str) -> None:
        self._catalog.setdefault(contract_id, ("data-product", "port"))
        self._contracts.setdefault(contract_id, {})

    def list_contracts(self) -> List[str]:
        return sorted(self._catalog.keys() | self._contracts.keys())

    def list_versions(self, contract_id: str) -> List[ContractSummary]:
        versions = []
        for ver, entry in self._contracts.get(contract_id, {}).items():
            versions.append(
                ContractSummary(
                    contract_id=contract_id,
                    version=ver,
                    status=str(entry.get("status", "Draft")),
                    updated_at=entry.get("updated_at"),
                )
            )
        versions.sort(key=lambda s: _semver_key(s.version))
        return versions

    def get_contract(self, contract_id: str, version: str) -> Mapping[str, object]:
        versions = self._contracts.get(contract_id)
        if not versions or version not in versions:
            raise LookupError(f"Contract {contract_id}:{version} not found in stub Collibra store")
        stored = versions[version]
        doc = stored.get("document")
        if not isinstance(doc, Mapping):
            raise TypeError("Stored contract payload must be a mapping")
        return json.loads(json.dumps(doc))

    def upsert_contract(
        self,
        contract: OpenDataContractStandard,
        *,
        status: str = "Draft",
    ) -> None:
        ensure_version(contract)
        cid, ver = contract_identity(contract)
        self._register_if_missing(cid)
        payload = as_odcs_dict(contract)
        entry = self._contracts.setdefault(cid, {}).setdefault(ver, {})
        entry["document"] = payload
        entry["status"] = status
        entry["updated_at"] = _now()

    def submit_draft(self, contract: OpenDataContractStandard) -> None:
        self.upsert_contract(contract, status="Draft")

    def update_status(self, contract_id: str, version: str, status: str) -> None:
        versions = self._contracts.get(contract_id)
        if not versions or version not in versions:
            raise LookupError(f"Contract {contract_id}:{version} not found in stub Collibra store")
        versions[version]["status"] = status
        versions[version]["updated_at"] = _now()

    def get_validated_contract(self, contract_id: str) -> Mapping[str, object]:
        validated = [s for s in self.list_versions(contract_id) if s.status == "Validated"]
        if not validated:
            raise LookupError(f"No validated contract found for {contract_id}")
        validated.sort(key=lambda s: _semver_key(s.version))
        latest = validated[-1]
        return self.get_contract(contract_id, latest.version)


class HttpCollibraContractGateway(CollibraContractGateway):
    """HTTP implementation talking to Collibra's REST API."""

    def __init__(
        self,
        base_url: str,
        *,
        token: Optional[str] = None,
        timeout: float = 10.0,
        contract_catalog: Optional[Mapping[str, Tuple[str, str]]] = None,
        client=None,
    ) -> None:
        try:
            import httpx  # type: ignore
        except Exception as exc:  # pragma: no cover - import guard
            raise RuntimeError("httpx is required to use HttpCollibraContractGateway") from exc

        self._httpx = httpx
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._catalog: Dict[str, Tuple[str, str]] = dict(contract_catalog or {})
        if client is None:
            self._client = httpx.Client(base_url=self._base_url, timeout=timeout)
            self._owns_client = True
        else:
            self._client = client
            self._owns_client = False

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "HttpCollibraContractGateway":  # pragma: no cover - trivial
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - trivial
        self.close()

    def _headers(self) -> Dict[str, str]:
        headers = {"accept": "application/json"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        return headers

    def _locate(self, contract_id: str) -> Tuple[str, str]:
        if contract_id not in self._catalog:
            raise LookupError(f"Contract {contract_id} is not registered in the Collibra catalog")
        return self._catalog[contract_id]

    def list_contracts(self) -> List[str]:
        return sorted(self._catalog.keys())

    def list_versions(self, contract_id: str) -> List[ContractSummary]:
        data_product, port = self._locate(contract_id)
        resp = self._client.get(
            f"/data-products/{data_product}/ports/{port}/contracts",
            headers=self._headers(),
        )
        resp.raise_for_status()
        payload = resp.json()
        summaries: List[ContractSummary] = []
        for item in payload.get("data", []):
            version = item.get("version")
            if not version:
                continue
            summaries.append(
                ContractSummary(
                    contract_id=contract_id,
                    version=str(version),
                    status=str(item.get("status", "Draft")),
                    updated_at=_parse_timestamp(item.get("updatedAt")),
                )
            )
        summaries.sort(key=lambda s: _semver_key(s.version))
        return summaries

    def get_contract(self, contract_id: str, version: str) -> Mapping[str, object]:
        data_product, port = self._locate(contract_id)
        resp = self._client.get(
            f"/data-products/{data_product}/ports/{port}/contracts/{version}",
            headers=self._headers(),
        )
        resp.raise_for_status()
        payload = resp.json()
        if "contract" in payload:
            return payload["contract"]
        return payload

    def upsert_contract(
        self,
        contract: OpenDataContractStandard,
        *,
        status: str = "Draft",
    ) -> None:
        ensure_version(contract)
        contract_dict = as_odcs_dict(contract)
        contract_id, version = contract_identity(contract)
        data_product, port = self._locate(contract_id)
        resp = self._client.put(
            f"/data-products/{data_product}/ports/{port}/contracts/{version}",
            headers=self._headers() | {"content-type": "application/json"},
            content=json.dumps({"status": status, "contract": contract_dict}),
        )
        resp.raise_for_status()

    def submit_draft(self, contract: OpenDataContractStandard) -> None:
        self.upsert_contract(contract, status="Draft")

    def update_status(self, contract_id: str, version: str, status: str) -> None:
        data_product, port = self._locate(contract_id)
        resp = self._client.patch(
            f"/data-products/{data_product}/ports/{port}/contracts/{version}",
            headers=self._headers() | {"content-type": "application/json"},
            content=json.dumps({"status": status}),
        )
        resp.raise_for_status()

    def get_validated_contract(self, contract_id: str) -> Mapping[str, object]:
        summaries = [s for s in self.list_versions(contract_id) if s.status == "Validated"]
        if not summaries:
            raise LookupError(f"No validated contract available for {contract_id}")
        summaries.sort(key=lambda s: _semver_key(s.version))
        latest = summaries[-1]
        return self.get_contract(contract_id, latest.version)
