from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
import json
import tempfile
from typing import Dict, List, Optional, Protocol, Tuple

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from .interface import ContractStore
from .filesystem import FSContractStore
from dc43_service_backends.core.odcs import as_odcs_dict, contract_identity, ensure_version, to_model
from dc43_service_backends.core.versioning import SemVer


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


class CollibraContractAdapter(Protocol):
    """Minimal abstraction over Collibra operations used by dc43."""

    def list_contracts(self) -> List[str]:
        """Return all contract identifiers known to the adapter."""

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


class CollibraContractStore(ContractStore):
    """Expose Collibra-managed contracts through the :class:`ContractStore` API."""

    def __init__(
        self,
        adapter: CollibraContractAdapter,
        *,
        default_status: str = "Draft",
        status_filter: Optional[str] = None,
    ) -> None:
        self._adapter = adapter
        self._default_status = default_status
        self._status_filter = status_filter

    def put(self, contract: OpenDataContractStandard) -> None:
        ensure_version(contract)
        self._adapter.upsert_contract(contract, status=self._default_status)

    def get(self, contract_id: str, version: str) -> OpenDataContractStandard:
        payload = self._adapter.get_contract(contract_id, version)
        return to_model(payload)

    def list_contracts(self) -> List[str]:
        return self._adapter.list_contracts()

    def list_versions(self, contract_id: str) -> List[str]:
        summaries = self._adapter.list_versions(contract_id)
        if self._status_filter:
            summaries = [s for s in summaries if s.status == self._status_filter]
        return [s.version for s in summaries]

    def latest(self, contract_id: str) -> Optional[OpenDataContractStandard]:
        versions = self.list_versions(contract_id)
        if not versions:
            return None
        versions.sort(key=_semver_key)
        return self.get(contract_id, versions[-1])

    def latest_validated(self, contract_id: str) -> Optional[OpenDataContractStandard]:
        """Return the latest contract marked as ``Validated`` if available."""

        try:
            payload = self._adapter.get_validated_contract(contract_id)
        except LookupError:
            return None
        return to_model(payload)


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


class StubCollibraContractAdapter(CollibraContractAdapter):
    """Filesystem-backed stub adapter used for tests and demos."""

    def __init__(
        self,
        *,
        base_path: Optional[str] = None,
        catalog: Optional[Mapping[str, Tuple[str, str]]] = None,
    ) -> None:
        self._catalog: Dict[str, Tuple[str, str]] = dict(catalog or {})
        if base_path is None:
            self._temp_dir = tempfile.TemporaryDirectory(prefix="dc43-collibra-stub-")
            base_path = self._temp_dir.name
        else:
            self._temp_dir = None
        self._store = FSContractStore(base_path)
        self._metadata: Dict[str, Dict[str, Dict[str, object]]] = {}

    def close(self) -> None:
        if getattr(self, "_temp_dir", None) is not None:
            self._temp_dir.cleanup()
            self._temp_dir = None

    def __del__(self) -> None:  # pragma: no cover - best effort cleanup
        try:
            self.close()
        except Exception:
            pass

    def _register_if_missing(self, contract_id: str) -> None:
        self._catalog.setdefault(contract_id, ("data-product", "port"))
        self._metadata.setdefault(contract_id, {})

    def _version_info(self, contract_id: str, version: str) -> Dict[str, object]:
        self._register_if_missing(contract_id)
        info = self._metadata[contract_id].setdefault(
            version,
            {"status": "Draft", "updated_at": None},
        )
        return info

    def list_contracts(self) -> List[str]:
        contracts = set(self._catalog.keys()) | set(self._store.list_contracts())
        return sorted(contracts)

    def list_versions(self, contract_id: str) -> List[ContractSummary]:
        versions: List[ContractSummary] = []
        for ver in self._store.list_versions(contract_id):
            info = self._version_info(contract_id, ver)
            versions.append(
                ContractSummary(
                    contract_id=contract_id,
                    version=ver,
                    status=str(info.get("status", "Draft")),
                    updated_at=info.get("updated_at"),
                )
            )
        versions.sort(key=lambda s: _semver_key(s.version))
        return versions

    def get_contract(self, contract_id: str, version: str) -> Mapping[str, object]:
        try:
            model = self._store.get(contract_id, version)
        except FileNotFoundError as exc:
            raise LookupError(
                f"Contract {contract_id}:{version} not found in stub Collibra store"
            ) from exc
        payload = as_odcs_dict(model)
        return json.loads(json.dumps(payload))

    def upsert_contract(
        self,
        contract: OpenDataContractStandard,
        *,
        status: str = "Draft",
    ) -> None:
        ensure_version(contract)
        cid, ver = contract_identity(contract)
        self._store.put(contract)
        info = self._version_info(cid, ver)
        info["status"] = status
        info["updated_at"] = _now()

    def submit_draft(self, contract: OpenDataContractStandard) -> None:
        self.upsert_contract(contract, status="Draft")

    def update_status(self, contract_id: str, version: str, status: str) -> None:
        if version not in self._store.list_versions(contract_id):
            raise LookupError(f"Contract {contract_id}:{version} not found in stub Collibra store")
        info = self._version_info(contract_id, version)
        info["status"] = status
        info["updated_at"] = _now()

    def get_validated_contract(self, contract_id: str) -> Mapping[str, object]:
        validated = [s for s in self.list_versions(contract_id) if s.status == "Validated"]
        if not validated:
            raise LookupError(f"No validated contract found for {contract_id}")
        latest = max(validated, key=lambda s: _semver_key(s.version))
        return self.get_contract(contract_id, latest.version)


class HttpCollibraContractAdapter(CollibraContractAdapter):
    """HTTP implementation aligned with Collibra Data Products REST API."""

    def __init__(
        self,
        base_url: str,
        *,
        token: Optional[str] = None,
        timeout: float = 10.0,
        contract_catalog: Optional[Mapping[str, Tuple[str, str]]] = None,
        client=None,
        contracts_endpoint_template: str = "/rest/2.0/dataproducts/{data_product}/ports/{port}/contracts",
    ) -> None:
        try:
            import httpx  # type: ignore
        except Exception as exc:  # pragma: no cover - import guard
            raise RuntimeError("httpx is required to use HttpCollibraContractAdapter") from exc

        self._httpx = httpx
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._catalog: Dict[str, Tuple[str, str]] = dict(contract_catalog or {})
        self._contracts_endpoint_template = contracts_endpoint_template
        if client is None:
            self._client = httpx.Client(base_url=self._base_url, timeout=timeout)
            self._owns_client = True
        else:
            self._client = client
            self._owns_client = False

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "HttpCollibraContractAdapter":  # pragma: no cover - trivial
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

    def _contracts_url(self, data_product: str, port: str, suffix: str = "") -> str:
        return self._contracts_endpoint_template.format(data_product=data_product, port=port) + suffix

    def list_contracts(self) -> List[str]:
        return sorted(self._catalog.keys())

    def list_versions(self, contract_id: str) -> List[ContractSummary]:
        data_product, port = self._locate(contract_id)
        resp = self._client.get(
            self._contracts_url(data_product, port),
            headers=self._headers(),
        )
        resp.raise_for_status()
        payload = resp.json()
        summaries: List[ContractSummary] = []
        items = []
        if isinstance(payload, Mapping):
            if "data" in payload and isinstance(payload["data"], list):
                items = payload["data"]
            elif "results" in payload and isinstance(payload["results"], list):
                items = payload["results"]
            elif "contracts" in payload and isinstance(payload["contracts"], list):
                items = payload["contracts"]
        if not items and isinstance(payload, list):
            items = payload
        for item in items:
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
            self._contracts_url(data_product, port, f"/{version}"),
            headers=self._headers(),
        )
        resp.raise_for_status()
        payload = resp.json()
        if isinstance(payload, Mapping):
            if "contract" in payload:
                return payload["contract"]
            if "data" in payload and isinstance(payload["data"], Mapping):
                return payload["data"]
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
            self._contracts_url(data_product, port, f"/{version}"),
            headers=self._headers(),
            json={"status": status, "contract": contract_dict},
        )
        resp.raise_for_status()

    def submit_draft(self, contract: OpenDataContractStandard) -> None:
        self.upsert_contract(contract, status="Draft")

    def update_status(self, contract_id: str, version: str, status: str) -> None:
        data_product, port = self._locate(contract_id)
        resp = self._client.patch(
            self._contracts_url(data_product, port, f"/{version}"),
            headers=self._headers(),
            json={"status": status},
        )
        resp.raise_for_status()

    def get_validated_contract(self, contract_id: str) -> Mapping[str, object]:
        summaries = [s for s in self.list_versions(contract_id) if s.status == "Validated"]
        if not summaries:
            raise LookupError(f"No validated contract available for {contract_id}")
        summaries.sort(key=lambda s: _semver_key(s.version))
        latest = summaries[-1]
        return self.get_contract(contract_id, latest.version)


# Backwards-compatible aliases retaining the previous gateway naming.
CollibraContractGateway = CollibraContractAdapter
StubCollibraContractGateway = StubCollibraContractAdapter
HttpCollibraContractGateway = HttpCollibraContractAdapter


__all__ = [
    "ContractSummary",
    "CollibraContractAdapter",
    "CollibraContractStore",
    "HttpCollibraContractAdapter",
    "StubCollibraContractAdapter",
    "CollibraContractGateway",
    "HttpCollibraContractGateway",
    "StubCollibraContractGateway",
]
