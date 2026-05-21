from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
import json
import tempfile
import yaml
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
    """HTTP implementation aligned with Collibra Data Products REST API v1 and cascading lookup."""

    def __init__(
        self,
        base_url: str,
        *,
        token: Optional[str] = None,
        timeout: float = 10.0,
        contract_catalog: Optional[Mapping[str, Tuple[str, str]]] = None,
        client=None,
        contracts_endpoint_template: str = "/rest/dataProduct/v1/dataContracts",
        relation_type_contains_id: Optional[str] = None,
        relation_type_governed_id: Optional[str] = None,
        data_product_type_id: Optional[str] = None,
    ) -> None:
        try:
            import httpx  # type: ignore
        except Exception as exc:  # pragma: no cover - import guard
            raise RuntimeError("httpx is required to use HttpCollibraContractAdapter") from exc

        self._httpx = httpx
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._catalog: Dict[str, Tuple[str, str]] = dict(contract_catalog or {})
        
        # UUID config for cascading lookup
        self._relation_type_contains_id = relation_type_contains_id or "rel-contains-uuid-1111"
        self._relation_type_governed_id = relation_type_governed_id or "rel-governed-uuid-2222"
        self._data_product_type_id = data_product_type_id or "dp-type-uuid-3333"

        # Caching of resolved contract UUIDs
        self._uuid_cache: Dict[str, str] = {}

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

    def _resolve_contract_uuid(self, contract_id: str) -> str:
        if contract_id in self._uuid_cache:
            return self._uuid_cache[contract_id]

        # 1. Fast Path (Option A) - query by manifestId
        try:
            resp = self._client.get(
                "/rest/dataProduct/v1/dataContracts",
                headers=self._headers(),
                params={"manifestId": contract_id},
            )
            resp.raise_for_status()
            payload = resp.json()
            items = payload.get("items", [])
            if items:
                contract_uuid = items[0]["id"]
                self._uuid_cache[contract_id] = contract_uuid
                return contract_uuid
        except Exception:
            pass

        # 2. Cascading Graph Lookup
        if contract_id not in self._catalog:
            raise LookupError(f"Contract {contract_id} is not registered in the Collibra catalog")
        
        data_product_name, port_name = self._catalog[contract_id]

        # Step 2a: Resolve Data Product UUID from name
        import re
        uuid_pattern = re.compile(r"^[a-fA-F0-9-]{36}$")
        
        dp_uuid = None
        if uuid_pattern.match(data_product_name):
            dp_uuid = data_product_name
        else:
            resp = self._client.get(
                "/rest/2.0/assets",
                headers=self._headers(),
                params={"name": data_product_name, "typeId": self._data_product_type_id},
            )
            resp.raise_for_status()
            results = resp.json().get("results", [])
            if not results:
                raise LookupError(f"Data Product asset '{data_product_name}' not found in Collibra")
            dp_uuid = results[0]["id"]

        # Step 2b: Resolve Port UUID from Data Product containing Port relations
        port_uuid = None
        if uuid_pattern.match(port_name):
            port_uuid = port_name
        else:
            resp = self._client.get(
                "/rest/2.0/relations",
                headers=self._headers(),
                params={
                    "sourceId": dp_uuid,
                    "relationTypeId": self._relation_type_contains_id,
                },
            )
            resp.raise_for_status()
            relations = resp.json().get("results", [])
            # Search for the port with matching name (exact then case-insensitive)
            for rel in relations:
                target = rel.get("target", {})
                if target.get("name") == port_name or target.get("displayName") == port_name:
                    port_uuid = target.get("id")
                    break
            
            if not port_uuid:
                # Case-insensitive fallback
                for rel in relations:
                    target = rel.get("target", {})
                    t_name = target.get("name", "").lower()
                    t_disp = target.get("displayName", "").lower()
                    if t_name == port_name.lower() or t_disp == port_name.lower():
                        port_uuid = target.get("id")
                        break
            
            if not port_uuid:
                raise LookupError(
                    f"Port asset '{port_name}' not found under Data Product '{data_product_name}' relations"
                )

        # Step 2c: Resolve Data Contract UUID from Port relations
        # Check targetId first (Port governed by Data Contract: Contract -> Port)
        resp = self._client.get(
            "/rest/2.0/relations",
            headers=self._headers(),
            params={
                "targetId": port_uuid,
                "relationTypeId": self._relation_type_governed_id,
            },
        )
        resp.raise_for_status()
        relations = resp.json().get("results", [])
        if relations:
            contract_uuid = relations[0]["source"]["id"]
            self._uuid_cache[contract_id] = contract_uuid
            return contract_uuid

        # Try sourceId fallback
        resp = self._client.get(
            "/rest/2.0/relations",
            headers=self._headers(),
            params={
                "sourceId": port_uuid,
                "relationTypeId": self._relation_type_governed_id,
            },
        )
        resp.raise_for_status()
        relations = resp.json().get("results", [])
        if relations:
            contract_uuid = relations[0]["target"]["id"]
            self._uuid_cache[contract_id] = contract_uuid
            return contract_uuid

        raise LookupError(f"No Data Contract asset found governing Port '{port_name}' (UUID: {port_uuid})")

    def _resolve_port_uuid(self, contract_id: str) -> str:
        if contract_id not in self._catalog:
            raise LookupError(f"Contract {contract_id} is not registered in the Collibra catalog")
        
        data_product_name, port_name = self._catalog[contract_id]

        import re
        uuid_pattern = re.compile(r"^[a-fA-F0-9-]{36}$")
        if uuid_pattern.match(port_name):
            return port_name

        dp_uuid = None
        if uuid_pattern.match(data_product_name):
            dp_uuid = data_product_name
        else:
            resp = self._client.get(
                "/rest/2.0/assets",
                headers=self._headers(),
                params={"name": data_product_name, "typeId": self._data_product_type_id},
            )
            resp.raise_for_status()
            results = resp.json().get("results", [])
            if not results:
                raise LookupError(f"Data Product asset '{data_product_name}' not found in Collibra")
            dp_uuid = results[0]["id"]

        resp = self._client.get(
            "/rest/2.0/relations",
            headers=self._headers(),
            params={
                "sourceId": dp_uuid,
                "relationTypeId": self._relation_type_contains_id,
            },
        )
        resp.raise_for_status()
        relations = resp.json().get("results", [])
        for rel in relations:
            target = rel.get("target", {})
            if target.get("name") == port_name or target.get("displayName") == port_name:
                return target["id"]
        
        # Case-insensitive fallback
        for rel in relations:
            target = rel.get("target", {})
            t_name = target.get("name", "").lower()
            t_disp = target.get("displayName", "").lower()
            if t_name == port_name.lower() or t_disp == port_name.lower():
                return target["id"]

        raise LookupError(
            f"Port asset '{port_name}' not found under Data Product '{data_product_name}' relations"
        )

    def list_contracts(self) -> List[str]:
        return sorted(self._catalog.keys())

    def list_versions(self, contract_id: str) -> List[ContractSummary]:
        contract_uuid = self._resolve_contract_uuid(contract_id)
        resp = self._client.get(
            f"/rest/dataProduct/v1/dataContracts/{contract_uuid}/versions",
            headers=self._headers(),
        )
        resp.raise_for_status()
        payload = resp.json()
        
        summaries: List[ContractSummary] = []
        for item in payload.get("items", []):
            version = item.get("version")
            if not version:
                continue
            
            # Map active=True to "Validated", active=False to "Draft"
            status = "Validated" if item.get("active") else "Draft"
            created_on_ms = item.get("lastModifiedOn") or item.get("createdOn")
            updated_at = datetime.utcfromtimestamp(created_on_ms / 1000.0) if created_on_ms else None
            
            summaries.append(
                ContractSummary(
                    contract_id=contract_id,
                    version=str(version),
                    status=status,
                    updated_at=updated_at,
                )
            )
        summaries.sort(key=lambda s: _semver_key(s.version))
        return summaries

    def get_contract(self, contract_id: str, version: str) -> Mapping[str, object]:
        contract_uuid = self._resolve_contract_uuid(contract_id)
        resp = self._client.get(
            f"/rest/dataProduct/v1/dataContracts/{contract_uuid}/versions/manifest",
            headers=self._headers(),
            params={"version": version},
        )
        resp.raise_for_status()
        # Parse YAML payload directly to dict
        return yaml.safe_load(resp.text)

    def upsert_contract(
        self,
        contract: OpenDataContractStandard,
        *,
        status: str = "Draft",
    ) -> None:
        ensure_version(contract)
        contract_id, version = contract_identity(contract)
        yaml_str = yaml.dump(as_odcs_dict(contract))

        # Check if contract already exists
        try:
            contract_uuid = self._resolve_contract_uuid(contract_id)
            exists = True
        except LookupError:
            exists = False

        if exists:
            # Upload a new version
            data = {
                "version": version,
                "active": "true" if status == "Validated" else "false",
            }
            files = {
                "manifest": ("manifest.yaml", yaml_str.encode("utf-8"), "text/plain")
            }
            resp = self._client.post(
                f"/rest/dataProduct/v1/dataContracts/{contract_uuid}/versions",
                headers=self._headers(),
                data=data,
                files=files,
            )
            resp.raise_for_status()
        else:
            # Initialize a new contract
            port_uuid = self._resolve_port_uuid(contract_id)
            data = {
                "governedAssetId": port_uuid,
                "manifestId": contract_id,
                "version": version,
                "name": contract.name or contract_id,
            }
            files = {
                "manifest": ("manifest.yaml", yaml_str.encode("utf-8"), "text/plain")
            }
            resp = self._client.post(
                "/rest/dataProduct/v1/dataContracts",
                headers=self._headers(),
                data=data,
                files=files,
            )
            resp.raise_for_status()
            payload = resp.json()
            new_uuid = payload.get("id")
            if new_uuid:
                self._uuid_cache[contract_id] = new_uuid

    def submit_draft(self, contract: OpenDataContractStandard) -> None:
        self.upsert_contract(contract, status="Draft")

    def update_status(self, contract_id: str, version: str, status: str) -> None:
        if status == "Validated":
            contract_uuid = self._resolve_contract_uuid(contract_id)
            resp = self._client.patch(
                f"/rest/dataProduct/v1/dataContracts/{contract_uuid}/activeVersion",
                headers=self._headers(),
                params={"version": version},
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
