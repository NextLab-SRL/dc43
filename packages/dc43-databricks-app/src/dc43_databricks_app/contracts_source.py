"""Resolve a dc43 :class:`ContractServiceClient` and read contracts through it.

This is the dc43-native replacement for parsing ODCS files directly: the app
only ever talks to the client protocol, so the same UI works against the
filesystem store, the SQL/Delta/Collibra stores, or a remote service backend.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from open_data_contract_standard.model import OpenDataContractStandard

from .config import DEMO_CONTRACTS_DIR, Settings


def build_contract_client(settings: Settings):
    """Build the appropriate ContractServiceClient for the environment."""
    if settings.contracts_url:
        from dc43_service_clients.contracts.client.remote import (
            RemoteContractServiceClient,
        )

        return RemoteContractServiceClient(
            base_url=settings.contracts_url,
            token=settings.contracts_token,
        )

    from dc43_service_backends.contracts.backend.stores import FSContractStore
    from dc43_service_clients.contracts.client.local import LocalContractServiceClient

    base_path = settings.contract_path or str(DEMO_CONTRACTS_DIR)
    return LocalContractServiceClient(store=FSContractStore(base_path))


# --------------------------------------------------------------- model helpers


@dataclass
class ContractSummary:
    """Flat, UI-friendly view over an ODCS document."""

    contract_id: str
    version: str
    status: str
    name: str
    description: str
    owner: str
    table_fqn: str | None
    properties: list[dict] = field(default_factory=list)
    model: OpenDataContractStandard | None = None

    @property
    def raw(self) -> dict[str, Any]:
        if self.model is None:
            return {}
        return self.model.model_dump(by_alias=True, exclude_none=True)


def _custom_property(contract: OpenDataContractStandard, key: str) -> str | None:
    for prop in contract.customProperties or []:
        if prop.property == key:
            return str(prop.value) if prop.value is not None else None
    return None


def bound_table_fqn(contract: OpenDataContractStandard) -> str | None:
    """UC table bound to a contract: ``uc.table`` custom property, else
    the first server's ``catalog.schema`` plus the schema object's physical name."""
    explicit = _custom_property(contract, "uc.table")
    if explicit:
        return explicit
    servers = contract.servers or []
    schema_objects = contract.schema_ or []
    if servers and schema_objects:
        srv = servers[0]
        physical = schema_objects[0].physicalName or schema_objects[0].name
        if srv.catalog and srv.schema_ and physical:
            return f"{srv.catalog}.{srv.schema_}.{physical}"
    return None


def summarize(contract: OpenDataContractStandard) -> ContractSummary:
    schema_objects = contract.schema_ or []
    properties = [
        {
            "name": p.name,
            "physicalType": p.physicalType,
            "required": bool(p.required),
        }
        for p in (schema_objects[0].properties or [])
    ] if schema_objects else []
    desc = contract.description
    description = (desc.usage or desc.purpose or "") if desc is not None else ""
    return ContractSummary(
        contract_id=contract.id or "unknown",
        version=str(contract.version or "0.0.0"),
        status=contract.status or "draft",
        name=contract.name or contract.id or "unknown",
        description=description,
        owner=_custom_property(contract, "owner") or "—",
        table_fqn=bound_table_fqn(contract),
        properties=properties,
        model=contract,
    )


def load_latest_contracts(client) -> list[ContractSummary]:
    """Latest version of every contract known to the service."""
    listing = client.list_contracts()
    items = listing["items"] if isinstance(listing, dict) else getattr(listing, "items", [])
    summaries: list[ContractSummary] = []
    for contract_id in items:
        latest = client.latest(contract_id)
        if latest is not None:
            summaries.append(summarize(latest))
    return summaries
