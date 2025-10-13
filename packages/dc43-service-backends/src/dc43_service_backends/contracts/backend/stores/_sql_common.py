"""Shared helpers for SQL-flavoured contract stores."""

from __future__ import annotations

import json
from typing import Optional, Tuple, Dict, Any

from dc43_service_backends.core.odcs import (
    as_odcs_dict,
    contract_identity,
    ensure_version,
    fingerprint,
)
from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore


def prepare_contract_row(
    contract: OpenDataContractStandard,
) -> Tuple[str, str, Dict[str, Any]]:
    """Return canonical row payload for persisting ``contract``.

    The helper normalises the ODCS document into the shared column layout used by
    both the relational SQL and Delta-backed stores. It guarantees ``contract``
    has a semantic version, extracts a stable fingerprint, and flattens optional
    description fields so callers can upsert without duplicating serialisation
    logic.
    """

    ensure_version(contract)
    contract_id, version = contract_identity(contract)
    odcs_dict = as_odcs_dict(contract)
    json_payload = json.dumps(odcs_dict, separators=(",", ":"))
    name_value = contract.name or odcs_dict.get("id", "")
    description_value: Optional[str] = None
    if contract.description and getattr(contract.description, "usage", None):
        description_value = str(contract.description.usage)

    payload: Dict[str, Any] = {
        "contract_id": contract_id,
        "version": version,
        "name": name_value,
        "description": description_value,
        "json": json_payload,
        "fingerprint": fingerprint(contract),
    }
    return contract_id, version, payload


__all__ = ["prepare_contract_row"]

