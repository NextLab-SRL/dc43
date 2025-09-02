from __future__ import annotations

"""ODCS (Bitol) helpers

Utilities to work with Open Data Contract Standard (Bitol) JSON documents
or their Python objects from the official ``open-data-contract-standard``
package. Helpers focus on identity, schema fields and strict `$schema`
version enforcement (no extra vendor fields).

Environment variables
- `DC43_ODCS_REQUIRED`: required ODCS version string embedded in `$schema`
  (default: ``3.0.2``).
"""

from typing import Any, Dict, List, Tuple, Optional
import os
import json
import hashlib

from open_data_contract_standard.model import (
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
    CustomProperty,
    Description,
)  # type: ignore
import open_data_contract_standard as _odcs_pkg  # type: ignore


ODCS_REQUIRED = os.getenv("DC43_ODCS_REQUIRED", "3.0.2")
BITOL_SCHEMA_URL = f"https://bitol.io/schema/{ODCS_REQUIRED}"


def as_odcs_dict(obj: OpenDataContractStandard) -> Dict[str, Any]:
    """Return a plain dict for an ODCS model instance (for storage/fingerprint).

    Uses aliases so that ``schema_`` serializes as ``schema``.
    """
    if hasattr(obj, "model_dump") and callable(obj.model_dump):
        return obj.model_dump(by_alias=True, exclude_none=True)  # type: ignore[attr-defined]
    if hasattr(obj, "dict") and callable(obj.dict):
        return obj.dict(by_alias=True, exclude_none=True)  # type: ignore[attr-defined]
    raise TypeError("Unsupported ODCS object; expected OpenDataContractStandard instance")


def odcs_package_version() -> Optional[str]:
    """Return the installed ODCS package version if available."""
    try:
        if _odcs_pkg and hasattr(_odcs_pkg, "__version__"):
            return str(_odcs_pkg.__version__)
    except Exception:
        return None
    return None


def to_model(doc: Dict[str, Any]) -> OpenDataContractStandard:
    """Convert a JSON-like dict to ``OpenDataContractStandard`` model."""
    d = doc
    # try from_dict
    if hasattr(OpenDataContractStandard, "from_dict"):
        try:
            return OpenDataContractStandard.from_dict(d)  # type: ignore[attr-defined]
        except Exception:
            pass
    # try pydantic v2
    if hasattr(OpenDataContractStandard, "model_validate"):
        try:
            return OpenDataContractStandard.model_validate(d)  # type: ignore[attr-defined]
        except Exception:
            pass
    # try direct constructor
    try:
        return OpenDataContractStandard(**d)  # type: ignore[misc]
    except Exception as e:
        raise TypeError("Cannot construct OpenDataContractStandard from dict") from e


def ensure_version(doc: OpenDataContractStandard) -> None:
    """Validate that the ODCS document matches the required `$schema` version.

    Raises ``ValueError`` if the schema URL is missing or mismatched.
    """
    # Prefer checking apiVersion directly on the model
    api_ver = doc.apiVersion
    if api_ver and str(api_ver) != str(ODCS_REQUIRED):
        raise ValueError(f"ODCS apiVersion mismatch. Required {ODCS_REQUIRED}, got {api_ver}")


def contract_identity(doc: OpenDataContractStandard) -> Tuple[str, str]:
    """Return the pair ``(contract_id, version)`` from an ODCS document."""
    ensure_version(doc)
    return doc.id, doc.version



def list_properties(doc: OpenDataContractStandard) -> List[SchemaProperty]:
    """Flatten and return all SchemaProperty from the contract schema."""
    ensure_version(doc)
    props: List[SchemaProperty] = []
    if doc.schema_:
        for obj in doc.schema_:
            if obj.properties:
                props.extend(obj.properties)
    return props


def field_map(doc: OpenDataContractStandard) -> Dict[str, SchemaProperty]:
    """Convenience mapping ``name -> SchemaProperty`` for normalized fields."""
    return {p.name: p for p in list_properties(doc) if p.name}


def fingerprint(doc: OpenDataContractStandard) -> str:
    """Return a stable SHA-256 fingerprint of an ODCS JSON document."""
    d = as_odcs_dict(doc)
    payload = json.dumps(d, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_odcs(
    *,
    contract_id: str,
    version: str,
    kind: str,
    api_version: str,
    name: str | None = None,
    description: str | None = None,
    properties: List[SchemaProperty] | None = None,
    schema_objects: List[SchemaObject] | None = None,
    custom_properties: List[CustomProperty] | None = None,
) -> OpenDataContractStandard:
    """Create a minimal ODCS document instance using typed classes.

    Pass either ``schema_objects`` (preferred) or ``properties`` to build
    a single SchemaObject.
    """
    if schema_objects is None:
        schema_objects = [SchemaObject(name=name, properties=properties or [])]
    return OpenDataContractStandard(
        version=version,
        kind=kind,
        apiVersion=api_version,
        id=contract_id,
        name=name or contract_id,
        description=None if description is None else Description(usage=description),
        schema=schema_objects,  # type: ignore[arg-type]
        customProperties=custom_properties,
    )
