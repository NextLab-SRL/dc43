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

from typing import Any, Dict, List, Tuple, Optional, Callable
from collections.abc import Iterable, Mapping
import os
import json
import hashlib

from open_data_contract_standard.model import (
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
    CustomProperty,
    Description,
    Server,
)  # type: ignore
import open_data_contract_standard as _odcs_pkg  # type: ignore


ODCS_REQUIRED = os.getenv("DC43_ODCS_REQUIRED", "3.0.2")
BITOL_SCHEMA_URL = f"https://bitol.io/schema/{ODCS_REQUIRED}"


# Provide backwards-compatible attribute aliases for ODCS models. Older parts of
# the codebase and downstream integrations expect ``contract_id``/``contract_version``
# attributes on ``OpenDataContractStandard`` instances whereas the upstream model
# exposes ``id``/``version``. Installing lightweight ``property`` aliases keeps both
# spellings in sync without mutating the stored payloads.
def _alias(attr: str) -> Callable[[OpenDataContractStandard], Any]:
    return lambda self: getattr(self, attr)


def _alias_setter(attr: str) -> Callable[[OpenDataContractStandard, Any], None]:
    return lambda self, value: setattr(self, attr, value)


if not hasattr(OpenDataContractStandard, "contract_id"):
    OpenDataContractStandard.contract_id = property(  # type: ignore[assignment]
        _alias("id"),
        _alias_setter("id"),
    )

if not hasattr(OpenDataContractStandard, "contract_version"):
    OpenDataContractStandard.contract_version = property(  # type: ignore[assignment]
        _alias("version"),
        _alias_setter("version"),
    )


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
    # Work with a shallow copy so we can normalize field names without
    # mutating the caller's object.
    d = dict(doc)
    # Pydantic exposes the ``schema`` field as ``schema_`` on the model to
    # avoid clashing with ``BaseModel.schema``. When contracts are serialized
    # without aliases this key may appear on disk. Map it back to the public
    # "schema" name so validation succeeds regardless of the source format.
    if "schema_" in d and "schema" not in d:
        d["schema"] = d.pop("schema_")
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


def normalise_custom_properties(raw: Any) -> List[Any]:
    """Return ``customProperties`` entries as a list while handling descriptors."""

    if raw is None or isinstance(raw, (str, bytes, bytearray)):
        return []
    if isinstance(raw, property):
        return []
    if isinstance(raw, Mapping):
        iterable = raw.values()
    elif isinstance(raw, Iterable):
        iterable = raw
    else:
        try:
            iterable = list(raw)
        except TypeError:
            return []
    return [item for item in iterable if item is not None]


def custom_properties_dict(source: Any) -> Dict[str, Any]:
    """Return a mapping of ``property`` -> ``value`` for ``source`` custom properties."""

    props: Dict[str, Any] = {}
    raw = getattr(source, "customProperties", None)
    for item in normalise_custom_properties(raw):
        key = None
        value = None
        if isinstance(item, Mapping):
            key = item.get("property")
            value = item.get("value")
        else:
            key = getattr(item, "property", None)
            value = getattr(item, "value", None)
        if key:
            props[str(key)] = value
    return props


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
    servers: List[Server] | None = None,
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
        servers=servers,
    )

__all__ = [
    "ODCS_REQUIRED",
    "BITOL_SCHEMA_URL",
    "as_odcs_dict",
    "odcs_package_version",
    "to_model",
    "ensure_version",
    "contract_identity",
    "list_properties",
    "field_map",
    "fingerprint",
    "normalise_custom_properties",
    "custom_properties_dict",
    "build_odcs",
]
