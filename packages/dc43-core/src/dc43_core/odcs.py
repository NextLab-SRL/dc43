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


ODCS_REQUIRED = os.getenv("DC43_ODCS_REQUIRED", "3.1.0")
ODCS_SUPPORTED_VERSIONS = {"3.0.2", "3.1.0", ODCS_REQUIRED}
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
    # Ensure version and apiVersion are string coerced if they are parsed as numbers (e.g. from unquoted YAML)
    if "version" in d and d["version"] is not None:
        d["version"] = str(d["version"])
    if "apiVersion" in d and d["apiVersion"] is not None:
        d["apiVersion"] = str(d["apiVersion"])
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
    """Validate that the ODCS document matches one of the supported `$schema` versions.

    Raises ``ValueError`` if the schema URL is missing or mismatched.
    """
    # Prefer checking apiVersion directly on the model
    api_ver = doc.apiVersion
    if api_ver:
        normalized_ver = str(api_ver).lstrip("v")
        normalized_supported = {v.lstrip("v") for v in ODCS_SUPPORTED_VERSIONS}
        if normalized_ver not in normalized_supported:
            supported = ", ".join(sorted(ODCS_SUPPORTED_VERSIONS))
            raise ValueError(f"ODCS apiVersion mismatch. Supported versions: {supported}, got {api_ver}")


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


def list_schema_objects(doc: OpenDataContractStandard) -> List[SchemaObject]:
    """Return all SchemaObject instances from the contract schema."""
    ensure_version(doc)
    return list(doc.schema_ or [])


def find_schema_object(
    doc: OpenDataContractStandard,
    name: str | None = None,
) -> Optional[SchemaObject]:
    """Find a SchemaObject by name, physicalName, or id, defaulting to the first."""
    objects = list_schema_objects(doc)
    if not objects:
        return None
    if name is None:
        return objects[0]
    for obj in objects:
        if (
            obj.name == name
            or getattr(obj, "physicalName", None) == name
            or getattr(obj, "id", None) == name
        ):
            return obj
    return None


def _get_server_attr(server: object, *attr_names: str) -> Optional[str]:
    """Safely extract non-callable attribute from a server object."""
    if server is None:
        return None
    for name in attr_names:
        if hasattr(server, "__dict__") and name in server.__dict__:
            val = server.__dict__[name]
            if val is not None and not callable(val):
                return str(val)
        val = getattr(server, name, None)
        if val is not None and not callable(val):
            return str(val)
    return None


def resolve_table_name(
    server: Optional[Server],
    schema_object: Optional[SchemaObject] = None,
) -> Optional[str]:
    """Derive fully-qualified table identifier from Server and SchemaObject."""
    object_name = None
    if schema_object is not None:
        object_name = getattr(schema_object, "physicalName", None) or getattr(
            schema_object, "name", None
        )
        if callable(object_name):
            object_name = None
        elif object_name is not None:
            object_name = str(object_name)

    if server is None:
        return object_name

    server_type = (_get_server_attr(server, "type") or "").lower()
    server_format = (_get_server_attr(server, "format") or "").lower()
    catalog = _get_server_attr(server, "catalog")
    schema_val = _get_server_attr(server, "schema_", "schema")
    database = _get_server_attr(server, "database")
    dataset = _get_server_attr(server, "dataset")
    project = _get_server_attr(server, "project")

    # Non-table server types (streaming, file storage) without table coordinates
    if server_type in ("stream", "streaming", "kafka", "s3", "adls", "abfss", "gcs", "file", "local", "filesystem") or server_format in ("rate", "memory", "kafka", "socket", "console"):
        if not (catalog or schema_val or database or project):
            return None

    # Databricks / Unity Catalog
    if server_type in ("databricks", "catalog", "unity", "delta") or catalog:
        parts = [p for p in (catalog, schema_val or database, object_name or dataset) if p]
        return ".".join(parts) if parts else None

    # BigQuery
    if server_type in ("bigquery", "googlecloudsql") or project:
        parts = [p for p in (project, dataset or schema_val, object_name) if p]
        return ".".join(parts) if parts else None

    # Snowflake / Postgres / SQL Databases
    if database or schema_val:
        filtered_parts: List[str] = []
        for p in (catalog, database, schema_val, object_name or dataset):
            if p and (not filtered_parts or p != filtered_parts[-1]):
                filtered_parts.append(p)
        return ".".join(filtered_parts) if filtered_parts else None

    if server_type in ("sql", "rdbms", "postgres", "postgresql", "mysql", "oracle", "sqlserver", "snowflake", "sqlite", "table", "view"):
        return object_name or dataset

    return None


def resolve_storage_path(
    server: Optional[Server],
    schema_object: Optional[SchemaObject] = None,
) -> Optional[str]:
    """Derive storage path / location from Server and SchemaObject."""
    if server is None:
        return None
    path = getattr(server, "path", None) or getattr(server, "location", None)
    if not path:
        return None
    return path


def build_odcs(
    *,
    contract_id: str,
    version: str,
    kind: str,
    api_version: str,
    name: str | None = None,
    description: str | None = None,
    physical_name: str | None = None,
    physical_type: str | None = None,
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
        schema_objects = [
            SchemaObject(
                name=name or contract_id,
                physicalName=physical_name,
                physicalType=physical_type,
                properties=properties or [],
            )
        ]
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
    "ODCS_SUPPORTED_VERSIONS",
    "BITOL_SCHEMA_URL",
    "as_odcs_dict",
    "odcs_package_version",
    "to_model",
    "ensure_version",
    "contract_identity",
    "list_properties",
    "list_schema_objects",
    "find_schema_object",
    "resolve_table_name",
    "resolve_storage_path",
    "field_map",
    "fingerprint",
    "normalise_custom_properties",
    "custom_properties_dict",
    "build_odcs",
]
