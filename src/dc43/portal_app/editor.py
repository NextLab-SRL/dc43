from __future__ import annotations

"""Helpers for the contract editor experience in the portal."""

import json
from collections import Counter
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from fastapi import Request
from fastapi.encoders import jsonable_encoder
from open_data_contract_standard.model import (  # type: ignore
    CustomProperty,
    DataQuality,
    Description,
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
    Server,
    ServiceLevelAgreementProperty,
    Support,
)
from packaging.version import Version
from pydantic import ValidationError

from dc43.odcs import normalise_custom_properties
from dc43.versioning import SemVer

_STATUS_OPTIONS: List[Tuple[str, str]] = [
    ("", "Unspecified"),
    ("draft", "Draft"),
    ("active", "Active"),
    ("deprecated", "Deprecated"),
    ("retired", "Retired"),
    ("suspended", "Suspended"),
]

_VERSIONING_MODES: List[Tuple[str, str]] = [
    ("", "Not specified"),
    ("delta", "Delta (time-travel compatible)"),
    ("snapshot", "Snapshot folders"),
    ("append", "Append-only log"),
]

_EXPECTATION_KEYS = (
    "mustBe",
    "mustNotBe",
    "mustBeGreaterThan",
    "mustBeGreaterOrEqualTo",
    "mustBeLessThan",
    "mustBeLessOrEqualTo",
    "mustBeBetween",
    "mustNotBeBetween",
    "query",
)

_SERVER_FIELD_MAP = {
    "description": "description",
    "environment": "environment",
    "format": "format",
    "path": "path",
    "dataset": "dataset",
    "database": "database",
    "schema": "schema_",
    "catalog": "catalog",
    "host": "host",
    "location": "location",
    "endpointUrl": "endpointUrl",
    "project": "project",
    "region": "region",
    "regionName": "regionName",
    "serviceName": "serviceName",
    "warehouse": "warehouse",
    "stagingDir": "stagingDir",
    "account": "account",
}


def _stringify_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (bool, int, float)):
        return json.dumps(value)
    if isinstance(value, (list, dict)):
        return json.dumps(value, indent=2, sort_keys=True)
    return str(value)


def _parse_json_value(raw: Any) -> Any:
    if raw is None:
        return None
    if isinstance(raw, (dict, list, bool, int, float)):
        return raw
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text
    return raw


def _as_bool(value: Any) -> Optional[bool]:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "y", "on"}:
            return True
        if lowered in {"false", "0", "no", "n", "off"}:
            return False
    return bool(value)


def _as_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return int(value)
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Expected integer value, got {value!r}") from exc


def _custom_properties_state(raw: Any) -> List[Dict[str, str]]:
    state: List[Dict[str, str]] = []
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
            state.append({"property": str(key), "value": _stringify_value(value)})
    return state


def _quality_state(items: Optional[Iterable[Any]]) -> List[Dict[str, Any]]:
    state: List[Dict[str, Any]] = []
    if not items:
        return state
    for item in items:
        if hasattr(item, "model_dump"):
            raw = item.model_dump(exclude_none=True)
        elif hasattr(item, "dict"):
            raw = item.dict(exclude_none=True)  # type: ignore[attr-defined]
        else:
            raw = {k: v for k, v in vars(item).items() if v is not None}
        expectation = None
        expectation_value = None
        for key in _EXPECTATION_KEYS:
            if key in raw:
                expectation = key
                expectation_value = raw.pop(key)
                break
        for key, value in list(raw.items()):
            if isinstance(value, (list, dict)):
                raw[key] = json.dumps(value, indent=2, sort_keys=True)
        entry: Dict[str, Any] = {k: v for k, v in raw.items() if v is not None}
        if expectation:
            entry["expectation"] = expectation
            if isinstance(expectation_value, list):
                entry["expectationValue"] = ", ".join(str(v) for v in expectation_value)
            elif isinstance(expectation_value, (dict, list)):
                entry["expectationValue"] = json.dumps(expectation_value, indent=2, sort_keys=True)
            elif expectation_value is None:
                entry["expectationValue"] = ""
            else:
                entry["expectationValue"] = str(expectation_value)
        state.append(entry)
    return state


def _schema_property_state(prop: SchemaProperty) -> Dict[str, Any]:
    examples = getattr(prop, "examples", None) or []
    return {
        "name": getattr(prop, "name", "") or "",
        "physicalType": getattr(prop, "physicalType", "") or "",
        "description": getattr(prop, "description", "") or "",
        "businessName": getattr(prop, "businessName", "") or "",
        "logicalType": getattr(prop, "logicalType", "") or "",
        "logicalTypeOptions": _stringify_value(getattr(prop, "logicalTypeOptions", None)),
        "required": bool(getattr(prop, "required", False)),
        "unique": bool(getattr(prop, "unique", False)),
        "partitioned": bool(getattr(prop, "partitioned", False)),
        "primaryKey": bool(getattr(prop, "primaryKey", False)),
        "classification": getattr(prop, "classification", "") or "",
        "examples": "\n".join(str(item) for item in examples),
        "customProperties": _custom_properties_state(getattr(prop, "customProperties", None)),
        "quality": _quality_state(getattr(prop, "quality", None)),
    }


def _schema_object_state(obj: SchemaObject) -> Dict[str, Any]:
    properties = [
        _schema_property_state(prop)
        for prop in getattr(obj, "properties", None) or []
    ]
    return {
        "name": getattr(obj, "name", "") or "",
        "description": getattr(obj, "description", "") or "",
        "businessName": getattr(obj, "businessName", "") or "",
        "logicalType": getattr(obj, "logicalType", "") or "",
        "customProperties": _custom_properties_state(getattr(obj, "customProperties", None)),
        "quality": _quality_state(getattr(obj, "quality", None)),
        "properties": properties,
    }


def _server_state(server: Server) -> Dict[str, Any]:
    state = {
        "server": getattr(server, "server", "") or "",
        "type": getattr(server, "type", "") or "",
        "port": getattr(server, "port", None) or "",
    }
    for field, attr in _SERVER_FIELD_MAP.items():
        state[field] = getattr(server, attr, "") or ""
    versioning_value: Any | None = None
    path_pattern_value: Any | None = None
    custom_entries: List[Dict[str, str]] = []
    for item in normalise_custom_properties(getattr(server, "customProperties", None)):
        key = None
        value = None
        if isinstance(item, Mapping):
            key = item.get("property")
            value = item.get("value")
        else:
            key = getattr(item, "property", None)
            value = getattr(item, "value", None)
        if not key:
            continue
        if str(key) == "dc43.versioning":
            versioning_value = value
            continue
        if str(key) == "dc43.pathPattern":
            path_pattern_value = value
            continue
        custom_entries.append({"property": str(key), "value": _stringify_value(value)})
    if versioning_value is not None:
        parsed = versioning_value
        if isinstance(parsed, str):
            parsed = _parse_json_value(parsed)
        state["versioningConfig"] = parsed if isinstance(parsed, Mapping) else None
    if path_pattern_value not in (None, ""):
        state["pathPattern"] = str(path_pattern_value)
    state["customProperties"] = custom_entries
    return state


def _support_state(items: Optional[Iterable[Support]]) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    if not items:
        return result
    for entry in items:
        payload: Dict[str, Any] = {}
        for field in ("channel", "url", "description", "tool", "scope", "invitationUrl"):
            value = getattr(entry, field, None)
            if value:
                payload[field] = value
        if payload:
            result.append(payload)
    return result


def _sla_state(items: Optional[Iterable[ServiceLevelAgreementProperty]]) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    if not items:
        return result
    for entry in items:
        payload: Dict[str, Any] = {}
        for field in ("property", "value", "valueExt", "unit", "element", "driver"):
            value = getattr(entry, field, None)
            if value is None:
                continue
            if field in {"value", "valueExt"}:
                payload[field] = _stringify_value(value)
            else:
                payload[field] = value
        if payload:
            result.append(payload)
    return result


def contract_editor_state(contract: Optional[OpenDataContractStandard] = None) -> Dict[str, Any]:
    if contract is None:
        return {
            "id": "",
            "version": "",
            "kind": "DataContract",
            "apiVersion": "3.0.2",
            "name": "",
            "description": "",
            "status": "",
            "domain": "",
            "dataProduct": "",
            "tenant": "",
            "tags": [],
            "customProperties": [],
            "servers": [],
            "schemaObjects": [
                {
                    "name": "",
                    "description": "",
                    "businessName": "",
                    "logicalType": "",
                    "customProperties": [],
                    "quality": [],
                    "properties": [],
                }
            ],
            "support": [],
            "slaProperties": [],
        }
    description = (
        getattr(contract.description, "usage", "")
        if getattr(contract, "description", None)
        else ""
    )
    state = {
        "id": getattr(contract, "id", "") or "",
        "version": getattr(contract, "version", "") or "",
        "kind": getattr(contract, "kind", "DataContract") or "DataContract",
        "apiVersion": getattr(contract, "apiVersion", "3.0.2") or "3.0.2",
        "name": getattr(contract, "name", "") or "",
        "description": description,
        "status": getattr(contract, "status", "") or "",
        "domain": getattr(contract, "domain", "") or "",
        "dataProduct": getattr(contract, "dataProduct", "") or "",
        "tenant": getattr(contract, "tenant", "") or "",
        "tags": list(getattr(contract, "tags", []) or []),
        "customProperties": _custom_properties_state(getattr(contract, "customProperties", None)),
        "servers": [_server_state(server) for server in getattr(contract, "servers", []) or []],
        "schemaObjects": [
            _schema_object_state(obj) for obj in getattr(contract, "schema_", None) or []
        ],
        "support": _support_state(getattr(contract, "support", None)),
        "slaProperties": _sla_state(getattr(contract, "slaProperties", None)),
    }
    if not state["schemaObjects"]:
        state["schemaObjects"] = contract_editor_state(None)["schemaObjects"]
    return state


def _sorted_versions(values: Iterable[str]) -> List[str]:
    parsed: List[Tuple[Version, str]] = []
    invalid: List[str] = []
    for value in values:
        if not value:
            continue
        try:
            parsed.append((Version(str(value)), str(value)))
        except Exception:
            invalid.append(str(value))
    parsed.sort(key=lambda entry: entry[0])
    return [ver for _, ver in parsed] + sorted(invalid)


def contract_to_dict(contract: OpenDataContractStandard) -> Dict[str, Any]:
    try:
        return contract.model_dump(by_alias=True, exclude_none=True)
    except AttributeError:  # pragma: no cover - Pydantic v1 fallback
        return contract.dict(by_alias=True, exclude_none=True)  # type: ignore[call-arg]


def _build_editor_meta(
    *,
    store: Any,
    editor_state: Mapping[str, Any],
    editing: bool,
    original_version: Optional[str],
    baseline_state: Optional[Mapping[str, Any]],
    baseline_contract: Optional[OpenDataContractStandard],
) -> Dict[str, Any]:
    existing_contracts = sorted(store.list_contracts()) if store else []
    version_map: Dict[str, List[str]] = {}
    for contract_id in existing_contracts:
        try:
            versions = store.list_versions(contract_id)
        except FileNotFoundError:
            versions = []
        version_map[contract_id] = _sorted_versions(versions)
    meta: Dict[str, Any] = {
        "existingContracts": existing_contracts,
        "existingVersions": version_map,
        "editing": editing,
        "originalVersion": original_version,
        "contractId": str(editor_state.get("id", ""))
        or (getattr(baseline_contract, "id", "") if baseline_contract else ""),
    }
    if original_version:
        meta["baseVersion"] = original_version
    if baseline_state is None and baseline_contract is not None:
        baseline_state = contract_editor_state(baseline_contract)
    if baseline_state is not None:
        meta["baselineState"] = jsonable_encoder(baseline_state)
    if baseline_contract is not None:
        meta["baseContract"] = contract_to_dict(baseline_contract)
    return meta


def editor_context(
    request: Request,
    *,
    store: Any,
    editor_state: Dict[str, Any],
    editing: bool = False,
    original_version: Optional[str] = None,
    baseline_state: Optional[Mapping[str, Any]] = None,
    baseline_contract: Optional[OpenDataContractStandard] = None,
    error: Optional[str] = None,
) -> Dict[str, Any]:
    context = {
        "request": request,
        "editing": editing,
        "editor_state": editor_state,
        "status_options": _STATUS_OPTIONS,
        "versioning_modes": _VERSIONING_MODES,
        "editor_meta": _build_editor_meta(
            store=store,
            editor_state=editor_state,
            editing=editing,
            original_version=original_version,
            baseline_state=baseline_state,
            baseline_contract=baseline_contract,
        ),
    }
    if original_version:
        context["original_version"] = original_version
    if error:
        context["error"] = error
    return context


def _custom_properties_models(
    items: Optional[Iterable[Mapping[str, Any]]]
) -> List[CustomProperty] | None:
    result: List[CustomProperty] = []
    if not items:
        return None
    for item in items:
        if not isinstance(item, Mapping):
            continue
        key = (str(item.get("property", ""))).strip()
        if not key:
            continue
        value = _parse_json_value(item.get("value"))
        result.append(CustomProperty(property=key, value=value))
    return result or None


def _parse_expectation_value(expectation: str, value: Any) -> Any:
    if value is None or value == "":
        return None
    if isinstance(value, (list, dict, bool, int, float)):
        return value
    if not isinstance(value, str):
        return value
    text = value.strip()
    if expectation in {"mustBeBetween", "mustNotBeBetween"}:
        separators = [",", ";"]
        for sep in separators:
            if sep in text:
                parts = [p.strip() for p in text.split(sep) if p.strip()]
                break
        else:
            parts = [p.strip() for p in text.split() if p.strip()]
        if len(parts) < 2:
            raise ValueError("Data quality range requires two numeric values")
        try:
            return [float(parts[0]), float(parts[1])]
        except ValueError as exc:
            raise ValueError("Data quality range must be numeric") from exc
    if expectation in {
        "mustBeGreaterThan",
        "mustBeGreaterOrEqualTo",
        "mustBeLessThan",
        "mustBeLessOrEqualTo",
    }:
        try:
            return float(text)
        except ValueError as exc:
            raise ValueError(
                f"Expectation {expectation} requires a numeric value"
            ) from exc
    if expectation in {"mustBe", "mustNotBe"}:
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text
    return text


def _quality_models(
    items: Optional[Iterable[Mapping[str, Any]]]
) -> List[DataQuality] | None:
    result: List[DataQuality] = []
    if not items:
        return None
    for item in items:
        if not isinstance(item, Mapping):
            continue
        payload: Dict[str, Any] = {}
        for field in (
            "name",
            "type",
            "rule",
            "description",
            "dimension",
            "severity",
            "unit",
            "schedule",
            "scheduler",
            "businessImpact",
            "method",
        ):
            value = item.get(field)
            if value not in (None, ""):
                payload[field] = value
        tags_value = item.get("tags")
        if isinstance(tags_value, str):
            tags = [t.strip() for t in tags_value.split(",") if t.strip()]
            if tags:
                payload["tags"] = tags
        elif isinstance(tags_value, Iterable):
            tags = [str(t).strip() for t in tags_value if str(t).strip()]
            if tags:
                payload["tags"] = tags
        expectation = item.get("expectation")
        if expectation:
            payload[expectation] = _parse_expectation_value(
                expectation, item.get("expectationValue")
            )
        implementation = item.get("implementation")
        if implementation not in (None, ""):
            payload["implementation"] = _parse_json_value(implementation)
        custom_props = _custom_properties_models(item.get("customProperties"))
        if custom_props:
            payload["customProperties"] = custom_props
        if payload:
            result.append(DataQuality(**payload))
    return result or None


def _schema_properties_models(
    items: Optional[Iterable[Mapping[str, Any]]]
) -> List[SchemaProperty]:
    result: List[SchemaProperty] = []
    if not items:
        return result
    for item in items:
        if not isinstance(item, Mapping):
            continue
        name = (str(item.get("name", ""))).strip()
        if not name:
            continue
        payload: Dict[str, Any] = {"name": name}
        physical_type = item.get("physicalType")
        if physical_type:
            payload["physicalType"] = physical_type
        for field in (
            "description",
            "businessName",
            "classification",
            "logicalType",
        ):
            value = item.get(field)
            if value not in (None, ""):
                payload[field] = value
        logical_type_options = item.get("logicalTypeOptions")
        if logical_type_options not in (None, ""):
            payload["logicalTypeOptions"] = _parse_json_value(logical_type_options)
        for boolean_field in ("required", "unique", "partitioned", "primaryKey"):
            value = _as_bool(item.get(boolean_field))
            if value is not None:
                payload[boolean_field] = value
        examples = item.get("examples")
        if isinstance(examples, str):
            values = [ex.strip() for ex in examples.splitlines() if ex.strip()]
            if values:
                payload["examples"] = values
        elif isinstance(examples, Iterable):
            values = [str(ex).strip() for ex in examples if str(ex).strip()]
            if values:
                payload["examples"] = values
        custom_props = _custom_properties_models(item.get("customProperties"))
        if custom_props:
            payload["customProperties"] = custom_props
        quality = _quality_models(item.get("quality"))
        if quality:
            payload["quality"] = quality
        result.append(SchemaProperty(**payload))
    return result


def _schema_objects_models(
    items: Optional[Iterable[Mapping[str, Any]]]
) -> List[SchemaObject]:
    result: List[SchemaObject] = []
    if not items:
        return result
    for item in items:
        if not isinstance(item, Mapping):
            continue
        name = (str(item.get("name", ""))).strip()
        payload: Dict[str, Any] = {}
        if name:
            payload["name"] = name
        for field in ("description", "businessName", "logicalType"):
            value = item.get(field)
            if value not in (None, ""):
                payload[field] = value
        custom_props = _custom_properties_models(item.get("customProperties"))
        if custom_props:
            payload["customProperties"] = custom_props
        quality = _quality_models(item.get("quality"))
        if quality:
            payload["quality"] = quality
        properties = _schema_properties_models(item.get("properties"))
        if properties:
            name_counts = Counter(
                prop.name for prop in properties if getattr(prop, "name", None)
            )
            duplicates = [name for name, count in name_counts.items() if count > 1]
            if duplicates:
                object_name = payload.get("name") or "schema object"
                dup_list = ", ".join(sorted(duplicates))
                raise ValueError(
                    f"Duplicate field name(s) {dup_list} in {object_name}"
                )
        payload["properties"] = properties
        result.append(SchemaObject(**payload))
    return result


def _server_models(
    items: Optional[Iterable[Mapping[str, Any]]]
) -> List[Server] | None:
    result: List[Server] = []
    if not items:
        return None
    for item in items:
        if not isinstance(item, Mapping):
            continue
        server_name = (str(item.get("server", ""))).strip()
        server_type = (str(item.get("type", ""))).strip()
        if not server_name or not server_type:
            continue
        payload: Dict[str, Any] = {"server": server_name, "type": server_type}
        for field, attr in _SERVER_FIELD_MAP.items():
            value = item.get(field)
            if value not in (None, ""):
                payload[attr] = value
        port_value = item.get("port")
        if port_value not in (None, ""):
            payload["port"] = _as_int(port_value)
        custom_props: List[CustomProperty] = []
        base_custom = _custom_properties_models(item.get("customProperties"))
        if base_custom:
            custom_props.extend(base_custom)
        versioning_config = item.get("versioningConfig")
        if versioning_config not in (None, "", {}):
            parsed_versioning = (
                versioning_config
                if isinstance(versioning_config, Mapping)
                else _parse_json_value(versioning_config)
            )
            if not isinstance(parsed_versioning, Mapping):
                raise ValueError("dc43.versioning must be provided as an object")
            custom_props.append(
                CustomProperty(property="dc43.versioning", value=parsed_versioning)
            )
        path_pattern = item.get("pathPattern")
        if path_pattern not in (None, ""):
            custom_props.append(
                CustomProperty(property="dc43.pathPattern", value=str(path_pattern))
            )
        if custom_props:
            payload["customProperties"] = custom_props
        result.append(Server(**payload))
    return result or None


def _support_models(
    items: Optional[Iterable[Mapping[str, Any]]]
) -> List[Support] | None:
    result: List[Support] = []
    if not items:
        return None
    for item in items:
        if not isinstance(item, Mapping):
            continue
        channel = (str(item.get("channel", ""))).strip()
        if not channel:
            continue
        payload: Dict[str, Any] = {"channel": channel}
        for field in ("url", "description", "tool", "scope", "invitationUrl"):
            value = item.get(field)
            if value:
                payload[field] = value
        result.append(Support(**payload))
    return result or None


def _sla_models(
    items: Optional[Iterable[Mapping[str, Any]]]
) -> List[ServiceLevelAgreementProperty] | None:
    result: List[ServiceLevelAgreementProperty] = []
    if not items:
        return None
    for item in items:
        if not isinstance(item, Mapping):
            continue
        key = (str(item.get("property", ""))).strip()
        if not key:
            continue
        payload: Dict[str, Any] = {"property": key}
        for field in ("unit", "element", "driver"):
            value = item.get(field)
            if value:
                payload[field] = value
        value = item.get("value")
        if value not in (None, ""):
            payload["value"] = _parse_json_value(value)
        value_ext = item.get("valueExt")
        if value_ext not in (None, ""):
            payload["valueExt"] = _parse_json_value(value_ext)
        result.append(ServiceLevelAgreementProperty(**payload))
    return result or None


def _normalise_tags(value: Any) -> List[str] | None:
    if value in (None, ""):
        return None
    if isinstance(value, str):
        tags = [item.strip() for item in value.split(",") if item.strip()]
        return tags or None
    if isinstance(value, Iterable):
        tags = [str(item).strip() for item in value if str(item).strip()]
        return tags or None
    return None


def build_contract_from_payload(payload: Mapping[str, Any]) -> OpenDataContractStandard:
    contract_id = (str(payload.get("id", ""))).strip()
    if not contract_id:
        raise ValueError("Contract ID is required")
    version = (str(payload.get("version", ""))).strip()
    if not version:
        raise ValueError("Version is required")
    name = (str(payload.get("name", "")) or contract_id).strip()
    description = str(payload.get("description", ""))
    kind = (str(payload.get("kind", "DataContract")) or "DataContract").strip()
    api_version = (str(payload.get("apiVersion", "3.0.2")) or "3.0.2").strip()
    status = str(payload.get("status", "")).strip() or None
    domain = str(payload.get("domain", "")).strip() or None
    data_product = str(payload.get("dataProduct", "")).strip() or None
    tenant = str(payload.get("tenant", "")).strip() or None
    tags = _normalise_tags(payload.get("tags"))
    custom_props = _custom_properties_models(payload.get("customProperties"))
    servers = _server_models(payload.get("servers"))
    schema_objects = _schema_objects_models(payload.get("schemaObjects"))
    if not schema_objects:
        raise ValueError("At least one schema object with fields is required")
    for obj in schema_objects:
        if not obj.properties:
            raise ValueError("Each schema object must define at least one field")
    support_entries = _support_models(payload.get("support"))
    sla_properties = _sla_models(payload.get("slaProperties"))
    return OpenDataContractStandard(
        version=version,
        kind=kind,
        apiVersion=api_version,
        id=contract_id,
        name=name,
        description=None if not description else Description(usage=description),
        status=status,
        domain=domain,
        dataProduct=data_product,
        tenant=tenant,
        tags=tags,
        customProperties=custom_props,
        servers=servers,
        schema=schema_objects,  # type: ignore[arg-type]
        support=support_entries,
        slaProperties=sla_properties,
    )


def validate_contract_payload(
    payload: Mapping[str, Any],
    *,
    store: Any,
    editing: bool,
    base_contract_id: Optional[str] = None,
    base_version: Optional[str] = None,
) -> None:
    contract_id = (str(payload.get("id", ""))).strip()
    if not contract_id:
        raise ValueError("Contract ID is required")
    version = (str(payload.get("version", ""))).strip()
    if not version:
        raise ValueError("Version is required")
    try:
        new_version = SemVer.parse(version)
    except ValueError as exc:
        raise ValueError(f"Invalid semantic version: {exc}") from exc
    existing_contracts: Sequence[str] = store.list_contracts() if store else []
    existing_versions = (
        set(store.list_versions(contract_id)) if contract_id in existing_contracts else set()
    )
    if editing:
        if base_contract_id and contract_id != base_contract_id:
            raise ValueError("Contract ID cannot be changed while editing")
        if base_version:
            try:
                prior = SemVer.parse(base_version)
            except ValueError:
                prior = None
            if prior and (
                (new_version.major, new_version.minor, new_version.patch)
                <= (prior.major, prior.minor, prior.patch)
            ):
                raise ValueError(
                    f"Version {version} must be greater than {base_version}"
                )
        if version in existing_versions:
            raise ValueError(
                f"Version {version} is already stored for contract {contract_id}"
            )
    else:
        if contract_id in existing_contracts and version in existing_versions:
            raise ValueError(
                f"Contract {contract_id} already has a version {version}."
            )


def next_version(ver: str) -> str:
    parsed = Version(ver)
    return f"{parsed.major}.{parsed.minor}.{parsed.micro + 1}"


__all__ = [
    "build_contract_from_payload",
    "contract_editor_state",
    "contract_to_dict",
    "editor_context",
    "next_version",
    "validate_contract_payload",
    "ValidationError",
]
