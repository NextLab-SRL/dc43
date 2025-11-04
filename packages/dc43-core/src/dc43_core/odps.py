"""Open Data Product Standard (ODPS) helpers.

The Open Data Product Standard currently has no official Python model binding,
so dc43 ships a light-weight representation that focuses on the portions we
interact with programmatically: input/output port definitions and version
management.  The helper keeps the following goals in mind:

* Provide a structured representation for ODPS documents while preserving
  unknown attributes.
* Offer primitives to add or update input/output ports while ensuring the
  document evolves into a draft version.
* Keep the implementation dependency-free (relying only on the standard
  library) so projects embedding dc43 are not forced to install additional
  modelling frameworks.

The minimal model implemented here deliberately ignores large sections of the
ODPS schema that dc43 does not need (for example the SBOM, support contacts or
team definitions).  The raw payload is preserved so documents can be
round-tripped even when new fields are added by upstream specifications.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional
import copy
import os

from .versioning import SemVer


ODPS_REQUIRED = os.getenv("DC43_ODPS_REQUIRED", "1.0.0")


def _normalise_custom_properties(raw: Any) -> List[Dict[str, Any]]:
    if not raw:
        return []
    if isinstance(raw, list):
        return [dict(item) for item in raw if isinstance(item, Mapping)]
    if isinstance(raw, Mapping):
        return [dict(raw)]
    try:
        return [dict(item) for item in raw if isinstance(item, Mapping)]
    except TypeError:
        return []


def _copy_unknown_fields(
    data: Mapping[str, Any],
    known: Iterable[str],
) -> Dict[str, Any]:
    unknown: Dict[str, Any] = {}
    known_keys = {str(key) for key in known}
    for key, value in data.items():
        if key in known_keys:
            continue
        unknown[key] = copy.deepcopy(value)
    return unknown


@dataclass
class DataProductInputPort:
    """Representation of an ODPS input port."""

    name: str
    version: str
    contract_id: str
    custom_properties: List[Dict[str, Any]] = field(default_factory=list)
    authoritative_definitions: List[Dict[str, Any]] = field(default_factory=list)
    extra: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DataProductInputPort":
        name = str(data.get("name", "")).strip()
        version = str(data.get("version", "")).strip()
        contract_id = str(data.get("contractId", "")).strip()
        if not name or not version or not contract_id:
            raise ValueError("Input port requires name, version, and contractId")
        extra = _copy_unknown_fields(data, ["name", "version", "contractId", "customProperties", "authoritativeDefinitions"])
        return cls(
            name=name,
            version=version,
            contract_id=contract_id,
            custom_properties=_normalise_custom_properties(data.get("customProperties")),
            authoritative_definitions=_normalise_custom_properties(data.get("authoritativeDefinitions")),
            extra=extra,
        )

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "name": self.name,
            "version": self.version,
            "contractId": self.contract_id,
        }
        if self.custom_properties:
            payload["customProperties"] = [copy.deepcopy(item) for item in self.custom_properties]
        if self.authoritative_definitions:
            payload["authoritativeDefinitions"] = [copy.deepcopy(item) for item in self.authoritative_definitions]
        if self.extra:
            payload.update(copy.deepcopy(self.extra))
        return payload


@dataclass
class DataProductOutputPort:
    """Representation of an ODPS output port."""

    name: str
    version: str
    contract_id: str
    description: Optional[str] = None
    type: Optional[str] = None
    sbom: List[Dict[str, Any]] = field(default_factory=list)
    input_contracts: List[Dict[str, Any]] = field(default_factory=list)
    custom_properties: List[Dict[str, Any]] = field(default_factory=list)
    authoritative_definitions: List[Dict[str, Any]] = field(default_factory=list)
    extra: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DataProductOutputPort":
        name = str(data.get("name", "")).strip()
        version = str(data.get("version", "")).strip()
        if not name or not version:
            raise ValueError("Output port requires name and version")
        contract_value = data.get("contractId")
        contract_id = str(contract_value).strip() if contract_value is not None else ""
        if not contract_id:
            raise ValueError("Output port requires contractId")
        known_fields = {
            "name",
            "version",
            "contractId",
            "description",
            "type",
            "sbom",
            "inputContracts",
            "customProperties",
            "authoritativeDefinitions",
        }
        extra = _copy_unknown_fields(data, known_fields)
        return cls(
            name=name,
            version=version,
            contract_id=contract_id,
            description=str(data["description"]).strip() if data.get("description") else None,
            type=str(data["type"]).strip() if data.get("type") else None,
            sbom=_normalise_custom_properties(data.get("sbom")),
            input_contracts=_normalise_custom_properties(data.get("inputContracts")),
            custom_properties=_normalise_custom_properties(data.get("customProperties")),
            authoritative_definitions=_normalise_custom_properties(data.get("authoritativeDefinitions")),
            extra=extra,
        )

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "name": self.name,
            "version": self.version,
            "contractId": self.contract_id,
        }
        if self.description:
            payload["description"] = self.description
        if self.type:
            payload["type"] = self.type
        if self.sbom:
            payload["sbom"] = [copy.deepcopy(item) for item in self.sbom]
        if self.input_contracts:
            payload["inputContracts"] = [copy.deepcopy(item) for item in self.input_contracts]
        if self.custom_properties:
            payload["customProperties"] = [copy.deepcopy(item) for item in self.custom_properties]
        if self.authoritative_definitions:
            payload["authoritativeDefinitions"] = [copy.deepcopy(item) for item in self.authoritative_definitions]
        if self.extra:
            payload.update(copy.deepcopy(self.extra))
        return payload


@dataclass
class OpenDataProductStandard:
    """Minimal representation of an ODPS document."""

    id: str
    status: str
    api_version: str = ODPS_REQUIRED
    kind: str = "DataProduct"
    version: Optional[str] = None
    name: Optional[str] = None
    description: Optional[Mapping[str, Any]] = None
    input_ports: List[DataProductInputPort] = field(default_factory=list)
    output_ports: List[DataProductOutputPort] = field(default_factory=list)
    custom_properties: List[Dict[str, Any]] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    extra: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "OpenDataProductStandard":
        api_version = str(data.get("apiVersion", "")).strip() or ODPS_REQUIRED
        if api_version != ODPS_REQUIRED:
            raise ValueError(
                f"ODPS apiVersion mismatch. Required {ODPS_REQUIRED}, got {api_version}"
            )
        product_id = str(data.get("id", "")).strip()
        status = str(data.get("status", "")).strip()
        if not product_id or not status:
            raise ValueError("ODPS document requires 'id' and 'status'")
        input_ports = []
        for item in data.get("inputPorts", []) or []:
            if isinstance(item, Mapping):
                input_ports.append(DataProductInputPort.from_dict(item))
        output_ports = []
        for item in data.get("outputPorts", []) or []:
            if isinstance(item, Mapping):
                output_ports.append(DataProductOutputPort.from_dict(item))
        known_fields = {
            "apiVersion",
            "kind",
            "id",
            "status",
            "version",
            "name",
            "description",
            "inputPorts",
            "outputPorts",
            "customProperties",
            "tags",
        }
        extra = _copy_unknown_fields(data, known_fields)
        return cls(
            api_version=api_version,
            kind=str(data.get("kind", "DataProduct")) or "DataProduct",
            id=product_id,
            status=status,
            version=str(data.get("version", "")).strip() or None,
            name=str(data.get("name", "")).strip() or None,
            description=(
                copy.deepcopy(data.get("description"))
                if isinstance(data.get("description"), Mapping)
                else None
            ),
            input_ports=input_ports,
            output_ports=output_ports,
            custom_properties=_normalise_custom_properties(data.get("customProperties")),
            tags=[str(tag) for tag in data.get("tags", []) if isinstance(tag, str)],
            extra=extra,
        )

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "apiVersion": self.api_version,
            "kind": self.kind,
            "id": self.id,
            "status": self.status,
        }
        if self.version:
            payload["version"] = self.version
        if self.name:
            payload["name"] = self.name
        if self.description:
            payload["description"] = copy.deepcopy(self.description)
        if self.custom_properties:
            payload["customProperties"] = [copy.deepcopy(item) for item in self.custom_properties]
        if self.tags:
            payload["tags"] = list(self.tags)
        if self.input_ports:
            payload["inputPorts"] = [port.to_dict() for port in self.input_ports]
        if self.output_ports:
            payload["outputPorts"] = [port.to_dict() for port in self.output_ports]
        if self.extra:
            payload.update(copy.deepcopy(self.extra))
        return payload

    def clone(self) -> "OpenDataProductStandard":
        return OpenDataProductStandard.from_dict(self.to_dict())

    def find_input_port(self, name: str) -> Optional[DataProductInputPort]:
        for port in self.input_ports:
            if port.name == name:
                return port
        return None

    def find_output_port(self, name: str) -> Optional[DataProductOutputPort]:
        for port in self.output_ports:
            if port.name == name:
                return port
        return None

    def ensure_input_port(self, port: DataProductInputPort) -> bool:
        existing = self.find_input_port(port.name)
        if existing and existing.contract_id == port.contract_id and existing.version == port.version:
            return False
        if existing:
            self.input_ports = [p for p in self.input_ports if p.name != port.name]
        self.input_ports.append(port)
        return True

    def ensure_output_port(self, port: DataProductOutputPort) -> bool:
        existing = self.find_output_port(port.name)
        if (
            existing
            and existing.contract_id == port.contract_id
            and existing.version == port.version
        ):
            return False
        if existing:
            self.output_ports = [p for p in self.output_ports if p.name != port.name]
        self.output_ports.append(port)
        return True


def as_odps_dict(doc: OpenDataProductStandard) -> Dict[str, Any]:
    to_dict = getattr(doc, "to_dict", None)
    if callable(to_dict):
        return to_dict()
    raise TypeError(
        "Unsupported data product object: expected OpenDataProductStandard, "
        f"got {type(doc).__name__}. Did you accidentally pass a data contract?"
    )


def to_model(data: Mapping[str, Any]) -> OpenDataProductStandard:
    return OpenDataProductStandard.from_dict(data)


def next_draft_version(
    *,
    current_version: Optional[str],
    existing_versions: Iterable[str],
    bump: str = "minor",
) -> str:
    """Return the next draft version string for a data product."""

    existing_set = {str(value) for value in existing_versions}
    if current_version:
        base = SemVer.parse(current_version)
        base = SemVer(base.major, base.minor, base.patch)
        candidate = base.bump(bump)
    else:
        # Follow the same defaults used for contracts: start at 0.1.0
        candidate = SemVer.parse("0.1.0")
        if bump == "major":
            candidate = SemVer.parse("1.0.0")
        elif bump == "patch":
            candidate = SemVer.parse("0.0.1")
    suffix = f"{candidate.major}.{candidate.minor}.{candidate.patch}-draft"
    version = suffix
    counter = 1
    while version in existing_set:
        counter += 1
        version = f"{suffix}.{counter}"
    return version


def evolve_to_draft(
    doc: OpenDataProductStandard,
    *,
    existing_versions: Iterable[str],
    bump: str = "minor",
) -> None:
    """Update ``doc`` to represent a draft version."""

    doc.version = next_draft_version(
        current_version=doc.version,
        existing_versions=existing_versions,
        bump=bump,
    )
    doc.status = "draft"


__all__ = [
    "ODPS_REQUIRED",
    "DataProductInputPort",
    "DataProductOutputPort",
    "OpenDataProductStandard",
    "as_odps_dict",
    "evolve_to_draft",
    "next_draft_version",
    "to_model",
]

