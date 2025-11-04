"""Open Data Product Standard helpers shared across dc43 packages.

This module prefers the canonical implementation that lives in
``dc43_service_backends.core`` when it is available so applications embedding
the backend distribution share a single source of truth for the ODPS model
helpers.  When that package (or the legacy ``dc43`` facade) is not installed –
for example when running the standalone service client test suite – the module
falls back to a self-contained implementation so the clients can continue to
operate.
"""

from __future__ import annotations

from importlib import import_module, util as importlib_util


_CORE_MODULES = (
    "dc43_core.odps",
    "dc43_service_backends.core.odps",
    "dc43.core.odps",
)

_core = None
for _module_name in _CORE_MODULES:  # pragma: no cover - exercised when core is available
    try:
        _spec = importlib_util.find_spec(_module_name)
    except ModuleNotFoundError:  # pragma: no cover - guard for namespace packages
        _spec = None
    if _spec is not None:
        _core = import_module(_module_name)
        break

if _core is not None:  # pragma: no cover - exercised when core is present
    DataProductInputPort = _core.DataProductInputPort
    DataProductOutputPort = _core.DataProductOutputPort
    OpenDataProductStandard = _core.OpenDataProductStandard
    as_odps_dict = _core.as_odps_dict
    evolve_to_draft = _core.evolve_to_draft
    next_draft_version = _core.next_draft_version
    to_model = _core.to_model
    ODPS_REQUIRED = _core.ODPS_REQUIRED

    __all__ = [
        "DataProductInputPort",
        "DataProductOutputPort",
        "OpenDataProductStandard",
        "as_odps_dict",
        "evolve_to_draft",
        "next_draft_version",
        "to_model",
        "ODPS_REQUIRED",
    ]
else:
    from dataclasses import dataclass, field
    from typing import Any, Dict, Iterable, List, Mapping, Optional
    import copy
    import os
    import re

    SEMVER_RE = re.compile(
        r"^(\d+)\.(\d+)\.(\d+)(?:-([0-9A-Za-z.-]+))?(?:\+([0-9A-Za-z.-]+))?$"
    )

    @dataclass(frozen=True)
    class SemVer:
        """Tiny SemVer parser/utility used for version checks."""

        major: int
        minor: int
        patch: int
        prerelease: Optional[str] = None
        build: Optional[str] = None

        def __str__(self) -> str:  # pragma: no cover - exercised via to_dict
            base = f"{self.major}.{self.minor}.{self.patch}"
            if self.prerelease:
                base += f"-{self.prerelease}"
            if self.build:
                base += f"+{self.build}"
            return base

        @staticmethod
        def parse(s: str) -> "SemVer":
            """Parse a ``MAJOR.MINOR.PATCH[-prerelease][+build]`` string."""

            match = SEMVER_RE.match(s)
            if not match:
                raise ValueError(f"Invalid semver: {s}")
            major, minor, patch, prerelease, build = match.groups()
            return SemVer(int(major), int(minor), int(patch), prerelease, build)

        def bump(self, level: str) -> "SemVer":
            """Return a new instance bumped at ``major``/``minor``/``patch`` level."""

            if level == "major":
                return SemVer(self.major + 1, 0, 0)
            if level == "minor":
                return SemVer(self.major, self.minor + 1, 0)
            if level == "patch":
                return SemVer(self.major, self.minor, self.patch + 1)
            raise ValueError("level must be one of: major, minor, patch")


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
            extra = _copy_unknown_fields(
                data,
                ["name", "version", "contractId", "customProperties", "authoritativeDefinitions"],
            )
            return cls(
                name=name,
                version=version,
                contract_id=contract_id,
                custom_properties=_normalise_custom_properties(data.get("customProperties")),
                authoritative_definitions=_normalise_custom_properties(
                    data.get("authoritativeDefinitions")
                ),
                extra=extra,
            )

        def to_dict(self) -> Dict[str, Any]:
            payload: Dict[str, Any] = {
                "name": self.name,
                "version": self.version,
                "contractId": self.contract_id,
            }
            if self.custom_properties:
                payload["customProperties"] = [
                    copy.deepcopy(item) for item in self.custom_properties
                ]
            if self.authoritative_definitions:
                payload["authoritativeDefinitions"] = [
                    copy.deepcopy(item) for item in self.authoritative_definitions
                ]
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
                authoritative_definitions=_normalise_custom_properties(
                    data.get("authoritativeDefinitions")
                ),
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
                payload["inputContracts"] = [
                    copy.deepcopy(item) for item in self.input_contracts
                ]
            if self.custom_properties:
                payload["customProperties"] = [
                    copy.deepcopy(item) for item in self.custom_properties
                ]
            if self.authoritative_definitions:
                payload["authoritativeDefinitions"] = [
                    copy.deepcopy(item) for item in self.authoritative_definitions
                ]
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
            status = str(data.get("status", "")).strip() or "draft"
            if not product_id:
                raise ValueError("Data product requires an id")
            output_ports_raw = data.get("outputPorts", [])
            input_ports_raw = data.get("inputPorts", [])
            custom_properties = _normalise_custom_properties(data.get("customProperties"))
            tags = [str(tag).strip() for tag in data.get("tags", []) if str(tag).strip()]
            known_fields = {
                "apiVersion",
                "id",
                "kind",
                "name",
                "description",
                "status",
                "version",
                "inputPorts",
                "outputPorts",
                "customProperties",
                "tags",
            }
            extra = _copy_unknown_fields(data, known_fields)
            instance = cls(
                id=product_id,
                status=status,
                api_version=api_version,
                kind=str(data.get("kind", "DataProduct")),
                version=str(data.get("version")) if data.get("version") else None,
                name=str(data.get("name")) if data.get("name") else None,
                description=data.get("description") if isinstance(data.get("description"), Mapping) else None,
                custom_properties=custom_properties,
                tags=tags,
                extra=extra,
            )
            instance.input_ports = [
                DataProductInputPort.from_dict(port)
                for port in input_ports_raw
                if isinstance(port, Mapping)
            ]
            instance.output_ports = [
                DataProductOutputPort.from_dict(port)
                for port in output_ports_raw
                if isinstance(port, Mapping)
            ]
            return instance

        def to_dict(self) -> Dict[str, Any]:
            payload: Dict[str, Any] = {
                "apiVersion": self.api_version,
                "id": self.id,
                "kind": self.kind,
                "status": self.status,
            }
            if self.version:
                payload["version"] = self.version
            if self.name:
                payload["name"] = self.name
            if self.description:
                payload["description"] = copy.deepcopy(self.description)
            if self.input_ports:
                payload["inputPorts"] = [port.to_dict() for port in self.input_ports]
            if self.output_ports:
                payload["outputPorts"] = [port.to_dict() for port in self.output_ports]
            if self.custom_properties:
                payload["customProperties"] = [
                    copy.deepcopy(item) for item in self.custom_properties
                ]
            if self.tags:
                payload["tags"] = [str(tag) for tag in self.tags]
            if self.extra:
                payload.update(copy.deepcopy(self.extra))
            return payload

        def clone(self) -> "OpenDataProductStandard":
            return OpenDataProductStandard.from_dict(self.to_dict())

        def find_input_port(self, name: str) -> Optional[DataProductInputPort]:
            return next((port for port in self.input_ports if port.name == name), None)

        def find_output_port(self, name: str) -> Optional[DataProductOutputPort]:
            return next((port for port in self.output_ports if port.name == name), None)

        def ensure_input_port(self, port: DataProductInputPort) -> bool:
            existing = self.find_input_port(port.name)
            if existing and existing.to_dict() == port.to_dict():
                return False
            if existing:
                self.input_ports = [
                    port if candidate.name == port.name else candidate
                    for candidate in self.input_ports
                ]
            else:
                self.input_ports.append(port)
            return True

        def ensure_output_port(self, port: DataProductOutputPort) -> bool:
            existing = self.find_output_port(port.name)
            if existing and existing.to_dict() == port.to_dict():
                return False
            if existing:
                self.output_ports = [
                    port if candidate.name == port.name else candidate
                    for candidate in self.output_ports
                ]
            else:
                self.output_ports.append(port)
            return True


    def _ensure_semver(candidate: str) -> str:
        try:
            SemVer.parse(candidate)
        except ValueError as exc:  # pragma: no cover - validation guard
            raise ValueError(f"Invalid version '{candidate}': {exc}") from exc
        return candidate


    def next_draft_version(existing_versions: Iterable[str]) -> str:
        versions = [SemVer.parse(version) for version in existing_versions]
        if not versions:
            return "0.1.0"
        latest = max(versions, key=lambda version: (version.major, version.minor, version.patch))
        if latest.major == 0 and latest.minor == 0:
            return str(latest.bump("patch"))
        return str(latest.bump("minor"))


    def evolve_to_draft(
        product: OpenDataProductStandard,
        *,
        existing_versions: Iterable[str],
        bump: str = "minor",
    ) -> OpenDataProductStandard:
        current_version = product.version
        if current_version:
            base = SemVer.parse(current_version)
            base = SemVer(base.major, base.minor, base.patch)
        else:
            base = SemVer.parse("0.1.0")
            if bump == "major":
                base = SemVer.parse("1.0.0")
            elif bump == "patch":
                base = SemVer.parse("0.0.1")
        versions = [SemVer.parse(version) for version in existing_versions]
        while True:
            candidate = base.bump(bump) if current_version else base
            if candidate not in versions:
                product.version = str(candidate)
                break
            base = candidate
        product.status = "draft"
        return product


    def as_odps_dict(product: OpenDataProductStandard) -> Dict[str, Any]:
        to_dict = getattr(product, "to_dict", None)
        if not callable(to_dict):
            raise TypeError(
                "Unsupported data product object: expected OpenDataProductStandard, "
                f"got {type(product).__name__}. Did you accidentally pass a data contract?"
            )
        payload = to_dict()
        payload["inputPorts"] = [port.to_dict() for port in product.input_ports]
        payload["outputPorts"] = [port.to_dict() for port in product.output_ports]
        return payload


    def to_model(payload: Mapping[str, Any]) -> OpenDataProductStandard:
        if not isinstance(payload, Mapping):
            raise TypeError("Payload must be a mapping")
        return OpenDataProductStandard.from_dict(payload)


    __all__ = [
        "DataProductInputPort",
        "DataProductOutputPort",
        "OpenDataProductStandard",
        "as_odps_dict",
        "evolve_to_draft",
        "next_draft_version",
        "to_model",
        "ODPS_REQUIRED",
    ]
