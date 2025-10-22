from __future__ import annotations

from dataclasses import fields
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
CONTRACTS_SRC = ROOT / "packages" / "dc43-contracts-app" / "src"
SERVICE_SRC = ROOT / "packages" / "dc43-service-backends" / "src"

for path in (CONTRACTS_SRC, SERVICE_SRC):
    if path.exists():
        sys.path.insert(0, str(path))

DOC_PATHS = (
    ROOT / "docs" / "configuration-reference.md",
    ROOT / "docs" / "service-backends-configuration.md",
)


def _combined_docs() -> str:
    return "\n".join(path.read_text(encoding="utf-8") for path in DOC_PATHS)


def _missing_fields(doc_text: str, *classes: type) -> dict[str, list[str]]:
    missing: dict[str, list[str]] = {}
    for cls in classes:
        for field in fields(cls):
            if field.name not in doc_text:
                missing.setdefault(cls.__name__, []).append(field.name)
    return missing


def test_setup_module_fields_have_documentation() -> None:
    pytest.importorskip("fastapi")
    pytest.importorskip("jinja2")
    pytest.importorskip("tomlkit")
    pytest.importorskip("httpx")
    from dc43_contracts_app import server  # type: ignore import

    doc_text = _combined_docs()
    missing = set()
    for module in server.SETUP_MODULES.values():
        for option in module.get("options", {}).values():
            for field in option.get("fields", []) or []:
                name = str(field.get("name") or "").strip()
                if name and name not in doc_text:
                    missing.add(name)
    assert not missing, f"Document configuration-reference.md is missing wizard fields: {sorted(missing)}"


def test_config_dataclasses_are_documented() -> None:
    from dc43_contracts_app.config import (  # type: ignore import
        BackendConfig,
        BackendProcessConfig,
        ContractsAppConfig,
        DocsChatConfig,
        WorkspaceConfig,
    )
    from dc43_service_backends.config import (  # type: ignore import
        AuthConfig,
        ContractStoreConfig,
        DataProductStoreConfig,
        DataQualityBackendConfig,
        GovernanceConfig,
        GovernanceStoreConfig,
        ServiceBackendsConfig,
        UnityCatalogConfig,
    )

    doc_text = _combined_docs()
    missing = _missing_fields(
        doc_text,
        ContractsAppConfig,
        WorkspaceConfig,
        BackendConfig,
        BackendProcessConfig,
        DocsChatConfig,
        ServiceBackendsConfig,
        ContractStoreConfig,
        DataProductStoreConfig,
        DataQualityBackendConfig,
        AuthConfig,
        UnityCatalogConfig,
        GovernanceConfig,
        GovernanceStoreConfig,
    )
    assert not missing, f"Configuration docs missing field descriptions: {missing}"
