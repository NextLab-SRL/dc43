"""Compatibility facade exposing contracts helpers for the demo pipeline."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Mapping

from dc43_service_clients.contracts.client.local import LocalContractServiceClient
from dc43_service_clients.data_products.client.local import (
    LocalDataProductServiceClient,
)
from dc43_service_clients.data_quality.client.local import LocalDataQualityServiceClient
from dc43_service_clients.governance.client.local import (
    LocalGovernanceServiceClient,
    build_local_governance_service,
)
try:  # pragma: no cover - optional dependency when contracts app missing
    from dc43_contracts_app import server as contracts_server
except ImportError:  # pragma: no cover - demo can operate without the UI package
    contracts_server = None  # type: ignore[assignment]
try:
    from dc43_service_backends.data_products import LocalDataProductServiceBackend
except ModuleNotFoundError:  # pragma: no cover - fallback when backends missing
    from dc43_service_clients.testing import LocalDataProductServiceBackend

from .contracts_records import (
    DatasetRecord,
    dq_version_records,
    get_store,
    load_data_product_documents,
    load_data_product_payloads,
    load_contract_meta,
    load_records,
    pop_flash,
    queue_flash,
    scenario_run_rows,
)
from .contracts_workspace import (
    current_workspace,
    prepare_demo_workspace,
    refresh_dataset_aliases as _refresh_dataset_aliases,
    register_dataset_version as _register_dataset_version,
    set_active_version as _set_active_version,
)

prepare_demo_workspace()
_WORKSPACE = current_workspace()

DATA_DIR: Path = _WORKSPACE.data_dir
DATASETS_FILE: Path = _WORKSPACE.datasets_file

store = get_store()
contract_service = LocalContractServiceClient(store=store)
dq_service = LocalDataQualityServiceClient()
_DATA_PRODUCT_BACKEND = LocalDataProductServiceBackend()
for _doc in load_data_product_documents():
    _DATA_PRODUCT_BACKEND.put(_doc)
governance_service: LocalGovernanceServiceClient = build_local_governance_service(
    store,
    data_product_backend=_DATA_PRODUCT_BACKEND,
)
data_product_service = LocalDataProductServiceClient(backend=_DATA_PRODUCT_BACKEND)

if contracts_server is not None:
    try:
        contracts_server.configure_workspace(_WORKSPACE)
    except Exception:  # pragma: no cover - defensive guardrail for optional UI package
        pass

    def _safe_fs_name(name: str) -> str:
        return "".join(
            ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in name
        ) or "version"

    def _dataset_version_dir(dataset: str, version: str) -> Path | None:
        base = current_workspace().data_dir / dataset
        for candidate in (base / version, base / _safe_fs_name(version)):
            if candidate.exists():
                return candidate
        return None

    def _preview_contents(dataset: str, version: str) -> str:
        target = _dataset_version_dir(dataset, version)
        if target is None:
            return ""

        lines: list[str] = []
        for entry in sorted(target.iterdir()):
            if entry.is_dir():
                continue
            try:
                with entry.open("r", encoding="utf-8") as handle:
                    for _, line in zip(range(20 - len(lines)), handle):
                        lines.append(line.rstrip())
            except OSError:
                continue
            if len(lines) >= 20:
                break
        return "\n".join(lines)

    def _dataset_preview(contract, dataset_name: str, dataset_version: str) -> str:
        del contract  # filesystem previews are demo-only
        return _preview_contents(dataset_name, dataset_version)

    contracts_server._dataset_preview = _dataset_preview  # type: ignore[attr-defined]


def register_dataset_version(dataset: str, version: str, source: Path) -> None:
    _register_dataset_version(current_workspace(), dataset, version, source)


def set_active_version(dataset: str, version: str) -> None:
    _set_active_version(current_workspace(), dataset, version)


def refresh_dataset_aliases(dataset: str | None = None) -> None:
    _refresh_dataset_aliases(current_workspace(), dataset)


def reset_governance_state() -> None:
    """Rebuild the local governance client, clearing recorded history."""

    global governance_service
    governance_service = build_local_governance_service(
        store, data_product_backend=_DATA_PRODUCT_BACKEND
    )


def save_records(records: Iterable[DatasetRecord]) -> None:
    """Persist ``records`` to the shared dataset registry."""

    workspace = current_workspace()
    try:
        global DATA_DIR, DATASETS_FILE
        DATA_DIR = workspace.data_dir
        DATASETS_FILE = workspace.datasets_file
    except NameError:
        pass

    if contracts_server is not None:
        try:
            contracts_server.configure_workspace(workspace)
        except Exception:  # pragma: no cover - optional UI package guards
            pass

    items: list[dict[str, Any]] = []
    for record in records:
        payload: dict[str, Any]
        if isinstance(record, DatasetRecord):
            payload = record.__dict__.copy()
        elif isinstance(record, Mapping):
            payload = dict(record)
        else:
            payload = dict(getattr(record, "__dict__", {}))
        items.append(payload)

    DATASETS_FILE.parent.mkdir(parents=True, exist_ok=True)
    DATASETS_FILE.write_text(json.dumps(items, indent=2), encoding="utf-8")

    if contracts_server is not None and hasattr(contracts_server, "save_records"):
        try:
            contracts_server.save_records(list(records))  # type: ignore[arg-type]
        except Exception:  # pragma: no cover - optional UI compatibility
            pass


__all__ = [
    "DATA_DIR",
    "DATASETS_FILE",
    "DatasetRecord",
    "contract_service",
    "data_product_service",
    "dq_service",
    "governance_service",
    "load_data_product_documents",
    "load_data_product_payloads",
    "load_records",
    "save_records",
    "queue_flash",
    "pop_flash",
    "scenario_run_rows",
    "dq_version_records",
    "load_contract_meta",
    "register_dataset_version",
    "set_active_version",
    "refresh_dataset_aliases",
    "reset_governance_state",
    "store",
]

