"""Compatibility facade exposing contracts helpers for the demo pipeline."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Mapping

_REPO_ROOT = Path(__file__).resolve().parents[4]
for _relative in ("packages/dc43-service-backends/src", "packages/dc43-contracts-app/src"):
    _candidate = (_REPO_ROOT / _relative).resolve()
    if _candidate.exists() and str(_candidate) not in sys.path:
        sys.path.insert(0, str(_candidate))

from dc43_service_clients.contracts.client.local import LocalContractServiceClient
from dc43_service_clients.data_products.client.local import (
    LocalDataProductServiceClient,
)
from dc43_service_clients.data_quality.client.local import LocalDataQualityServiceClient
from dc43_service_clients.governance.client.local import (
    LocalGovernanceServiceClient,
)
from dc43_service_backends.bootstrap import build_backends
from dc43_service_backends.config import (
    ContractStoreConfig,
    DataProductStoreConfig,
    DataQualityBackendConfig,
    GovernanceStoreConfig,
    ServiceBackendsConfig,
)
from dc43_service_backends.contracts import LocalContractServiceBackend
from dc43_service_backends.data_quality import LocalDataQualityServiceBackend
from dc43_service_backends.governance.backend.local import LocalGovernanceServiceBackend
from dc43_service_backends.governance.storage.filesystem import FilesystemGovernanceStore
from uuid import uuid4
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
    seed_governance_records,
    refresh_dataset_aliases as _refresh_dataset_aliases,
    register_dataset_version as _register_dataset_version,
    set_active_version as _set_active_version,
    DemoGovernanceStore,
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
def _demo_normalise_record_status(value: str | None) -> str:
    if not value:
        return "ok"
    text = value.lower()
    if text in {"warn", "warning"}:
        return "warning"
    if text in {"block", "error", "fail", "invalid"}:
        return "error"
    return "ok"


def _demo_extract_violation_count(section: Mapping[str, object] | None) -> int:
    if not isinstance(section, Mapping):
        return 0
    total = 0
    candidate = section.get("violations")
    if isinstance(candidate, (int, float)):
        total = max(total, int(candidate))
    metrics = section.get("metrics")
    if isinstance(metrics, Mapping):
        for key, value in metrics.items():
            if str(key).startswith("violations") and isinstance(value, (int, float)):
                total = max(total, int(value))
    failed = section.get("failed_expectations")
    if isinstance(failed, Mapping):
        for info in failed.values():
            if not isinstance(info, Mapping):
                continue
            count = info.get("count")
            if isinstance(count, (int, float)):
                total = max(total, int(count))
    errors = section.get("errors")
    if isinstance(errors, list):
        total = max(total, len(errors))
    details = section.get("details")
    if isinstance(details, Mapping):
        total = max(total, _demo_extract_violation_count(details))
    dq_status = section.get("dq_status")
    if isinstance(dq_status, Mapping):
        total = max(total, _demo_extract_violation_count(dq_status))
    return total


seed_governance_records()


def _build_governance_service() -> LocalGovernanceServiceClient:
    workspace = current_workspace()
    governance_store = DemoGovernanceStore(workspace.records_dir / "governance")
    backend = LocalGovernanceServiceBackend(
        contract_client=LocalContractServiceBackend(store),
        dq_client=LocalDataQualityServiceBackend(),
        data_product_client=_DATA_PRODUCT_BACKEND,
        draft_store=store,
        store=governance_store,
    )
    return LocalGovernanceServiceClient(backend)


governance_service: LocalGovernanceServiceClient = _build_governance_service()
data_product_service = LocalDataProductServiceClient(backend=_DATA_PRODUCT_BACKEND)

if contracts_server is not None:
    from dc43_contracts_app import services as contracts_services
    import dc43_service_backends.governance.storage.filesystem as gov_fs

    gov_fs.FilesystemGovernanceStore = DemoGovernanceStore  # type: ignore[attr-defined]
    FilesystemGovernanceStore = DemoGovernanceStore  # type: ignore[assignment]

    if not hasattr(FilesystemGovernanceStore, "_activity_dir"):
        def _activity_dir(self: FilesystemGovernanceStore) -> Path:  # type: ignore[name-defined]
            path = self.base_path / "pipeline_activity"
            path.mkdir(parents=True, exist_ok=True)
            return path

        FilesystemGovernanceStore._activity_dir = _activity_dir  # type: ignore[attr-defined]

    if not hasattr(contracts_server, "_normalise_record_status"):
        contracts_server._normalise_record_status = _demo_normalise_record_status  # type: ignore[attr-defined]
    if not hasattr(contracts_server, "_extract_violation_count"):
        contracts_server._extract_violation_count = _demo_extract_violation_count  # type: ignore[attr-defined]

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
    def _initialise_contracts_backend() -> None:
        workspace = current_workspace()
        backend_config = ServiceBackendsConfig(
            contract_store=ContractStoreConfig(
                type="filesystem",
                root=workspace.contracts_dir,
            ),
            data_product_store=DataProductStoreConfig(type="memory"),
            data_quality=DataQualityBackendConfig(type="local"),
            governance_store=GovernanceStoreConfig(
                type="filesystem",
                root=workspace.records_dir / "governance",
            ),
        )
        suite = build_backends(backend_config)
        bundle = contracts_services.ServiceBundle(
            contract=LocalContractServiceClient(suite.contract),
            data_product=LocalDataProductServiceClient(suite.data_product),
            data_quality=LocalDataQualityServiceClient(suite.data_quality),
            governance=LocalGovernanceServiceClient(suite.governance),
        )
        contracts_services._SERVICE_BACKENDS_CONFIG = backend_config
        contracts_services._assign_service_clients(bundle)
        contracts_services._BACKEND_MODE = "embedded"
        contracts_services._BACKEND_BASE_URL = "http://dc43-services"
        contracts_services._BACKEND_TOKEN = uuid4().hex

    _initialise_contracts_backend()


def register_dataset_version(dataset: str, version: str, source: Path) -> None:
    _register_dataset_version(current_workspace(), dataset, version, source)


def set_active_version(dataset: str, version: str) -> None:
    _set_active_version(current_workspace(), dataset, version)


def refresh_dataset_aliases(dataset: str | None = None) -> None:
    _refresh_dataset_aliases(current_workspace(), dataset)


def reset_governance_state() -> None:
    """Rebuild the local governance client, clearing recorded history."""

    global governance_service
    seed_governance_records()
    governance_service = _build_governance_service()
    if contracts_server is not None:
        _initialise_contracts_backend()


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

