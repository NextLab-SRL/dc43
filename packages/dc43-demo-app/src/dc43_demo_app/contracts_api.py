"""Compatibility facade exposing contracts helpers for the demo pipeline."""

from __future__ import annotations

from __future__ import annotations

import json
import os
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Mapping
from uuid import uuid4

_REPO_ROOT = Path(__file__).resolve().parents[4]
for _relative in ("packages/dc43-service-backends/src", "packages/dc43-contracts-app/src"):
    _candidate = (_REPO_ROOT / _relative).resolve()
    if _candidate.exists() and str(_candidate) not in sys.path:
        sys.path.insert(0, str(_candidate))

from dc43_service_clients.contracts.client.local import LocalContractServiceClient
from dc43_service_clients.data_products.client.local import (
    LocalDataProductServiceClient,
)
from dc43_service_clients.data_quality import ValidationResult
from dc43_service_clients.data_quality.client.local import LocalDataQualityServiceClient
from dc43_service_clients.governance.client.local import LocalGovernanceServiceClient
from dc43_service_backends.contracts import LocalContractServiceBackend
from dc43_service_backends.data_products import LocalDataProductServiceBackend
from dc43_service_backends.data_quality import LocalDataQualityServiceBackend
from dc43_service_backends.governance.backend.local import LocalGovernanceServiceBackend
try:  # pragma: no cover - optional dependency when contracts app missing
    from dc43_contracts_app import server as contracts_server
except ImportError:  # pragma: no cover - demo can operate without the UI package
    contracts_server = None  # type: ignore[assignment]
if contracts_server is not None:  # pragma: no cover - optional dependency
    from dc43_contracts_app import services as contracts_services
    from fastapi import HTTPException
    from starlette.responses import JSONResponse

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
    _version_sort_key,
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


def _build_governance_service() -> LocalGovernanceServiceClient:
    workspace = current_workspace()
    governance_root = workspace.records_dir / "governance"
    backend = LocalGovernanceServiceBackend(
        contract_client=LocalContractServiceBackend(store),
        dq_client=LocalDataQualityServiceBackend(),
        data_product_client=_DATA_PRODUCT_BACKEND,
        draft_store=store,
        store=DemoGovernanceStore(governance_root),
    )
    return LocalGovernanceServiceClient(backend)


governance_service: LocalGovernanceServiceClient = _build_governance_service()
data_product_service = LocalDataProductServiceClient(backend=_DATA_PRODUCT_BACKEND)

if contracts_server is not None:
    bundle = contracts_services.ServiceBundle(
        contract=contract_service,
        data_product=data_product_service,
        data_quality=dq_service,
        governance=governance_service,
    )
    contracts_services._assign_service_clients(bundle)
    contracts_services._BACKEND_MODE = "embedded"
    contracts_services._BACKEND_BASE_URL = os.getenv(
        "DC43_DEMO_BACKEND_URL", "http://dc43-services"
    )
    contracts_services._BACKEND_TOKEN = uuid4().hex
    contracts_server.load_records = load_records  # type: ignore[assignment]

    def _demo_dataset_ids_service() -> list[str]:
        dataset_ids: list[str] = []
        for record in load_records():
            name = record.dataset_name
            if not name or name in dataset_ids:
                continue
            dataset_ids.append(name)
        return dataset_ids

    def _demo_pipeline_activity(
        dataset_id: str,
        dataset_version: str | None = None,
        *,
        include_status: bool = False,
    ) -> list[Mapping[str, object]]:
        entries: list[Mapping[str, object]] = []
        for record in load_records():
            if record.dataset_name != dataset_id:
                continue
            if dataset_version and record.dataset_version != dataset_version:
                continue
            event: dict[str, object] = {
                "recorded_at": record.dataset_version,
                "dq_status": record.status,
                "dq_details": dict(record.dq_details),
                "pipeline_context": {"run_type": record.run_type},
            }
            if record.reason:
                event["dq_reason"] = record.reason
            if record.scenario_key:
                event["pipeline_context"]["scenario_key"] = record.scenario_key
            if record.draft_contract_version:
                event["draft_contract_version"] = record.draft_contract_version
            product: dict[str, str] = {}
            if record.data_product_id:
                product["id"] = record.data_product_id
            if record.data_product_port:
                product["port"] = record.data_product_port
            if record.data_product_role:
                product["role"] = record.data_product_role
            if product:
                event["data_product"] = product
            entry: dict[str, object] = {
                "dataset_id": dataset_id,
                "dataset_version": record.dataset_version,
                "contract_id": record.contract_id,
                "contract_version": record.contract_version,
                "events": [event],
            }
            if include_status:
                entry["validation_status"] = {
                    "status": record.status,
                    "details": dict(record.dq_details),
                    "reason": record.reason,
                }
            entries.append(entry)
        return entries

    def _demo_validation_status(
        *,
        contract_id: str,
        contract_version: str,
        dataset_id: str,
        dataset_version: str,
    ) -> ValidationResult | None:
        for record in load_records():
            if (
                record.dataset_name == dataset_id
                and record.dataset_version == dataset_version
                and record.contract_id == contract_id
                and record.contract_version == contract_version
            ):
                return ValidationResult(
                    status=record.status or "unknown",
                    reason=record.reason or None,
                    details=dict(record.dq_details),
                )
        return None

    contracts_services.list_dataset_ids = _demo_dataset_ids_service  # type: ignore[assignment]
    contracts_services.dataset_pipeline_activity = _demo_pipeline_activity  # type: ignore[assignment]
    contracts_services.dataset_validation_status = _demo_validation_status  # type: ignore[assignment]

    try:
        existing_normaliser = contracts_server._normalise_record_status  # type: ignore[attr-defined]
    except AttributeError:
        existing_normaliser = None
    if existing_normaliser is None:
        contracts_server._normalise_record_status = _demo_normalise_record_status  # type: ignore[attr-defined]

    try:
        existing_counter = contracts_server._extract_violation_count  # type: ignore[attr-defined]
    except AttributeError:
        existing_counter = None
    if existing_counter is None:
        contracts_server._extract_violation_count = _demo_extract_violation_count  # type: ignore[attr-defined]

    def _serialise_contract(contract) -> Mapping[str, object]:
        try:
            return json.loads(
                contract.model_dump_json(indent=2, by_alias=True, exclude_none=True)
            )
        except AttributeError:  # pragma: no cover - defensive for unexpected contract types
            return json.loads(json.dumps(contract))

    def _dataset_record(dataset_name: str, dataset_version: str | None = None) -> DatasetRecord:
        records = [
            rec
            for rec in load_records()
            if rec.dataset_name == dataset_name and rec.dataset_version
        ]
        if dataset_version is not None:
            records = [rec for rec in records if rec.dataset_version == dataset_version]
        records.sort(key=lambda rec: _version_sort_key(rec.dataset_version))
        if not records:
            raise HTTPException(status_code=404, detail="Dataset not found")
        return records[-1]

    @contracts_server.app.get("/datasets/{dataset_name}.json")
    async def _dataset_latest_json(dataset_name: str) -> Mapping[str, object]:
        record = _dataset_record(dataset_name)
        payload = asdict(record)
        payload["dq_details"] = record.dq_details
        return payload

    @contracts_server.app.get("/datasets/{dataset_name}/{dataset_version}.json")
    async def _dataset_version_json(
        dataset_name: str, dataset_version: str
    ) -> Mapping[str, object]:
        record = _dataset_record(dataset_name, dataset_version)
        payload = asdict(record)
        payload["dq_details"] = record.dq_details
        return payload

    @contracts_server.app.get("/datasets.json")
    async def _datasets_index_json() -> Mapping[str, object]:
        dataset_names = sorted({rec.dataset_name for rec in load_records() if rec.dataset_name})
        return {"datasets": dataset_names}

    @contracts_server.app.get("/contracts/{cid}.json")
    async def _contract_versions_json(cid: str) -> Mapping[str, object]:
        try:
            versions = sorted(store.list_versions(cid))
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        if not versions:
            raise HTTPException(status_code=404, detail="Contract not found")
        return {"contract_id": cid, "versions": versions}

    @contracts_server.app.get("/contracts/{cid}/{ver}.json")
    async def _contract_detail_json(cid: str, ver: str) -> Mapping[str, object]:
        try:
            contract = contract_service.get(cid, ver)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        datasets = [
            asdict(rec)
            for rec in load_records()
            if rec.contract_id == cid and rec.contract_version == ver
        ]
        return {
            "contract_id": cid,
            "contract_version": ver,
            "contract": _serialise_contract(contract),
            "datasets": datasets,
        }

    @contracts_server.app.middleware("http")
    async def _json_suffix_router(request, call_next):  # pragma: no cover - demo helper
        path = request.url.path
        try:
            if path == "/datasets.json":
                payload = await _datasets_index_json()
                return JSONResponse(payload)
            if path.startswith("/datasets/") and path.endswith(".json"):
                parts = path.strip("/").split("/")
                if len(parts) == 2:
                    dataset = parts[1][:-5]
                    payload = await _dataset_latest_json(dataset)
                    return JSONResponse(payload)
                if len(parts) == 3:
                    dataset = parts[1]
                    version = parts[2][:-5]
                    payload = await _dataset_version_json(dataset, version)
                    return JSONResponse(payload)
            if path.startswith("/contracts/") and path.endswith(".json"):
                parts = path.strip("/").split("/")
                if len(parts) == 2:
                    cid = parts[1][:-5]
                    payload = await _contract_versions_json(cid)
                    return JSONResponse(payload)
                if len(parts) == 3:
                    cid = parts[1]
                    ver = parts[2][:-5]
                    payload = await _contract_detail_json(cid, ver)
                    return JSONResponse(payload)
        except HTTPException as exc:
            return JSONResponse({"detail": exc.detail}, status_code=exc.status_code)
        return await call_next(request)

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
    seed_governance_records(current_workspace())
    governance_service = _build_governance_service()
    if contracts_server is not None:
        bundle = contracts_services.ServiceBundle(
            contract=contract_service,
            data_product=data_product_service,
            data_quality=dq_service,
            governance=governance_service,
        )
        contracts_services._assign_service_clients(bundle)
        contracts_services._BACKEND_TOKEN = uuid4().hex


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

