"""Local replicas of the contracts application helpers used by the demo."""

from __future__ import annotations

import json
import os
from pathlib import Path
from dataclasses import dataclass, field
from threading import Lock
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from uuid import uuid4

from open_data_contract_standard.model import OpenDataContractStandard

from dc43_service_clients.odps import OpenDataProductStandard

from dc43_service_backends.contracts.backend.stores import FSContractStore

from .contracts_workspace import ContractsAppWorkspace, current_workspace


@dataclass
class DatasetRecord:
    contract_id: str
    contract_version: str
    dataset_name: str = ""
    dataset_version: str = ""
    status: str = "unknown"
    dq_details: Dict[str, Any] = field(default_factory=dict)
    run_type: str = "infer"
    violations: int = 0
    reason: str = ""
    draft_contract_version: str | None = None
    scenario_key: str | None = None
    data_product_id: str = ""
    data_product_port: str = ""
    data_product_role: str = ""


_FLASH_LOCK = Lock()
_FLASH_MESSAGES: Dict[str, Dict[str, str | None]] = {}


def _datasets_file(workspace: ContractsAppWorkspace | None = None) -> os.PathLike[str]:
    ws = workspace or current_workspace()
    return ws.datasets_file


def _data_products_file(workspace: ContractsAppWorkspace | None = None) -> Path:
    ws = workspace or current_workspace()
    return ws.data_products_file


def load_records(workspace: ContractsAppWorkspace | None = None) -> List[DatasetRecord]:
    datasets_path = _datasets_file(workspace)
    if not Path(datasets_path).exists():
        return []
    try:
        raw = json.loads(Path(datasets_path).read_text())
    except (OSError, json.JSONDecodeError):
        return []
    return [DatasetRecord(**r) for r in raw]


def save_records(records: List[DatasetRecord], workspace: ContractsAppWorkspace | None = None) -> None:
    datasets_path = Path(_datasets_file(workspace))
    datasets_path.parent.mkdir(parents=True, exist_ok=True)
    datasets_path.write_text(
        json.dumps([r.__dict__ for r in records], indent=2), encoding="utf-8"
    )


def load_data_product_payloads(
    workspace: ContractsAppWorkspace | None = None,
) -> List[Mapping[str, Any]]:
    path = _data_products_file(workspace)
    try:
        raw = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return []
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, Mapping)]
    return []


def load_data_product_documents(
    workspace: ContractsAppWorkspace | None = None,
) -> List[OpenDataProductStandard]:
    documents: List[OpenDataProductStandard] = []
    for payload in load_data_product_payloads(workspace):
        try:
            documents.append(OpenDataProductStandard.from_dict(payload))
        except Exception:
            continue
    return documents


def queue_flash(message: str | None = None, error: str | None = None) -> str:
    token = uuid4().hex
    with _FLASH_LOCK:
        _FLASH_MESSAGES[token] = {"message": message, "error": error}
    return token


def pop_flash(token: str) -> Tuple[str | None, str | None]:
    with _FLASH_LOCK:
        payload = _FLASH_MESSAGES.pop(token, None) or {}
    return payload.get("message"), payload.get("error")


def _version_sort_key(value: str) -> tuple[int, str]:
    cleaned = value or ""
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1]
    return (len(cleaned), cleaned)


def _scenario_dataset_name(params: Mapping[str, Any]) -> str:
    dataset_name = params.get("dataset_name")
    if dataset_name:
        return str(dataset_name)
    contract_id = params.get("contract_id")
    if contract_id:
        return str(contract_id)
    dataset_id = params.get("dataset_id")
    if dataset_id:
        return str(dataset_id)
    return "result"


def scenario_run_rows(
    records: Iterable[DatasetRecord],
    scenarios: Mapping[str, Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    by_dataset: Dict[str, List[DatasetRecord]] = {}
    by_scenario: Dict[str, List[DatasetRecord]] = {}
    for record in records:
        if record.dataset_name:
            by_dataset.setdefault(record.dataset_name, []).append(record)
        if record.scenario_key:
            by_scenario.setdefault(record.scenario_key, []).append(record)

    for entries in by_dataset.values():
        entries.sort(key=lambda item: _version_sort_key(item.dataset_version or ""))
    for entries in by_scenario.values():
        entries.sort(key=lambda item: _version_sort_key(item.dataset_version or ""))

    rows: List[Dict[str, Any]] = []
    for key, cfg in scenarios.items():
        params: Mapping[str, Any] = cfg.get("params", {})
        dataset_name = _scenario_dataset_name(params)
        dataset_records: List[DatasetRecord] = list(by_scenario.get(key, []))

        if not dataset_records:
            candidate_records = by_dataset.get(dataset_name, [])
            if candidate_records:
                contract_id = params.get("contract_id")
                contract_version = params.get("contract_version")
                run_type = params.get("run_type")
                filtered: List[DatasetRecord] = []
                for record in candidate_records:
                    if record.scenario_key:
                        continue
                    if contract_id and record.contract_id and record.contract_id != contract_id:
                        continue
                    if (
                        contract_version
                        and record.contract_version
                        and record.contract_version != contract_version
                    ):
                        continue
                    if run_type and record.run_type and record.run_type != run_type:
                        continue
                    filtered.append(record)
                if filtered:
                    dataset_records = filtered
                else:
                    dataset_records = [rec for rec in candidate_records if not rec.scenario_key]

        dataset_records = list(dataset_records)
        dataset_records.sort(key=lambda item: _version_sort_key(item.dataset_version or ""))
        primary_run_type = params.get("run_type", "infer")
        primary_records = [
            record
            for record in dataset_records
            if (record.run_type or primary_run_type) == primary_run_type
        ]
        if primary_run_type == "infer" and not primary_records:
            primary_records = dataset_records
        latest_candidates = primary_records or dataset_records
        latest_record = latest_candidates[-1] if latest_candidates else None
        run_count = len(primary_records) if primary_records else len(dataset_records)

        rows.append(
            {
                "key": key,
                "label": cfg.get("label", key.replace("-", " ").title()),
                "description": cfg.get("description"),
                "diagram": cfg.get("diagram"),
                "category": cfg.get("category", "contract"),
                "dataset_name": dataset_name,
                "contract_id": params.get("contract_id"),
                "contract_version": params.get("contract_version"),
                "run_type": params.get("run_type", "infer"),
                "run_count": run_count,
                "latest": latest_record.__dict__.copy() if latest_record else None,
            }
        )

    return rows


def scenario_history(
    records: Iterable[DatasetRecord],
    scenario_key: str,
    scenario_cfg: Mapping[str, Any],
) -> Tuple[List[DatasetRecord], str]:
    """Return the ordered history of runs associated with ``scenario_key``."""

    params: Mapping[str, Any] = scenario_cfg.get("params", {})
    dataset_name = _scenario_dataset_name(params)
    dataset_records: List[DatasetRecord] = [
        record for record in records if record.scenario_key == scenario_key
    ]

    if dataset_records:
        filtered_records = [
            record
            for record in dataset_records
            if not (record.run_type or "").endswith("-batch")
        ]
        if dataset_name:
            targeted = [
                record
                for record in filtered_records
                if record.dataset_name == dataset_name
            ]
            if targeted:
                filtered_records = targeted
        if filtered_records:
            dataset_records = filtered_records

    if not dataset_records:
        candidate_records = [
            record for record in records if record.dataset_name == dataset_name
        ]
        if candidate_records:
            contract_id = params.get("contract_id")
            contract_version = params.get("contract_version")
            run_type = params.get("run_type")
            filtered: List[DatasetRecord] = []
            for record in candidate_records:
                if record.scenario_key and record.scenario_key != scenario_key:
                    continue
                if contract_id and record.contract_id and record.contract_id != contract_id:
                    continue
                if (
                    contract_version
                    and record.contract_version
                    and record.contract_version != contract_version
                ):
                    continue
                if run_type and record.run_type and record.run_type != run_type:
                    continue
                filtered.append(record)
            if filtered:
                dataset_records = filtered
            else:
                dataset_records = [
                    record for record in candidate_records if not record.scenario_key
                ]

    dataset_records = list(dataset_records)
    dataset_records.sort(
        key=lambda item: _version_sort_key(item.dataset_version or ""),
        reverse=True,
    )
    return dataset_records, dataset_name


_STORE: FSContractStore | None = None


def _ensure_store() -> FSContractStore:
    global _STORE
    if _STORE is None:
        workspace = current_workspace()
        _STORE = FSContractStore(str(workspace.contracts_dir))
    return _STORE


def get_store() -> FSContractStore:
    """Expose the filesystem contract store used by the demo."""

    return _ensure_store()


def load_contract_meta() -> List[Dict[str, Any]]:
    store = _ensure_store()
    meta: List[Dict[str, Any]] = []
    for cid in store.list_contracts():
        for ver in store.list_versions(cid):
            try:
                contract = store.get(cid, ver)
            except FileNotFoundError:
                continue
            server = (contract.servers or [None])[0]
            path = ""
            if server:
                parts: List[str] = []
                if getattr(server, "address", None):
                    parts.append(str(server.address))
                if getattr(server, "path", None):
                    parts.append(str(server.path))
                path = "/".join(parts)
            meta.append(
                {
                    "id": contract.id,
                    "version": contract.version,
                    "name": contract.info.title if contract.info else contract.id,
                    "description": contract.info.description if contract.info else "",
                    "path": path,
                }
            )
    return meta


def _dataset_root_for(
    dataset_id: str,
    *,
    dataset_path: str | None = None,
    workspace: ContractsAppWorkspace | None = None,
) -> Path | None:
    ws = workspace or current_workspace()
    if dataset_path:
        root = Path(dataset_path)
        if not root.is_absolute():
            root = ws.data_dir / dataset_path
        return root
    return ws.data_dir / dataset_id


def _has_version_materialisation(root: Path | None, version: str) -> bool:
    if root is None:
        return False
    target = root / version
    if target.exists():
        return True
    safe = target.parent / _safe_fs_name(version)
    return safe.exists()


def _safe_fs_name(name: str) -> str:
    safe = "".join(
        ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in name
    )
    return safe or "version"


def _dq_status_entries(dataset_id: str, workspace: ContractsAppWorkspace | None = None):
    ws = workspace or current_workspace()
    status_dir = ws.dq_status_dir / dataset_id
    if not status_dir.exists():
        return []
    entries: List[Tuple[str, str, Mapping[str, Any]]] = []
    for payload_file in status_dir.glob("*.json"):
        try:
            payload = json.loads(payload_file.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        display_version = payload.get("version") or payload_file.stem
        entries.append((str(display_version), payload_file.stem, payload))
    entries.sort(key=lambda item: _version_sort_key(item[0]))
    return entries


_DQ_STATUS_BADGES: Dict[str, str] = {
    "ok": "bg-success",
    "warn": "bg-warning text-dark",
    "block": "bg-danger",
    "stale": "bg-secondary",
    "unknown": "bg-secondary",
}


def dq_version_records(
    dataset_id: str,
    *,
    contract: Optional[OpenDataContractStandard] = None,
    dataset_path: Optional[str] = None,
    dataset_records: Optional[Iterable[DatasetRecord]] = None,
) -> List[Dict[str, Any]]:
    entries = _dq_status_entries(dataset_id)
    records: List[Dict[str, Any]] = []

    scoped_versions: set[str] = set()
    dataset_record_map: Dict[str, DatasetRecord] = {}
    if dataset_records:
        for record in dataset_records:
            if not record.dataset_version:
                continue
            scoped_versions.add(record.dataset_version)
            dataset_record_map[record.dataset_version] = record

    dataset_dir = _dataset_root_for(dataset_id, dataset_path=dataset_path)
    skip_fs_check = False
    if contract and contract.servers:
        server = contract.servers[0]
        fmt = (getattr(server, "format", "") or "").lower()
        if fmt == "delta":
            skip_fs_check = True

    seen_versions: set[str] = set()
    for display_version, stored_version, payload in entries:
        record = dataset_record_map.get(display_version)
        payload_contract_id = str(payload.get("contract_id") or "")
        payload_contract_version = str(payload.get("contract_version") or "")
        if contract and (contract.id or contract.version):
            contract_id_value = contract.id or ""
            if payload_contract_id and payload_contract_version:
                if (
                    payload_contract_id != contract_id_value
                    or payload_contract_version != contract.version
                ):
                    continue
            elif scoped_versions and display_version not in scoped_versions:
                continue
        elif scoped_versions and display_version not in scoped_versions:
            continue
        if not skip_fs_check and not _has_version_materialisation(dataset_dir, display_version):
            continue
        status_value = str(payload.get("status", "unknown") or "unknown")
        records.append(
            {
                "version": display_version,
                "stored_version": stored_version,
                "status": status_value,
                "status_label": status_value.replace("_", " ").title(),
                "badge": _DQ_STATUS_BADGES.get(status_value, "bg-secondary"),
                "contract_id": payload_contract_id or (record.contract_id if record else ""),
                "contract_version": payload_contract_version
                or (record.contract_version if record else ""),
                "recorded_at": payload.get("recorded_at"),
            }
        )
        seen_versions.add(display_version)

    if scoped_versions:
        for missing_version in scoped_versions - seen_versions:
            record = dataset_record_map.get(missing_version)
            status_value = str(record.status or "unknown") if record else "unknown"
            records.append(
                {
                    "version": missing_version,
                    "stored_version": _safe_fs_name(missing_version),
                    "status": status_value,
                    "status_label": status_value.replace("_", " ").title(),
                    "badge": _DQ_STATUS_BADGES.get(status_value, "bg-secondary"),
                    "contract_id": record.contract_id if record else "",
                    "contract_version": record.contract_version if record else "",
                    "recorded_at": None,
                }
            )

    records.sort(key=lambda item: _version_sort_key(item["version"]))
    return records


__all__ = [
    "DatasetRecord",
    "load_data_product_documents",
    "load_data_product_payloads",
    "dq_version_records",
    "get_store",
    "load_contract_meta",
    "load_records",
    "pop_flash",
    "queue_flash",
    "save_records",
    "scenario_history",
    "scenario_run_rows",
]

