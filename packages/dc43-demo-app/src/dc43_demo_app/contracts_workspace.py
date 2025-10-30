"""Demo workspace helpers independent from the contracts app package."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Sequence, Tuple

from open_data_contract_standard.model import OpenDataContractStandard

try:  # pragma: no cover - demo works even if contracts app not installed
    from dc43_contracts_app.hints import register_workspace_hint_supplier
except ImportError:  # pragma: no cover - optional dependency for demos
    register_workspace_hint_supplier = None  # type: ignore[assignment]

from dc43_service_backends.governance.storage.filesystem import FilesystemGovernanceStore
from dc43_service_clients.data_quality import ValidationResult

from .scenarios import _DEFAULT_SLICE, _INVALID_SLICE

BASE_DIR = Path(__file__).resolve().parent
SAMPLE_ROOT = BASE_DIR / "demo_data"


@dataclass(slots=True)
class ContractsAppWorkspace:
    """Filesystem layout backing the demo contracts experience."""

    root: Path
    contracts_dir: Path
    data_dir: Path
    records_dir: Path
    datasets_file: Path
    dq_status_dir: Path
    data_products_file: Path

    def ensure(self) -> None:
        """Create any directories and default files required by the UI."""

        self.contracts_dir.mkdir(parents=True, exist_ok=True)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.records_dir.mkdir(parents=True, exist_ok=True)
        self.dq_status_dir.mkdir(parents=True, exist_ok=True)
        if not self.datasets_file.exists():
            self.datasets_file.write_text("[]", encoding="utf-8")
        if not self.data_products_file.exists():
            self.data_products_file.parent.mkdir(parents=True, exist_ok=True)
            self.data_products_file.write_text("[]", encoding="utf-8")


class DemoGovernanceStore(FilesystemGovernanceStore):
    """Filesystem governance store with helpers expected by the demo."""

    def _activity_dir(self) -> Path:
        path = self.base_path / "pipeline_activity"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def list_datasets(self) -> Sequence[str]:
        datasets: list[str] = []
        activity_dir = self._activity_dir()
        if not activity_dir.exists():
            return datasets
        for entry in activity_dir.iterdir():
            if not entry.is_file() or entry.suffix != ".json":
                continue
            try:
                payload = json.loads(entry.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if not isinstance(payload, Mapping):
                continue
            dataset_id = payload.get("dataset_id")
            if isinstance(dataset_id, str) and dataset_id:
                datasets.append(dataset_id)
        datasets.sort()
        return datasets

    def load_pipeline_activity(
        self,
        *,
        dataset_id: str,
        dataset_version: str | None = None,
    ) -> Sequence[Mapping[str, object]]:
        path = self._activity_path(dataset_id)
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return []
        if not isinstance(payload, Mapping):
            return []
        versions = payload.get("versions")
        if not isinstance(versions, Mapping):
            return []
        if dataset_version is not None:
            record = versions.get(str(dataset_version))
            if isinstance(record, Mapping):
                return [dict(record)]
            return []
        entries: list[Mapping[str, object]] = []
        for record in versions.values():
            if isinstance(record, Mapping):
                entries.append(dict(record))
        entries.sort(
            key=lambda item: (
                0,
                str(
                    (item.get("events") or [{}])[-1].get("recorded_at", "")
                    if isinstance(item.get("events"), list) and item.get("events")
                    else "",
                ),
            ),
        )
        return entries

    def load_metrics(
        self,
        *,
        dataset_id: str,
        dataset_version: str | None = None,
        contract_id: str | None = None,
        contract_version: str | None = None,
    ) -> Sequence[Mapping[str, object]]:
        path = self._metrics_path(dataset_id)
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return []
        if not isinstance(payload, Mapping):
            return []
        versions = payload.get("versions")
        if not isinstance(versions, Mapping):
            return []

        def _matches(entry: Mapping[str, object]) -> bool:
            if dataset_version is not None and entry.get("dataset_version") != dataset_version:
                return False
            if contract_id is not None and entry.get("contract_id") != contract_id:
                return False
            if contract_version is not None and entry.get("contract_version") != contract_version:
                return False
            return True

        records: list[Mapping[str, object]] = []
        for record_list in versions.values():
            if isinstance(record_list, list):
                for entry in record_list:
                    if isinstance(entry, Mapping) and _matches(entry):
                        records.append(dict(entry))

        records.sort(
            key=lambda item: (
                str(item.get("status_recorded_at", "")),
                str(item.get("metric_key", "")),
            ),
        )
        return records


def _normalise_details(payload: object) -> dict[str, object]:
    if isinstance(payload, Mapping):
        return {str(key): value for key, value in payload.items()}
    return {}


def _normalise_metrics(payload: Mapping[str, object] | None) -> dict[str, object]:
    if not isinstance(payload, Mapping):
        return {}
    return {str(key): value for key, value in payload.items()}


def _record_sample_governance(store: FilesystemGovernanceStore, entries: Sequence[Mapping[str, object]]) -> None:
    for entry in entries:
        dataset_name = str(entry.get("dataset_name") or "").strip()
        dataset_version = str(entry.get("dataset_version") or "").strip()
        if not dataset_name or not dataset_version:
            continue

        contract_id = str(entry.get("contract_id") or "").strip()
        contract_version = str(entry.get("contract_version") or "").strip()
        status_value = str(entry.get("status") or "unknown")
        reason_value = str(entry.get("reason") or "").strip() or None
        details = _normalise_details(entry.get("dq_details"))
        metrics = _normalise_metrics(details.get("metrics"))
        schema_payload = details.get("schema") if isinstance(details.get("schema"), Mapping) else None

        validation = ValidationResult(
            status=status_value,
            reason=reason_value,
            metrics=metrics,
            schema={str(k): dict(v) for k, v in schema_payload.items()} if isinstance(schema_payload, Mapping) else None,
            details=details,
        )

        if contract_id and contract_version:
            store.save_status(
                contract_id=contract_id,
                contract_version=contract_version,
                dataset_id=dataset_name,
                dataset_version=dataset_version,
                status=validation,
            )
            store.link_dataset_contract(
                dataset_id=dataset_name,
                dataset_version=dataset_version,
                contract_id=contract_id,
                contract_version=contract_version,
            )

        context: dict[str, object] = {}
        run_type = entry.get("run_type")
        if isinstance(run_type, str) and run_type:
            context["run_type"] = run_type
        scenario_key = entry.get("scenario_key")
        if isinstance(scenario_key, str) and scenario_key:
            context["scenario_key"] = scenario_key

        event: dict[str, object] = {"dq_status": status_value}
        if reason_value:
            event["dq_reason"] = reason_value
        if details:
            event["dq_details"] = details
        event["pipeline_context"] = context

        draft_version = entry.get("draft_contract_version")
        if isinstance(draft_version, str) and draft_version:
            event["draft_contract_version"] = draft_version

        data_product_id = str(entry.get("data_product_id") or "").strip()
        data_product_port = str(entry.get("data_product_port") or "").strip()
        data_product_role = str(entry.get("data_product_role") or "").strip()
        data_product: dict[str, str] = {}
        if data_product_id:
            data_product["id"] = data_product_id
        if data_product_port:
            data_product["port"] = data_product_port
        if data_product_role:
            data_product["role"] = data_product_role
        if data_product:
            event["data_product"] = data_product

        store.record_pipeline_event(
            contract_id=contract_id,
            contract_version=contract_version,
            dataset_id=dataset_name,
            dataset_version=dataset_version,
            event=event,
        )


def _seed_governance_records(workspace: ContractsAppWorkspace) -> None:
    datasets_path = workspace.datasets_file
    try:
        payload = json.loads(datasets_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        payload = []

    entries: list[Mapping[str, object]] = []
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, Mapping):
                entries.append(item)

    governance_root = workspace.records_dir / "governance"
    shutil.rmtree(governance_root, ignore_errors=True)
    if not entries:
        governance_root.mkdir(parents=True, exist_ok=True)
        return

    store = DemoGovernanceStore(governance_root)
    _record_sample_governance(store, entries)


_CURRENT_WORKSPACE: ContractsAppWorkspace | None = None


def _copy_tree(source: Path, destination: Path) -> None:
    if not source.exists():
        return
    if destination.exists():
        shutil.copytree(source, destination, dirs_exist_ok=True)
    else:
        shutil.copytree(source, destination)


def _link_path(target: Path, source: Path) -> None:
    """Best-effort creation of ``target`` pointing at ``source``."""

    if target.exists() or target.is_symlink():
        try:
            target.unlink()
        except OSError:
            if target.is_dir():
                shutil.rmtree(target, ignore_errors=True)
            else:
                raise
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        relative = os.path.relpath(source, target.parent)
        target.symlink_to(relative, target_is_directory=source.is_dir())
    except OSError:
        if source.is_dir():
            shutil.copytree(source, target, dirs_exist_ok=True)
        else:
            shutil.copy2(source, target)


def _iter_versions(dataset_dir: Path) -> list[Path]:
    versions: list[Path] = []
    for candidate in dataset_dir.iterdir():
        if not candidate.is_dir():
            continue
        name = candidate.name
        if name == "latest" or name.startswith("latest__"):
            continue
        versions.append(candidate)
    return sorted(versions)


def _existing_version_dir(dataset_dir: Path, version: str) -> Path | None:
    candidate = dataset_dir / version
    if candidate.exists():
        return candidate
    safe_name = _safe_fs_name(version)
    candidate = dataset_dir / safe_name
    if candidate.exists():
        return candidate
    return None


def _target_version_dir(dataset_dir: Path, version: str) -> Path:
    existing = _existing_version_dir(dataset_dir, version)
    if existing is not None:
        return existing
    safe_name = _safe_fs_name(version)
    return dataset_dir / safe_name


def _safe_fs_name(name: str) -> str:
    safe = "".join(
        ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in name
    )
    return safe or "version"


def _dataset_roots(workspace: ContractsAppWorkspace, dataset: str | None) -> Iterable[Path]:
    if dataset:
        base = workspace.data_dir / dataset
        if base.exists():
            yield base
        return
    for candidate in workspace.data_dir.iterdir():
        if candidate.is_dir() and "__" not in candidate.name:
            yield candidate


def refresh_dataset_aliases(
    workspace: ContractsAppWorkspace, dataset: str | None = None
) -> None:
    """Populate ``latest`` aliases for datasets within ``workspace``."""

    for dataset_dir in _dataset_roots(workspace, dataset):
        versions = _iter_versions(dataset_dir)
        if not versions:
            continue
        latest = versions[-1]
        _link_path(dataset_dir / "latest", latest)

        derived_dirs = sorted(workspace.data_dir.glob(f"{dataset_dir.name}__*"))
        for derived_dir in derived_dirs:
            if not derived_dir.is_dir():
                continue
            suffix = derived_dir.name.split("__", 1)[1]
            derived_versions = _iter_versions(derived_dir)
            for version_dir in derived_versions:
                target = dataset_dir / version_dir.name / suffix
                _link_path(target, version_dir)
            if derived_versions:
                _link_path(dataset_dir / f"latest__{suffix}", derived_versions[-1])


def set_active_version(
    workspace: ContractsAppWorkspace, dataset: str, version: str
) -> None:
    """Point the ``latest`` alias of ``dataset`` to ``version``."""

    dataset_dir = workspace.data_dir / dataset
    target = _existing_version_dir(dataset_dir, version)
    if target is None:
        target = _target_version_dir(dataset_dir, version)
    if not target.exists():
        raise FileNotFoundError(f"Unknown dataset version: {dataset} {version}")

    _link_path(dataset_dir / "latest", target)

    if "__" not in dataset:
        for derived_dir in workspace.data_dir.glob(f"{dataset}__*"):
            suffix = derived_dir.name.split("__", 1)[1]
            derived_target = _existing_version_dir(derived_dir, version)
            if derived_target is None:
                continue
            _link_path(target / suffix, derived_target)
            _link_path(dataset_dir / f"latest__{suffix}", derived_target)
    else:
        base, suffix = dataset.split("__", 1)
        base_dir = workspace.data_dir / base
        if base_dir.exists():
            base_target = _existing_version_dir(base_dir, version)
            if base_target is not None:
                _link_path(base_target / suffix, target)
                _link_path(base_dir / f"latest__{suffix}", target)


def workspace_from_env(default_root: str | None = None) -> Tuple[ContractsAppWorkspace, bool]:
    """Return (workspace, created) derived from environment variables."""

    env_root = os.getenv("DC43_DEMO_WORK_DIR") or default_root
    created = False
    if env_root:
        root = Path(env_root).expanduser()
        if not root.exists():
            created = True
            root.mkdir(parents=True, exist_ok=True)
    else:
        root = Path(tempfile.mkdtemp(prefix="dc43_demo_"))
        created = True

    workspace = ContractsAppWorkspace(
        root=root,
        contracts_dir=root / "contracts",
        data_dir=root / "data",
        records_dir=root / "records",
        datasets_file=root / "records" / "datasets.json",
        dq_status_dir=root / "records" / "dq_state" / "status",
        data_products_file=root / "records" / "data_products.json",
    )
    workspace.ensure()
    return workspace, created


def _prepare_contracts(workspace: ContractsAppWorkspace) -> None:
    contracts_src = SAMPLE_ROOT / "contracts"
    if not contracts_src.exists():
        return

    for src in contracts_src.rglob("*.json"):
        model = OpenDataContractStandard.model_validate_json(src.read_text())
        for server in model.servers or []:
            path = Path(server.path or "")
            if not path.is_absolute():
                path = (workspace.root / path).resolve()
            base = path.parent if path.suffix else path
            base.mkdir(parents=True, exist_ok=True)
            server.path = str(path)
        dest = workspace.contracts_dir / src.relative_to(contracts_src)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(
            model.model_dump_json(indent=2, by_alias=True, exclude_none=True),
            encoding="utf-8",
        )


def prepare_demo_workspace() -> Tuple[ContractsAppWorkspace, bool]:
    """Ensure demo data exists on disk and record the active workspace."""

    workspace, created = workspace_from_env()
    os.environ.setdefault("DC43_DEMO_WORK_DIR", str(workspace.root))

    data_src = SAMPLE_ROOT / "data"
    records_src = SAMPLE_ROOT / "records"

    _copy_tree(data_src, workspace.data_dir)
    _copy_tree(records_src, workspace.records_dir)
    _prepare_contracts(workspace)

    _seed_governance_records(workspace)

    refresh_dataset_aliases(workspace)
    for dataset, version in {**_DEFAULT_SLICE, **_INVALID_SLICE}.items():
        try:
            set_active_version(workspace, dataset, version)
        except FileNotFoundError:
            continue

    os.environ.setdefault("DC43_CONTRACT_STORE", str(workspace.contracts_dir))
    os.environ.setdefault("DC43_GOVERNANCE_STORE_TYPE", "filesystem")
    os.environ.setdefault("DC43_GOVERNANCE_STORE", str(workspace.records_dir / "governance"))

    global _CURRENT_WORKSPACE
    _CURRENT_WORKSPACE = workspace

    if register_workspace_hint_supplier is not None:
        def _demo_workspace_hints(active: ContractsAppWorkspace = workspace) -> dict[str, str]:
            return {
                "root": str(active.root),
                "contracts_dir": str(active.contracts_dir),
                "products_dir": str(active.root / "products"),
                "expectations_dir": str(active.records_dir / "expectations"),
                "governance_dir": str(active.root / "governance"),
            }

        register_workspace_hint_supplier(_demo_workspace_hints)

    return workspace, created


def current_workspace() -> ContractsAppWorkspace:
    """Return the currently prepared workspace, creating it if required."""

    global _CURRENT_WORKSPACE
    if _CURRENT_WORKSPACE is None:
        workspace, _ = prepare_demo_workspace()
        _CURRENT_WORKSPACE = workspace
    return _CURRENT_WORKSPACE


def seed_governance_records() -> None:
    """Repopulate governance history files from the bundled sample payload."""

    _seed_governance_records(current_workspace())


def _ensure_version_marker(target: Path, version: str) -> None:
    marker = target / ".dc43_version"
    if not marker.exists():
        marker.write_text(version, encoding="utf-8")


def register_dataset_version(
    workspace: ContractsAppWorkspace, dataset: str, version: str, source: Path
) -> None:
    """Expose ``source`` under the workspace dataset hierarchy."""

    dataset_dir = workspace.data_dir / dataset
    dataset_dir.mkdir(parents=True, exist_ok=True)
    target = _target_version_dir(dataset_dir, version)
    try:
        if source.resolve() == target.resolve():
            _ensure_version_marker(target, version)
            return
    except OSError:
        pass
    _link_path(target, source)
    _ensure_version_marker(target, version)


__all__ = [
    "ContractsAppWorkspace",
    "current_workspace",
    "prepare_demo_workspace",
    "refresh_dataset_aliases",
    "register_dataset_version",
    "set_active_version",
    "seed_governance_records",
]

