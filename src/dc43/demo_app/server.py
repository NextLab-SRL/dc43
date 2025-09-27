from __future__ import annotations

"""FastAPI demo application for dc43.

This application provides a small Bootstrap-powered UI to manage data
contracts and run an example Spark pipeline that records dataset versions
with their validation status. Contracts are stored on the local
filesystem using :class:`~dc43.components.contract_store.impl.filesystem.FSContractStore` and dataset
metadata lives in a JSON file.

Run the application with::

    uvicorn dc43.demo_app.server:app --reload

Optional dependencies needed: ``fastapi``, ``uvicorn``, ``jinja2`` and
``pyspark``.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Any, Tuple, Mapping, Optional, Iterable
from uuid import uuid4
from threading import Lock
from textwrap import dedent
import json
import os
import re
import shutil
import tempfile
from datetime import datetime

from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.encoders import jsonable_encoder
from urllib.parse import urlencode

from dc43.components.contract_store.impl.filesystem import FSContractStore
from dc43.components.data_quality.integration import expectations_from_contract
from dc43.versioning import SemVer
from open_data_contract_standard.model import (
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
    Description,
    Server,
    DataQuality,
)
from pydantic import ValidationError
from packaging.version import Version

# Optional pyspark-based helpers. Keep imports lazy-friendly so the demo UI can
# still load when pyspark is not installed (for example when running fast unit
# tests).
try:  # pragma: no cover - exercised indirectly when pyspark is available
    from dc43.integration import (  # type: ignore[attr-defined]
        ContractVersionLocator,
        read_with_contract,
    )
except ModuleNotFoundError as exc:  # pragma: no cover - safety net for CI
    if exc.name != "pyspark":
        raise
    ContractVersionLocator = None  # type: ignore[assignment]
    read_with_contract = None  # type: ignore[assignment]

_SPARK_SESSION: Any | None = None


def _spark_session() -> Any:
    """Return a cached local Spark session for previews."""

    global _SPARK_SESSION
    if _SPARK_SESSION is None:
        from pyspark.sql import SparkSession  # type: ignore

        _SPARK_SESSION = (
            SparkSession.builder.master("local[1]")
            .appName("dc43-preview")
            .getOrCreate()
        )
    return _SPARK_SESSION

BASE_DIR = Path(__file__).resolve().parent
SAMPLE_DIR = BASE_DIR / "demo_data"
WORK_DIR = Path(tempfile.mkdtemp(prefix="dc43_demo_"))
if not os.getenv("SHOW_WORK_DIR") == "false":
    print(f"The working dir for the demo is: {WORK_DIR}")
    import subprocess, sys
    if sys.platform == "darwin":
        subprocess.run(["open", WORK_DIR])
CONTRACT_DIR = WORK_DIR / "contracts"
DATA_DIR = WORK_DIR / "data"
RECORDS_DIR = WORK_DIR / "records"
DATASETS_FILE = RECORDS_DIR / "datasets.json"
DQ_STATUS_DIR = RECORDS_DIR / "dq_state" / "status"

# Copy sample data and records into a temporary working directory so the
# application operates on absolute paths that are isolated per run.
shutil.copytree(SAMPLE_DIR / "data", DATA_DIR)
shutil.copytree(SAMPLE_DIR / "records", RECORDS_DIR)


_VERSION_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}(?:T[^_]+Z)?")
_DERIVED_SUFFIXES = {"valid", "reject"}
_VERSION_ALIASES = {
    "partial": "2024-04-10",
}


def _normalise_dataset_layout(root: Path) -> None:
    """Ensure datasets under ``root`` follow ``dataset/version/file`` layout."""

    for candidate in list(root.glob("*.json")):
        stem = candidate.stem
        if "_" not in stem:
            dataset_dir = root / stem
            if dataset_dir.exists():
                candidate.unlink()
            continue

        parts = stem.split("_")
        suffix: Optional[str] = None
        if parts[-1] in _DERIVED_SUFFIXES:
            suffix = parts.pop()

        version = parts[-1]
        version = _VERSION_ALIASES.get(version, version)
        if not _VERSION_PATTERN.fullmatch(version):
            continue

        dataset = "_".join(parts[:-1]) or parts[-1]
        if not dataset:
            continue

        dataset_dir_name = dataset if suffix is None else f"{dataset}__{suffix}"
        target_dir = root / dataset_dir_name / version
        target_dir.mkdir(parents=True, exist_ok=True)

        destination = target_dir / f"{dataset}.json"
        if destination.exists():
            candidate.unlink()
        else:
            candidate.rename(destination)


_normalise_dataset_layout(DATA_DIR)


def _safe_fs_name(value: str) -> str:
    """Return a filesystem-friendly representation for governance ids."""

    return "".join(ch if ch.isalnum() or ch in ("_", "-", ".") else "_" for ch in value)


def _read_json_file(path: Path) -> Optional[Dict[str, Any]]:
    """Return decoded JSON for ``path`` or ``None`` on failure."""

    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None


def _version_sort_key(value: str) -> tuple[int, Tuple[int, int, int] | float | str, str]:
    """Sort versions treating ISO timestamps and SemVer intelligently."""

    candidate = value
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(candidate)
        return (0, dt.timestamp(), value)
    except ValueError:
        pass
    try:
        parsed = SemVer.parse(value)
        return (1, (parsed.major, parsed.minor, parsed.patch), value)
    except ValueError:
        return (2, value, value)


def _sort_versions(entries: Iterable[str]) -> List[str]:
    """Return ``entries`` sorted using :func:`_version_sort_key`."""

    return sorted(entries, key=_version_sort_key)


def _dq_status_dir_for(dataset_id: str) -> Path:
    """Return the directory that stores compatibility statuses for ``dataset_id``."""

    return DQ_STATUS_DIR / _safe_fs_name(dataset_id)


def _dq_status_path(dataset_id: str, dataset_version: str) -> Path:
    """Return the JSON payload path for the supplied dataset/version pair."""

    directory = _dq_status_dir_for(dataset_id)
    return directory / f"{_safe_fs_name(dataset_version)}.json"


def _dq_status_payload(dataset_id: str, dataset_version: str) -> Optional[Dict[str, Any]]:
    """Load the compatibility payload if available."""

    path = _dq_status_path(dataset_id, dataset_version)
    if not path.exists():
        return None
    return _read_json_file(path)


def _dataset_root_for(dataset_id: str, dataset_path: Optional[str] = None) -> Optional[Path]:
    """Return the directory that should contain materialised versions."""

    base: Optional[Path] = None
    if dataset_path:
        try:
            path = Path(dataset_path)
        except (TypeError, ValueError):
            path = None
        if path is not None:
            if path.suffix:
                path = path.parent / path.stem
            if not path.is_absolute():
                path = (Path(DATA_DIR).parent / path).resolve()
            base = path
    if base is None and dataset_id:
        base = DATA_DIR / dataset_id.replace("::", "__")
    return base


def _version_marker_value(folder: Path) -> str:
    """Return the canonical version value for ``folder`` if annotated."""

    marker = folder / ".dc43_version"
    if marker.exists():
        try:
            text = marker.read_text().strip()
        except OSError:
            text = ""
        if text:
            return text
    return folder.name


def _candidate_version_paths(dataset_dir: Path, version: str) -> List[Path]:
    """Return directories that may correspond to ``version``."""

    candidates: List[Path] = []
    direct = dataset_dir / version
    candidates.append(direct)
    safe = dataset_dir / _safe_fs_name(version)
    if safe != direct:
        candidates.append(safe)
    try:
        for entry in dataset_dir.iterdir():
            if not entry.is_dir():
                continue
            if _version_marker_value(entry) == version and entry not in candidates:
                candidates.append(entry)
    except FileNotFoundError:
        return []
    return candidates


def _has_version_materialisation(dataset_dir: Path, version: str) -> bool:
    """Return ``True`` if ``dataset_dir`` contains files for ``version``."""

    lowered = version.lower()
    if lowered in {"latest", "current"} or lowered.startswith("latest__"):
        return True
    for candidate in _candidate_version_paths(dataset_dir, version):
        if candidate.exists():
            return True
    return False


def _existing_version_dir(dataset_dir: Path, version: str) -> Optional[Path]:
    """Return an existing directory matching ``version`` if available."""

    for candidate in _candidate_version_paths(dataset_dir, version):
        if candidate.exists():
            return candidate
    return None


def _target_version_dir(dataset_dir: Path, version: str) -> Path:
    """Return the directory path where ``version`` should be materialised."""

    safe = _safe_fs_name(version)
    if not safe:
        safe = "version"
    return dataset_dir / safe


def _ensure_version_marker(path: Path, version: str) -> None:
    """Record ``version`` inside ``path`` for lookup when sanitised."""

    if not path.exists() or not path.is_dir():
        return
    marker = path / ".dc43_version"
    try:
        marker.write_text(version)
    except OSError:
        pass


def _dq_status_entries(dataset_id: str) -> List[Tuple[str, str, Dict[str, Any]]]:
    """Return (display_version, stored_version, payload) tuples."""

    directory = _dq_status_dir_for(dataset_id)
    entries: List[Tuple[str, str, Dict[str, Any]]] = []
    if not directory.exists():
        return entries
    for path in directory.glob("*.json"):
        payload = _read_json_file(path) or {}
        display_version = str(payload.get("dataset_version") or path.stem)
        entries.append((display_version, path.stem, payload))
    entries.sort(key=lambda item: _version_sort_key(item[0]))
    return entries


def _dq_status_versions(dataset_id: str) -> List[str]:
    """Return known dataset versions recorded by the governance stub."""

    return [entry[0] for entry in _dq_status_entries(dataset_id)]


def _link_path(target: Path, source: Path) -> None:
    """Create a symlink (or copy fallback) from ``target`` to ``source``."""

    if target.exists() or target.is_symlink():
        try:
            if target.is_symlink() and target.resolve() == source.resolve():
                return
        except OSError:
            pass
        if target.is_dir() and not target.is_symlink():
            shutil.rmtree(target)
        else:
            target.unlink()

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
    """Return sorted dataset version directories ignoring alias folders."""

    versions: list[Path] = []
    for candidate in dataset_dir.iterdir():
        if not candidate.is_dir():
            continue
        name = candidate.name
        if name == "latest" or name.startswith("latest__"):
            continue
        versions.append(candidate)
    return sorted(versions)


def refresh_dataset_aliases(dataset: str | None = None) -> None:
    """Populate ``latest``/derived aliases for the selected dataset(s)."""

    roots: list[Path]
    if dataset:
        base = DATA_DIR / dataset
        roots = [base] if base.exists() else []
    else:
        roots = [p for p in DATA_DIR.iterdir() if p.is_dir() and "__" not in p.name]

    for dataset_dir in roots:
        versions = _iter_versions(dataset_dir)
        if not versions:
            continue
        latest = versions[-1]
        _link_path(dataset_dir / "latest", latest)

        derived_dirs = sorted(DATA_DIR.glob(f"{dataset_dir.name}__*"))
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


def set_active_version(dataset: str, version: str) -> None:
    """Point the ``latest`` alias of ``dataset`` (and derivatives) to ``version``."""

    dataset_dir = DATA_DIR / dataset
    target = _existing_version_dir(dataset_dir, version)
    if target is None:
        target = _target_version_dir(dataset_dir, version)
    if not target.exists():
        raise FileNotFoundError(f"Unknown dataset version: {dataset} {version}")

    _link_path(dataset_dir / "latest", target)

    if "__" not in dataset:
        for derived_dir in DATA_DIR.glob(f"{dataset}__*"):
            suffix = derived_dir.name.split("__", 1)[1]
            derived_target = _existing_version_dir(derived_dir, version)
            if derived_target is None:
                continue
            _link_path(target / suffix, derived_target)
            _link_path(dataset_dir / f"latest__{suffix}", derived_target)
    else:
        base, suffix = dataset.split("__", 1)
        base_dir = DATA_DIR / base
        version_dir = _existing_version_dir(base_dir, version)
        if version_dir is not None and version_dir.exists():
            _link_path(version_dir / suffix, target)
            _link_path(base_dir / f"latest__{suffix}", target)


def register_dataset_version(dataset: str, version: str, source: Path) -> None:
    """Expose ``source`` under ``data/<dataset>/<version>`` via symlink."""

    dataset_dir = DATA_DIR / dataset
    dataset_dir.mkdir(parents=True, exist_ok=True)
    target = _target_version_dir(dataset_dir, version)
    _link_path(target, source)
    _ensure_version_marker(target, version)


refresh_dataset_aliases()
try:
    set_active_version("customers", "2024-01-01")
    set_active_version("orders", "2024-01-01")
    set_active_version("orders__valid", "2025-09-28")
    set_active_version("orders__reject", "2025-09-28")
except FileNotFoundError:
    # Sample data may be absent during tests that override the workspace.
    pass

# Prepare contracts with absolute server paths pointing inside the working dir.
for src in (SAMPLE_DIR / "contracts").rglob("*.json"):
    model = OpenDataContractStandard.model_validate_json(src.read_text())
    for srv in model.servers or []:
        p = Path(srv.path or "")
        if not p.is_absolute():
            p = (WORK_DIR / p).resolve()
        base = p.parent if p.suffix else p
        base.mkdir(parents=True, exist_ok=True)
        srv.path = str(p)
    dest = CONTRACT_DIR / src.relative_to(SAMPLE_DIR / "contracts")
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(
        model.model_dump_json(indent=2, by_alias=True, exclude_none=True),
        encoding="utf-8",
    )

store = FSContractStore(str(CONTRACT_DIR))

# Populate server paths with sample datasets matching recorded versions
_sample_records = json.loads((RECORDS_DIR / "datasets.json").read_text())
for _r in _sample_records:
    try:
        _c = store.get(_r["contract_id"], _r["contract_version"])
    except FileNotFoundError:
        continue
    _srv = (_c.servers or [None])[0]
    if not _srv or not _srv.path:
        continue
    _dest = Path(_srv.path)
    base = _dest.parent if _dest.suffix else _dest
    if base.name == _r["dataset_name"]:
        target_root = base / _r["dataset_version"]
    else:
        target_root = base / _r["dataset_name"] / _r["dataset_version"]
    _base_dir = SAMPLE_DIR / "data" / _r["dataset_name"]
    _src_dir = _base_dir / _r["dataset_version"]
    if _src_dir.is_dir():
        shutil.copytree(_src_dir, target_root, dirs_exist_ok=True)
        continue
    _src_file = SAMPLE_DIR / "data" / f"{_r['dataset_name']}.json"
    if not _src_file.exists():
        continue
    target_root.mkdir(parents=True, exist_ok=True)
    shutil.copy2(_src_file, target_root / _src_file.name)

app = FastAPI(title="DC43 Demo")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static"), check_dir=False), name="static")


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
    draft_contract_version: str | None = None


_STATUS_BADGES: Dict[str, str] = {
    "kept": "bg-success",
    "updated": "bg-primary",
    "relaxed": "bg-warning text-dark",
    "removed": "bg-danger",
    "added": "bg-info text-dark",
    "missing": "bg-secondary",
    "error": "bg-danger",
    "warning": "bg-warning text-dark",
    "not_nullable": "bg-info text-dark",
}


_DQ_STATUS_BADGES: Dict[str, str] = {
    "ok": "bg-success",
    "warn": "bg-warning text-dark",
    "block": "bg-danger",
    "stale": "bg-secondary",
    "unknown": "bg-secondary",
}


def _dq_version_records(
    dataset_id: str,
    *,
    contract: Optional[OpenDataContractStandard] = None,
    dataset_path: Optional[str] = None,
    dataset_records: Optional[Iterable[DatasetRecord]] = None,
) -> List[Dict[str, Any]]:
    """Return version → status entries for the supplied dataset id.

    ``dataset_records`` can be provided to scope compatibility information to
    runs that were produced for a specific contract version. This ensures, for
    example, that the compatibility matrix rendered for ``orders`` version
    ``1.0.0`` does not surface the validation outcome that belongs to the
    ``1.1.0`` contract.
    """

    records: List[Dict[str, Any]] = []
    entries = _dq_status_entries(dataset_id)

    scoped_versions: set[str] = set()
    dataset_record_map: Dict[str, DatasetRecord] = {}
    if dataset_records:
        for record in dataset_records:
            if not record.dataset_version:
                continue
            scoped_versions.add(record.dataset_version)
            dataset_record_map[record.dataset_version] = record

    dataset_dir = _dataset_root_for(dataset_id, dataset_path)
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
        if not skip_fs_check and dataset_dir is not None:
            if not _has_version_materialisation(dataset_dir, display_version):
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

    # If we scoped by contract runs, surface any versions without a stored DQ
    # payload using the dataset records so the UI can still display a verdict.
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


def _server_details(contract: OpenDataContractStandard) -> Optional[Dict[str, Any]]:
    """Summarise the first server entry for UI consumption."""

    if not contract.servers:
        return None
    first = contract.servers[0]
    custom: Dict[str, Any] = {}
    for item in getattr(first, "customProperties", []) or []:
        key = getattr(item, "property", None)
        if key:
            custom[key] = item.value
    dataset_id = contract.id or getattr(first, "dataset", None) or contract.id
    info: Dict[str, Any] = {
        "server": getattr(first, "server", ""),
        "type": getattr(first, "type", ""),
        "format": getattr(first, "format", ""),
        "path": getattr(first, "path", ""),
        "dataset": getattr(first, "dataset", ""),
        "dataset_id": dataset_id,
    }
    if custom:
        info["custom"] = custom
        if "dc43.versioning" in custom:
            info["versioning"] = custom.get("dc43.versioning")
        if "dc43.pathPattern" in custom:
            info["path_pattern"] = custom.get("dc43.pathPattern")
    return info


def _format_scope(scope: str | None) -> str:
    """Return a human readable label for change log scopes."""

    if not scope or scope == "contract":
        return "Contract"
    if scope.startswith("field:"):
        return f"Field {scope.split(':', 1)[1]}"
    return scope.replace("_", " ").title()


def _stringify_value(value: Any) -> str:
    """Return a readable representation for rule parameter values."""

    if isinstance(value, (list, tuple, set)):
        return ", ".join(str(item) for item in value)
    return str(value)


def _quality_rule_summary(dq: DataQuality) -> Dict[str, Any]:
    """Produce a structured summary for a data-quality rule."""

    conditions: List[str] = []
    if dq.description:
        conditions.append(str(dq.description))

    if dq.mustBeGreaterThan is not None:
        conditions.append(f"Value must be greater than {dq.mustBeGreaterThan}")
    if dq.mustBeGreaterOrEqualTo is not None:
        conditions.append(f"Value must be greater than or equal to {dq.mustBeGreaterOrEqualTo}")
    if dq.mustBeLessThan is not None:
        conditions.append(f"Value must be less than {dq.mustBeLessThan}")
    if dq.mustBeLessOrEqualTo is not None:
        conditions.append(f"Value must be less than or equal to {dq.mustBeLessOrEqualTo}")
    if dq.mustBeBetween:
        low, high = dq.mustBeBetween
        conditions.append(f"Value must be between {low} and {high}")
    if dq.mustNotBeBetween:
        low, high = dq.mustNotBeBetween
        conditions.append(f"Value must not be between {low} and {high}")

    if dq.mustBe is not None:
        if (dq.rule or "").lower() == "regex":
            conditions.append(f"Value must match the pattern {dq.mustBe}")
        elif isinstance(dq.mustBe, (list, tuple, set)):
            conditions.append(
                "Value must be one of: " + ", ".join(str(item) for item in dq.mustBe)
            )
        else:
            conditions.append(f"Value must be {_stringify_value(dq.mustBe)}")

    if dq.mustNotBe is not None:
        if isinstance(dq.mustNotBe, (list, tuple, set)):
            conditions.append(
                "Value must not be any of: "
                + ", ".join(str(item) for item in dq.mustNotBe)
            )
        else:
            conditions.append(f"Value must not be {_stringify_value(dq.mustNotBe)}")

    if dq.query:
        engine = (dq.engine or "spark_sql").replace("_", " ")
        conditions.append(f"Query ({engine}): {dq.query}")

    if not conditions:
        label = dq.rule or dq.name or "rule"
        conditions.append(f"See contract metadata for details on {label}.")

    title = dq.name or dq.rule or "Rule"
    title = title.replace("_", " ").title()

    return {
        "title": title,
        "conditions": conditions,
        "severity": dq.severity,
        "dimension": dq.dimension,
    }


def _field_quality_sections(contract: OpenDataContractStandard) -> List[Dict[str, Any]]:
    """Return quality rule summaries grouped per field."""

    sections: List[Dict[str, Any]] = []
    for obj in contract.schema_ or []:
        for prop in obj.properties or []:
            rules: List[Dict[str, Any]] = []
            if prop.required:
                rules.append(
                    {
                        "title": "Required",
                        "conditions": [
                            "Field must always be present (non-null values required)."
                        ],
                    }
                )
            if prop.unique:
                rules.append(
                    {
                        "title": "Unique",
                        "conditions": [
                            "Each record must contain a distinct value for this field.",
                        ],
                    }
                )
            for dq in prop.quality or []:
                rules.append(_quality_rule_summary(dq))

            sections.append(
                {
                    "name": prop.name or "",
                    "type": prop.physicalType or "",
                    "required": bool(prop.required),
                    "rules": rules,
                }
            )
    return sections


def _dataset_quality_sections(contract: OpenDataContractStandard) -> List[Dict[str, Any]]:
    """Return dataset-level quality rules defined on schema objects."""

    sections: List[Dict[str, Any]] = []
    for obj in contract.schema_ or []:
        rules = [_quality_rule_summary(dq) for dq in obj.quality or []]
        if rules:
            sections.append({"name": obj.name or contract.id or "dataset", "rules": rules})
    return sections


def _summarise_change_entry(entry: Mapping[str, Any]) -> str:
    details = entry.get("details")
    if isinstance(details, Mapping):
        for key in ("message", "reason"):
            message = details.get(key)
            if message:
                return str(message)
    target = entry.get("constraint") or entry.get("rule") or entry.get("kind")
    status = entry.get("status")
    if target and status:
        return f"{str(target).replace('_', ' ').title()} {str(status).replace('_', ' ')}."
    if status:
        return str(status).replace("_", " ").title()
    return ""


def _contract_change_log(contract: OpenDataContractStandard) -> List[Dict[str, Any]]:
    """Extract change log entries from the contract custom properties."""

    entries: List[Dict[str, Any]] = []
    for prop in contract.customProperties or []:
        if prop.property != "draft_change_log":
            continue
        for item in prop.value or []:
            if not isinstance(item, Mapping):
                continue
            details = item.get("details")
            details_text = ""
            if details is not None:
                try:
                    details_text = json.dumps(details, indent=2, sort_keys=True, default=str)
                except TypeError:
                    details_text = str(details)
            status = str(item.get("status", ""))
            entries.append(
                {
                    "scope": item.get("scope", ""),
                    "scope_label": _format_scope(item.get("scope")),
                    "kind": item.get("kind", ""),
                    "status": status,
                    "status_label": status.replace("_", " ").title(),
                    "constraint": item.get("constraint"),
                    "rule": item.get("rule"),
                    "summary": _summarise_change_entry(item),
                    "details_text": details_text,
                }
            )
        break
    return entries


def load_records() -> List[DatasetRecord]:
    raw = json.loads(DATASETS_FILE.read_text())
    return [DatasetRecord(**r) for r in raw]


def save_records(records: List[DatasetRecord]) -> None:
    DATASETS_FILE.write_text(
        json.dumps([r.__dict__ for r in records], indent=2), encoding="utf-8"
    )


# Default slice activations used to drive the ``latest`` aliases for scenarios.
_DEFAULT_SLICE = {
    "orders": "2024-01-01",
    "customers": "2024-01-01",
}

_INVALID_SLICE = {
    "orders": "2025-09-28",
    "orders__valid": "2025-09-28",
    "orders__reject": "2025-09-28",
    "customers": "2024-01-01",
}

# Predefined pipeline scenarios exposed in the UI. Each scenario describes the
# parameters passed to the example pipeline along with a human readable
# description shown to the user.
SCENARIOS: Dict[str, Dict[str, Any]] = {
    "no-contract": {
        "label": "No contract provided",
        "description": (
            "<p>Run the pipeline without supplying an output contract.</p>"
            "<ul>"
            "<li><strong>Inputs:</strong> Reads <code>orders:1.1.0</code> and "
            "<code>customers:1.0.0</code> with schema validation.</li>"
            "<li><strong>Contract:</strong> None provided, so no draft can be"
            " created.</li>"
            "<li><strong>Writes:</strong> Planned dataset <code>result-no-existing-contract</code>"
            " is blocked before any files are materialised, so no version is"
            " assigned.</li>"
            "<li><strong>Status:</strong> The run exits with an error because the contract is"
            " missing.</li>"
            "</ul>"
        ),
        "diagram": (
            "<div class=\"mermaid\">"
            + dedent(
                """
                flowchart TD
                    Orders["orders latest → 2024-01-01\ncontract orders:1.1.0"] --> Join[Join datasets]
                    Customers["customers latest → 2024-01-01\ncontract customers:1.0.0"] --> Join
                    Join --> Write["Plan result-no-existing-contract\nno output contract"]
                    Write -->|no contract| Block[Run blocked, nothing written]
                """
            ).strip()
            + "</div>"
        ),
        "activate_versions": dict(_DEFAULT_SLICE),
        "params": {
            "contract_id": None,
            "contract_version": None,
            "dataset_name": "result-no-existing-contract",
            "run_type": "enforce",
        },
    },
    "ok": {
        "label": "Existing contract OK",
        "description": (
            "<p>Happy path using contract <code>orders_enriched:1.0.0</code>.</p>"
            "<ul>"
            "<li><strong>Inputs:</strong> Reads <code>orders:1.1.0</code> and"
            " <code>customers:1.0.0</code> then aligns to the target schema.</li>"
            "<li><strong>Contract:</strong> Targets <code>orders_enriched:1.0.0</code>"
            " with no draft changes.</li>"
            "<li><strong>Writes:</strong> Persists dataset <code>orders_enriched</code>"
            " tagged with the run timestamp so repeated runs never collide.</li>"
            "<li><strong>Status:</strong> Post-write validation reports OK.</li>"
            "</ul>"
        ),
        "diagram": (
            "<div class=\"mermaid\">"
            + dedent(
                """
                flowchart TD
                    Orders["orders latest → 2024-01-01\ncontract orders:1.1.0"] --> Join[Join datasets]
                    Customers["customers latest → 2024-01-01\ncontract customers:1.0.0"] --> Join
                    Join --> Validate[Align to contract orders_enriched:1.0.0]
                    Validate --> Write["orders_enriched «timestamp»\ncontract orders_enriched:1.0.0"]
                    Write --> Status[Run status: OK]
                """
            ).strip()
            + "</div>"
        ),
        "activate_versions": dict(_DEFAULT_SLICE),
        "params": {
            "contract_id": "orders_enriched",
            "contract_version": "1.0.0",
            "run_type": "enforce",
        },
    },
    "dq": {
        "label": "Existing contract fails DQ",
        "description": (
            "<p>Demonstrates a data quality failure.</p>"
            "<ul>"
            "<li><strong>Inputs:</strong> Reads <code>orders:1.1.0</code> and"
            " <code>customers:1.0.0</code>.</li>"
            "<li><strong>Contract:</strong> Validates against"
            " <code>orders_enriched:1.1.0</code> and prepares draft"
            " <code>orders_enriched:1.2.0</code>.</li>"
            "<li><strong>Writes:</strong> Persists"
            " <code>orders_enriched</code> with the run timestamp before"
            " governance flips the outcome to <code>block</code> and records"
            " draft <code>orders_enriched:1.2.0</code>.</li>"
            "<li><strong>Status:</strong> The enforcement run errors when rule"
            " <code>amount &gt; 100</code> is violated.</li>"
            "</ul>"
        ),
        "diagram": (
            "<div class=\"mermaid\">"
            + dedent(
                """
                flowchart TD
                    Orders["orders latest → 2024-01-01\ncontract orders:1.1.0"] --> Join[Join datasets]
                    Customers["customers latest → 2024-01-01\ncontract customers:1.0.0"] --> Join
                    Join --> Write["orders_enriched «timestamp»\ncontract orders_enriched:1.1.0"]
                    Write --> Governance[Post-write validation]
                    Governance --> Draft[Draft orders_enriched 1.2.0]
                    Governance -->|violations| Block["DQ verdict: block"]
                """
            ).strip()
            + "</div>"
        ),
        "activate_versions": dict(_DEFAULT_SLICE),
        "params": {
            "contract_id": "orders_enriched",
            "contract_version": "1.1.0",
            "run_type": "enforce",
            "collect_examples": True,
            "examples_limit": 3,
        },
    },
    "schema-dq": {
        "label": "Contract fails schema and DQ",
        "description": (
            "<p>Shows combined schema and data quality issues.</p>"
            "<ul>"
            "<li><strong>Inputs:</strong> Reads <code>orders:1.1.0</code> and"
            " <code>customers:1.0.0</code>.</li>"
            "<li><strong>Contract:</strong> Targets <code>orders_enriched:2.0.0</code>"
            " and proposes draft <code>orders_enriched:2.1.0</code>.</li>"
            "<li><strong>Writes:</strong> Persists"
            " <code>orders_enriched</code> with the run timestamp, then"
            " validation downgrades the outcome to <code>block</code> while"
            " recording draft <code>orders_enriched:2.1.0</code>.</li>"
            "<li><strong>Status:</strong> Schema drift plus failed expectations"
            " produce an error outcome.</li>"
            "</ul>"
        ),
        "diagram": (
            "<div class=\"mermaid\">"
            + dedent(
                """
                flowchart TD
                    Orders["orders latest → 2024-01-01\ncontract orders:1.1.0"] --> Join[Join datasets]
                    Customers["customers latest → 2024-01-01\ncontract customers:1.0.0"] --> Join
                    Join --> Align[Schema align to contract orders_enriched:2.0.0]
                    Align --> Write["orders_enriched «timestamp»\ncontract orders_enriched:2.0.0"]
                    Write --> Governance[Post-write validation]
                    Governance --> Draft[Draft orders_enriched 2.1.0]
                    Governance -->|violations| Block["DQ verdict: block"]
                """
            ).strip()
            + "</div>"
        ),
        "activate_versions": dict(_DEFAULT_SLICE),
        "params": {
            "contract_id": "orders_enriched",
            "contract_version": "2.0.0",
            "run_type": "enforce",
        },
    },
    "read-invalid-block": {
        "label": "Invalid input blocked",
        "description": (
            "<p>Attempts to process the latest slice (→2025-09-28) flagged as invalid.</p>"
            "<ul>"
            "<li><strong>Inputs:</strong> Governance records mark"
            " <code>orders latest → 2025-09-28</code> as <code>block</code> while pointing"
            " at curated <code>valid</code> and <code>reject</code> slices.</li>"
            "<li><strong>Contract:</strong> Targets <code>orders_enriched:1.1.0</code>"
            " but enforcement aborts before writes.</li>"
            "<li><strong>Outputs:</strong> None; the job fails fast.</li>"
            "<li><strong>Governance:</strong> Stub DQ client returns the stored"
            " `block` verdict and its auxiliary dataset hints.</li>"
            "</ul>"
        ),
        "diagram": (
            "<div class=\"mermaid\">"
            + dedent(
                """
                flowchart TD
                    Invalid["orders latest → 2025-09-28\ncontract orders:1.1.0\nDQ status: block"] -->|default enforcement| Halt[Read aborted]
                    Invalid -.-> Valid["orders::valid latest__valid → 2025-09-28\ncontract orders:1.1.0"]
                    Invalid -.-> Reject["orders::reject latest__reject → 2025-09-28\ncontract orders:1.1.0"]
                """
            ).strip()
            + "</div>"
        ),
        "activate_versions": dict(_INVALID_SLICE),
        "params": {
            "contract_id": "orders_enriched",
            "contract_version": "1.1.0",
            "run_type": "enforce",
            "inputs": {
                "orders": {
                    "dataset_version": "latest",
                }
            },
        },
    },
    "read-valid-subset": {
        "label": "Prefer valid subset",
        "description": (
            "<p>Steers reads toward the curated valid slice.</p>"
            "<ul>"
            "<li><strong>Inputs:</strong> Uses <code>orders::valid</code>"
            " <code>latest__valid → 2025-09-28</code> alongside"
            " <code>customers latest → 2024-01-01</code> to satisfy governance.</li>"
            "<li><strong>Contract:</strong> Applies <code>orders_enriched:1.1.0</code>"
            " and keeps draft creation disabled.</li>"
            "<li><strong>Outputs:</strong> Writes <code>orders_enriched</code>"
            " stamped with the run timestamp under contract"
            " <code>orders_enriched:1.1.0</code> with a clean DQ verdict.</li>"
            "<li><strong>Governance:</strong> Stub evaluates post-write metrics"
            " and records an OK status.</li>"
            "</ul>"
        ),
        "diagram": (
            "<div class=\"mermaid\">"
            + dedent(
                """
                flowchart TD
                    Valid["orders::valid latest__valid → 2025-09-28\ncontract orders:1.1.0"] --> Join[Join datasets]
                    Customers["customers latest → 2024-01-01\ncontract customers:1.0.0"] --> Join
                    Join --> Write["orders_enriched «timestamp»\ncontract orders_enriched:1.1.0"]
                    Write --> Governance[Governance verdict ok]
                    Governance --> Status["DQ status: ok"]
                """
            ).strip()
            + "</div>"
        ),
        "activate_versions": dict(_INVALID_SLICE),
        "params": {
            "contract_id": "orders_enriched",
            "contract_version": "1.1.0",
            "run_type": "observe",
            "collect_examples": True,
            "examples_limit": 3,
            "inputs": {
                "orders": {
                    "dataset_id": "orders::valid",
                    "dataset_version": "latest__valid",
                }
            },
        },
    },
    "read-valid-subset-violation": {
        "label": "Valid subset, invalid output",
        "description": (
            "<p>Highlights when clean inputs still breach the output contract.</p>"
            "<ul>"
            "<li><strong>Inputs:</strong> Same curated"
            " <code>orders::valid</code> <code>latest__valid → 2025-09-28</code> slice.</li>"
            "<li><strong>Contract:</strong> Writes to"
            " <code>orders_enriched</code> under <code>orders_enriched:1.1.0</code>.</li>"
            "<li><strong>Outputs:</strong> Produces <code>orders_enriched</code>"
            " (timestamped under contract <code>1.1.0</code>) but post-write checks fail because"
            " the demo purposely lowers one amount below the"
            " <code>&gt; 100</code> expectation.</li>"
            "<li><strong>Governance:</strong> Stub DQ client records a blocking"
            " verdict and drafts <code>orders_enriched:1.2.0</code>.</li>"
            "</ul>"
        ),
        "diagram": (
            "<div class=\"mermaid\">"
            + dedent(
                """
                flowchart TD
                    Valid["orders::valid latest__valid → 2025-09-28\ncontract orders:1.1.0"] --> Join[Join datasets]
                    Join --> Adjust[Lower amount to 60]
                    Adjust --> Write["orders_enriched «timestamp»\ncontract orders_enriched:1.1.0"]
                    Write --> Governance[Governance verdict block]
                    Governance --> Draft["Draft orders_enriched 1.2.0"]
                    Governance --> Status["DQ status: block"]
                """
            ).strip()
            + "</div>"
        ),
        "activate_versions": dict(_INVALID_SLICE),
        "params": {
            "contract_id": "orders_enriched",
            "contract_version": "1.1.0",
            "run_type": "enforce",
            "collect_examples": True,
            "examples_limit": 3,
            "inputs": {
                "orders": {
                    "dataset_id": "orders::valid",
                    "dataset_version": "latest__valid",
                }
            },
            "output_adjustment": "valid-subset-violation",
        },
    },
    "read-override-full": {
        "label": "Force blocked slice (manual override)",
        "description": (
            "<p>Documents what happens when the blocked data is forced through.</p>"
            "<ul>"
            "<li><strong>Inputs:</strong> Reuses the blocked"
            " <code>orders latest → 2025-09-28</code> and downgrades the read status to"
            " <code>warn</code>.</li>"
            "<li><strong>Override strategy:</strong> Uses"
            " <code>allow-block</code> to document that the blocked slice was"
            " manually forced through despite the governance verdict.</li>"
            "<li><strong>Contract:</strong> Applies"
            " <code>orders_enriched:1.1.0</code> and captures draft"
            " <code>orders_enriched:1.2.0</code>.</li>"
            "<li><strong>Outputs:</strong> Writes <code>orders_enriched</code>"
            " (timestamped under contract <code>1.1.0</code>) while surfacing the manual override"
            " note alongside the reject-row metrics.</li>"
            "<li><strong>Governance:</strong> Stub records the downgrade in the"
            " run summary alongside violation counts and the explicit override"
            " note.</li>"
            "</ul>"
        ),
        "diagram": (
            "<div class=\"mermaid\">"
            + dedent(
                """
                flowchart TD
                    Invalid["orders latest → 2025-09-28\ncontract orders:1.1.0\nDQ status: block"] --> Override[Downgrade to warn]
                    Override --> Write["orders_enriched «timestamp»\ncontract orders_enriched:1.1.0"]
                    Write --> Governance[Governance verdict warn]
                    Governance --> Draft["Draft orders_enriched 1.2.0"]
                    Governance --> Status["DQ status: warn"]
                """
            ).strip()
            + "</div>"
        ),
        "activate_versions": dict(_INVALID_SLICE),
        "params": {
            "contract_id": "orders_enriched",
            "contract_version": "1.1.0",
            "run_type": "observe",
            "collect_examples": True,
            "examples_limit": 3,
            "inputs": {
                "orders": {
                    "dataset_version": "latest",
                    "status_strategy": {
                        "name": "allow-block",
                        "note": "Manual override: forced latest slice (→2025-09-28)",
                        "target_status": "warn",
                    },
                }
            },
            "output_adjustment": "amplify-negative",
        },
    },
    "split-lenient": {
        "label": "Split invalid rows",
        "description": (
            "<p>Routes violations to dedicated datasets using the split strategy.</p>"
            "<ul>"
            "<li><strong>Inputs:</strong> Reads <code>orders:1.1.0</code> and"
            " <code>customers:1.0.0</code> before aligning to"
            " <code>orders_enriched:1.1.0</code>.</li>"
            "<li><strong>Contract:</strong> Validates against"
            " <code>orders_enriched:1.1.0</code> and stores draft"
            " <code>orders_enriched:1.2.0</code> when rejects exist.</li>"
            "<li><strong>Writes:</strong> Persists three datasets sharing the same"
            " timestamp: the contracted"
            " <code>orders_enriched</code> (full slice),"
            " <code>orders_enriched::valid</code>, and"
            " <code>orders_enriched::reject</code>.</li>"
            "<li><strong>Status:</strong> Run finishes with a warning because"
            " validation finds violations, and the UI links the auxiliary"
            " datasets.</li>"
            "</ul>"
        ),
        "diagram": (
            "<div class=\"mermaid\">"
            + dedent(
                """
                flowchart TD
                    Orders["orders latest → 2024-01-01\ncontract orders:1.1.0"] --> Join[Join datasets]
                    Customers["customers latest → 2024-01-01\ncontract customers:1.0.0"] --> Join
                    Join --> Validate[Validate contract orders_enriched:1.1.0]
                    Validate --> Strategy[Split strategy]
                    Strategy --> Full["orders_enriched «timestamp»\ncontract orders_enriched:1.1.0"]
                    Strategy --> Valid["orders_enriched::valid «timestamp»\ncontract orders_enriched:1.1.0"]
                    Strategy --> Reject["orders_enriched::reject «timestamp»\ncontract orders_enriched:1.1.0"]
                """
            ).strip()
            + "</div>"
        ),
        "activate_versions": dict(_DEFAULT_SLICE),
        "params": {
            "contract_id": "orders_enriched",
            "contract_version": "1.1.0",
            "run_type": "observe",
            "collect_examples": True,
            "examples_limit": 3,
            "violation_strategy": {
                "name": "split",
                "include_valid": True,
                "include_reject": True,
                "write_primary_on_violation": True,
            },
        },
    },
}


_FLASH_LOCK = Lock()
_FLASH_MESSAGES: Dict[str, Dict[str, str | None]] = {}


def queue_flash(message: str | None = None, error: str | None = None) -> str:
    """Store a transient flash payload and return a lookup token."""

    token = uuid4().hex
    with _FLASH_LOCK:
        _FLASH_MESSAGES[token] = {"message": message, "error": error}
    return token


def pop_flash(token: str) -> Tuple[str | None, str | None]:
    """Return and remove the flash payload associated with ``token``."""

    with _FLASH_LOCK:
        payload = _FLASH_MESSAGES.pop(token, None) or {}
    return payload.get("message"), payload.get("error")


def load_contract_meta() -> List[Dict[str, Any]]:
    """Return contract info derived from the store without extra metadata."""
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
                if getattr(server, "path", None):
                    parts.append(server.path)
                if getattr(server, "dataset", None):
                    parts.append(server.dataset)
                path = "/".join(parts)
            meta.append({"id": cid, "version": ver, "path": path})
    return meta


def save_contract_meta(meta: List[Dict[str, Any]]) -> None:
    """No-op retained for backwards compatibility."""
    return None


def contract_to_dict(c: OpenDataContractStandard) -> Dict[str, Any]:
    """Return a plain dict for a contract using public field aliases."""
    try:
        return c.model_dump(by_alias=True, exclude_none=True)
    except AttributeError:  # pragma: no cover - Pydantic v1 fallback
        return c.dict(by_alias=True, exclude_none=True)  # type: ignore[call-arg]


@app.get("/api/contracts")
async def api_contracts() -> List[Dict[str, Any]]:
    return load_contract_meta()


@app.get("/api/contracts/{cid}/{ver}")
async def api_contract_detail(cid: str, ver: str) -> Dict[str, Any]:
    try:
        contract = store.get(cid, ver)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    datasets = [r.__dict__ for r in load_records() if r.contract_id == cid and r.contract_version == ver]
    expectations = expectations_from_contract(contract)
    return {
        "contract": contract_to_dict(contract),
        "datasets": datasets,
        "expectations": expectations,
    }


@app.get("/api/contracts/{cid}/{ver}/preview")
async def api_contract_preview(
    cid: str,
    ver: str,
    dataset_version: Optional[str] = None,
    dataset_id: Optional[str] = None,
    limit: int = 100,
) -> Dict[str, Any]:
    if read_with_contract is None or ContractVersionLocator is None:
        raise HTTPException(status_code=503, detail="pyspark is required for data previews")
    try:
        contract = store.get(cid, ver)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    effective_dataset_id = str(dataset_id or contract.id or cid)
    server = (contract.servers or [None])[0]
    dataset_path_hint = getattr(server, "path", None) if server else None
    version_contract = contract if effective_dataset_id == (contract.id or cid) else None
    scoped_records = [
        record
        for record in load_records()
        if record.contract_id == cid
        and record.contract_version == ver
        and record.dataset_name == effective_dataset_id
    ]
    version_records = _dq_version_records(
        effective_dataset_id,
        contract=version_contract,
        dataset_path=dataset_path_hint if version_contract else None,
        dataset_records=scoped_records,
    )
    known_versions = [entry["version"] for entry in version_records]
    if not known_versions:
        known_versions = ["latest"]
    selected_version = str(dataset_version or known_versions[-1])
    if selected_version not in known_versions:
        known_versions = _sort_versions([*known_versions, selected_version])
    limit = max(1, min(limit, 500))

    try:
        spark = _spark_session()
        locator = ContractVersionLocator(
            dataset_version=selected_version,
            dataset_id=effective_dataset_id,
        )
        df = read_with_contract(  # type: ignore[misc]
            spark,
            contract_id=cid,
            contract_store=store,
            expected_contract_version=f"=={ver}",
            dataset_locator=locator,
            enforce=False,
            auto_cast=False,
            return_status=False,
        )
        rows_raw = [row.asDict(recursive=True) for row in df.limit(limit).collect()]
        rows = jsonable_encoder(rows_raw)
        columns = list(df.columns)
    except Exception as exc:  # pragma: no cover - defensive guard for preview errors
        raise HTTPException(status_code=500, detail=str(exc))

    status_payload = _dq_status_payload(effective_dataset_id, selected_version)
    status_value = str(status_payload.get("status", "unknown")) if status_payload else "unknown"
    response = {
        "dataset_id": effective_dataset_id,
        "dataset_version": selected_version,
        "rows": rows,
        "columns": columns,
        "limit": limit,
        "known_versions": known_versions,
        "status": {
            "status": status_value,
            "status_label": status_value.replace("_", " ").title(),
            "badge": _DQ_STATUS_BADGES.get(status_value, "bg-secondary"),
            "details": status_payload.get("details") if status_payload else None,
        },
    }
    return response


@app.post("/api/contracts/{cid}/{ver}/validate")
async def api_validate_contract(cid: str, ver: str) -> Dict[str, str]:
    return {"status": "active"}


@app.get("/api/datasets")
async def api_datasets() -> List[Dict[str, Any]]:
    records = load_records()
    return [r.__dict__.copy() for r in records]


@app.get("/api/datasets/{dataset_version}")
async def api_dataset_detail(dataset_version: str) -> Dict[str, Any]:
    for r in load_records():
        if r.dataset_version == dataset_version:
            contract = store.get(r.contract_id, r.contract_version)
            return {
                "record": r.__dict__,
                "contract": contract_to_dict(contract),
                "expectations": expectations_from_contract(contract),
            }
    raise HTTPException(status_code=404, detail="Dataset not found")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/contracts", response_class=HTMLResponse)
async def list_contracts(request: Request) -> HTMLResponse:
    contract_ids = store.list_contracts()
    return templates.TemplateResponse(
        "contracts.html", {"request": request, "contracts": contract_ids}
    )


@app.get("/contracts/{cid}", response_class=HTMLResponse)
async def list_contract_versions(request: Request, cid: str) -> HTMLResponse:
    versions = store.list_versions(cid)
    if not versions:
        raise HTTPException(status_code=404, detail="Contract not found")
    contracts = []
    for ver in versions:
        try:
            contract = store.get(cid, ver)
        except FileNotFoundError:
            continue
        server = (contract.servers or [None])[0]
        path = ""
        if server:
            parts: List[str] = []
            if getattr(server, "path", None):
                parts.append(server.path)
            if getattr(server, "dataset", None):
                parts.append(server.dataset)
            path = "/".join(parts)
        contracts.append({"id": cid, "version": ver, "path": path})
    context = {"request": request, "contract_id": cid, "contracts": contracts}
    return templates.TemplateResponse("contract_versions.html", context)


@app.get("/contracts/{cid}/{ver}", response_class=HTMLResponse)
async def contract_detail(request: Request, cid: str, ver: str) -> HTMLResponse:
    try:
        contract = store.get(cid, ver)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    datasets = [r for r in load_records() if r.contract_id == cid and r.contract_version == ver]
    field_quality = _field_quality_sections(contract)
    dataset_quality = _dataset_quality_sections(contract)
    change_log = _contract_change_log(contract)
    server_info = _server_details(contract)
    dataset_id = server_info.get("dataset_id") if server_info else contract.id or cid
    dataset_path_hint = server_info.get("path") if server_info else None
    version_records = _dq_version_records(
        dataset_id or cid,
        contract=contract,
        dataset_path=dataset_path_hint,
        dataset_records=datasets,
    )
    version_list = [entry["version"] for entry in version_records]
    status_map = {
        entry["version"]: {
            "status": entry["status"],
            "label": entry["status_label"],
            "badge": entry["badge"],
        }
        for entry in version_records
    }
    default_index = len(version_list) - 1 if version_list else None
    context = {
        "request": request,
        "contract": contract_to_dict(contract),
        "datasets": datasets,
        "expectations": expectations_from_contract(contract),
        "field_quality": field_quality,
        "dataset_quality": dataset_quality,
        "change_log": change_log,
        "status_badges": _STATUS_BADGES,
        "server_info": server_info,
        "compatibility_versions": version_records,
        "preview_versions": version_list,
        "preview_status_map": status_map,
        "preview_default_index": default_index,
        "preview_dataset_id": dataset_id,
    }
    return templates.TemplateResponse("contract_detail.html", context)


def _next_version(ver: str) -> str:
    v = Version(ver)
    return f"{v.major}.{v.minor}.{v.micro + 1}"


@app.get("/contracts/{cid}/{ver}/edit", response_class=HTMLResponse)
async def edit_contract_form(request: Request, cid: str, ver: str) -> HTMLResponse:
    try:
        contract = store.get(cid, ver)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    new_ver = _next_version(ver)
    server = (contract.servers or [None])[0]
    path = getattr(server, "path", "") if server else ""
    props = []
    if contract.schema:
        props = contract.schema[0].properties or []
    columns = "\n".join(f"{p.name}:{p.physicalType}" for p in props)
    context = {
        "request": request,
        "editing": True,
        "contract_id": contract.id,
        "contract_version": new_ver,
        "name": contract.name,
        "description": getattr(contract.description, "usage", ""),
        "dataset_path": path,
        "columns": columns,
        "original_version": ver,
    }
    return templates.TemplateResponse("new_contract.html", context)


@app.post("/contracts/{cid}/{ver}/edit", response_class=HTMLResponse)
async def save_contract_edits(
    request: Request,
    cid: str,
    ver: str,
    contract_id: str = Form(...),
    contract_version: str = Form(...),
    name: str = Form(...),
    description: str = Form(""),
    columns: str = Form(""),
    dataset_path: str = Form(""),
) -> HTMLResponse:
    path = Path(dataset_path)
    try:
        props = []
        for line in columns.splitlines():
            line = line.strip()
            if not line:
                continue
            col_name, col_type = [p.strip() for p in line.split(":", 1)]
            props.append(SchemaProperty(name=col_name, physicalType=col_type, required=True))
        if not path.is_absolute():
            path = (Path(DATA_DIR).parent / path).resolve()
        model = OpenDataContractStandard(
            version=contract_version,
            kind="DataContract",
            apiVersion="3.0.2",
            id=contract_id,
            name=name,
            description=Description(usage=description),
            schema=[SchemaObject(name=name, properties=props)],
            servers=[Server(server="local", type="filesystem", path=str(path))],
        )
        store.put(model)
        return RedirectResponse(url=f"/contracts/{contract_id}/{contract_version}", status_code=303)
    except ValidationError as ve:
        error = str(ve)
    except Exception as exc:  # pragma: no cover - display any other error
        error = str(exc)
    context = {
        "request": request,
        "editing": True,
        "error": error,
        "contract_id": contract_id,
        "contract_version": contract_version,
        "name": name,
        "description": description,
        "columns": columns,
        "dataset_path": str(path),
        "original_version": ver,
    }
    return templates.TemplateResponse("new_contract.html", context)


@app.post("/contracts/{cid}/{ver}/validate")
async def html_validate_contract(cid: str, ver: str) -> HTMLResponse:
    return RedirectResponse(url=f"/contracts/{cid}/{ver}", status_code=303)


@app.get("/contracts/new", response_class=HTMLResponse)
async def new_contract_form(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("new_contract.html", {"request": request})


@app.post("/contracts/new", response_class=HTMLResponse)
async def create_contract(
    request: Request,
    contract_id: str = Form(...),
    contract_version: str = Form(...),
    name: str = Form(...),
    description: str = Form(""),
    columns: str = Form(""),
    dataset_path: str = Form(""),
) -> HTMLResponse:
    path = Path(dataset_path)
    try:
        props = []
        for line in columns.splitlines():
            line = line.strip()
            if not line:
                continue
            col_name, col_type = [p.strip() for p in line.split(":", 1)]
            props.append(SchemaProperty(name=col_name, physicalType=col_type, required=True))
        if not path.is_absolute():
            path = (Path(DATA_DIR).parent / path).resolve()
        model = OpenDataContractStandard(
            version=contract_version,
            kind="DataContract",
            apiVersion="3.0.2",
            id=contract_id,
            name=name,
            description=Description(usage=description),
            schema=[SchemaObject(name=name, properties=props)],
            servers=[Server(server="local", type="filesystem", path=str(path))],
        )
        store.put(model)
        return RedirectResponse(url="/contracts", status_code=303)
    except ValidationError as ve:
        error = str(ve)
    except Exception as exc:  # pragma: no cover - display any other error
        error = str(exc)
    context = {
        "request": request,
        "error": error,
        "contract_id": contract_id,
        "contract_version": contract_version,
        "name": name,
        "description": description,
        "columns": columns,
        "dataset_path": str(path),
    }
    return templates.TemplateResponse("new_contract.html", context)


@app.get("/datasets", response_class=HTMLResponse)
async def list_datasets(request: Request) -> HTMLResponse:
    records = load_records()
    recs = []
    for r in records:
        recs.append(r.__dict__.copy())
    flash_token = request.query_params.get("flash")
    flash_message: str | None = None
    flash_error: str | None = None
    if flash_token:
        flash_message, flash_error = pop_flash(flash_token)
    else:
        flash_message = request.query_params.get("msg")
        flash_error = request.query_params.get("error")
    context = {
        "request": request,
        "records": recs,
        "scenarios": SCENARIOS,
        "message": flash_message,
        "error": flash_error,
    }
    return templates.TemplateResponse("datasets.html", context)


@app.get("/datasets/{dataset_name}", response_class=HTMLResponse)
async def dataset_versions(request: Request, dataset_name: str) -> HTMLResponse:
    records = [r.__dict__.copy() for r in load_records() if r.dataset_name == dataset_name]
    context = {"request": request, "dataset_name": dataset_name, "records": records}
    return templates.TemplateResponse("dataset_versions.html", context)


def _dataset_path(contract: OpenDataContractStandard | None, dataset_name: str, dataset_version: str) -> Path:
    server = (contract.servers or [None])[0] if contract else None
    data_root = Path(DATA_DIR).parent
    base = Path(getattr(server, "path", "")) if server else data_root
    if base.suffix:
        base = base.parent
    if not base.is_absolute():
        base = data_root / base
    if base.name == dataset_name:
        return base / dataset_version
    return base / dataset_name / dataset_version


def _dataset_preview(contract: OpenDataContractStandard | None, dataset_name: str, dataset_version: str) -> str:
    ds_path = _dataset_path(contract, dataset_name, dataset_version)
    server = (contract.servers or [None])[0] if contract else None
    fmt = getattr(server, "format", None)
    try:
        if fmt == "parquet":
            from pyspark.sql import SparkSession  # type: ignore
            spark = SparkSession.builder.master("local[1]").appName("preview").getOrCreate()
            df = spark.read.parquet(str(ds_path))
            return "\n".join(str(r.asDict()) for r in df.limit(10).collect())[:1000]
        if fmt == "json":
            target = ds_path if ds_path.is_file() else next(ds_path.glob("*.json"), None)
            if target:
                return target.read_text()[:1000]
        if ds_path.is_file():
            return ds_path.read_text()[:1000]
        if ds_path.is_dir():
            target = next((p for p in ds_path.iterdir() if p.is_file()), None)
            if target:
                return target.read_text()[:1000]
    except Exception:
        return ""
    return ""


@app.get("/datasets/{dataset_name}/{dataset_version}", response_class=HTMLResponse)
async def dataset_detail(request: Request, dataset_name: str, dataset_version: str) -> HTMLResponse:
    for r in load_records():
        if r.dataset_name == dataset_name and r.dataset_version == dataset_version:
            contract_obj: OpenDataContractStandard | None = None
            if r.contract_id and r.contract_version:
                try:
                    contract_obj = store.get(r.contract_id, r.contract_version)
                except FileNotFoundError:
                    contract_obj = None
            preview = _dataset_preview(contract_obj, dataset_name, dataset_version)
            context = {
                "request": request,
                "record": r,
                "contract": contract_to_dict(contract_obj) if contract_obj else None,
                "data_preview": preview,
            }
            return templates.TemplateResponse("dataset_detail.html", context)
    raise HTTPException(status_code=404, detail="Dataset not found")


@app.post("/pipeline/run", response_class=HTMLResponse)
async def run_pipeline_endpoint(scenario: str = Form(...)) -> HTMLResponse:
    from .pipeline import run_pipeline

    cfg = SCENARIOS.get(scenario)
    if not cfg:
        params = urlencode({"error": f"Unknown scenario: {scenario}"})
        return RedirectResponse(url=f"/datasets?{params}", status_code=303)
    p = cfg["params"]
    for dataset, version in cfg.get("activate_versions", {}).items():
        try:
            set_active_version(dataset, version)
        except FileNotFoundError:
            continue
    try:
        dataset_name, new_version = run_pipeline(
            p.get("contract_id"),
            p.get("contract_version"),
            p.get("dataset_name"),
            p.get("dataset_version"),
            p.get("run_type", "infer"),
            p.get("collect_examples", False),
            p.get("examples_limit", 5),
            p.get("violation_strategy"),
            p.get("inputs"),
            p.get("output_adjustment"),
        )
        label = dataset_name or p.get("dataset_name") or p.get("contract_id") or "dataset"
        token = queue_flash(message=f"Run succeeded: {label} {new_version}")
        params = urlencode({"flash": token})
    except Exception as exc:  # pragma: no cover - surface pipeline errors
        token = queue_flash(error=str(exc))
        params = urlencode({"flash": token})
    return RedirectResponse(url=f"/datasets?{params}", status_code=303)


def run() -> None:  # pragma: no cover - convenience runner
    """Run the demo app with uvicorn."""
    import uvicorn

    uvicorn.run("dc43.demo_app.server:app", host="0.0.0.0", port=8000)
