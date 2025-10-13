from __future__ import annotations

"""FastAPI demo application for dc43.

This application provides a small Bootstrap-powered UI to manage data
contracts and run an example Spark pipeline that records dataset versions
with their validation status. Contracts are stored on the local
filesystem using :class:`~dc43_service_backends.contracts.backend.stores.FSContractStore` and dataset
metadata lives in a JSON file.

Run the UI directly with::

    uvicorn dc43_demo_app.server:app --reload

or start the full demo (UI + HTTP backend) with::

    dc43-demo

Optional dependencies needed: ``fastapi``, ``uvicorn``, ``jinja2`` and
``pyspark``.
"""

import asyncio
import io
import contextlib
import logging
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Any, Tuple, Mapping, Optional, Iterable, Callable, Set
from uuid import uuid4
from threading import Lock
import threading
import json
import os
import re
import shutil
import tempfile
import textwrap
from datetime import datetime
from collections import Counter
import zipfile

import httpx
from fastapi import APIRouter, FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.encoders import jsonable_encoder
from httpx import ASGITransport
from fastapi.concurrency import run_in_threadpool

from dc43_service_backends.contracts.backend.stores import FSContractStore
from dc43_service_backends.web import build_local_app
from dc43_service_backends.config import (
    AuthConfig as BackendAuthConfig,
    ContractStoreConfig as BackendContractStoreConfig,
    DataProductStoreConfig as BackendDataProductStoreConfig,
    GovernanceConfig as BackendGovernanceConfig,
    ServiceBackendsConfig,
    UnityCatalogConfig as BackendUnityCatalogConfig,
    dumps as dump_service_backends_config,
)
from dc43_service_clients._http_sync import close_client
from dc43_service_clients.contracts.client.remote import RemoteContractServiceClient
from dc43_service_clients.data_quality.client.remote import RemoteDataQualityServiceClient
from dc43_service_clients.governance.client.remote import RemoteGovernanceServiceClient
from dc43_service_clients.odps import OpenDataProductStandard
from ._odcs import custom_properties_dict, normalise_custom_properties
from ._versioning import SemVer
from .config import (
    BackendConfig,
    BackendProcessConfig,
    ContractsAppConfig,
    WorkspaceConfig,
    dumps as dump_contracts_app_config,
    load_config,
)
from .workspace import ContractsAppWorkspace, workspace_from_env
from open_data_contract_standard.model import (
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
from pydantic import ValidationError
from packaging.version import Version, InvalidVersion

# Optional pyspark-based helpers. Keep imports lazy-friendly so the demo UI can
# still load when pyspark is not installed (for example when running fast unit
# tests).
try:  # pragma: no cover - exercised indirectly when pyspark is available
    from dc43_integrations.spark.io import ContractVersionLocator, read_with_contract
except ModuleNotFoundError as exc:  # pragma: no cover - safety net for CI
    if exc.name != "pyspark":
        raise
    ContractVersionLocator = None  # type: ignore[assignment]
    read_with_contract = None  # type: ignore[assignment]

_SPARK_SESSION: Any | None = None
logger = logging.getLogger(__name__)


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
REPO_ROOT = BASE_DIR.parents[4]
TERRAFORM_TEMPLATE_ROOT = REPO_ROOT / "deploy" / "terraform"

_CONFIG_LOCK = Lock()
_ACTIVE_CONFIG: ContractsAppConfig | None = None
_WORKSPACE_LOCK = Lock()
_WORKSPACE: ContractsAppWorkspace | None = None
WORK_DIR: Path
CONTRACT_DIR: Path
DATA_DIR: Path
RECORDS_DIR: Path
DATASETS_FILE: Path
DATA_PRODUCTS_FILE: Path
DQ_STATUS_DIR: Path
store: FSContractStore


def configure_workspace(workspace: ContractsAppWorkspace) -> None:
    """Set the active filesystem layout for the application."""

    global _WORKSPACE, WORK_DIR, CONTRACT_DIR, DATA_DIR, RECORDS_DIR, DATASETS_FILE, DATA_PRODUCTS_FILE, DQ_STATUS_DIR, store

    workspace.ensure()
    WORK_DIR = workspace.root
    CONTRACT_DIR = workspace.contracts_dir
    DATA_DIR = workspace.data_dir
    RECORDS_DIR = workspace.records_dir
    DATASETS_FILE = workspace.datasets_file
    DATA_PRODUCTS_FILE = workspace.data_products_file
    DQ_STATUS_DIR = workspace.dq_status_dir
    _WORKSPACE = workspace
    store = FSContractStore(str(CONTRACT_DIR))
    try:
        os.environ.setdefault("DC43_CONTRACTS_APP_WORK_DIR", str(WORK_DIR))
    except Exception:  # pragma: no cover - defensive
        pass


def current_workspace() -> ContractsAppWorkspace:
    """Return the configured workspace initialising defaults when needed."""

    _current_config()
    global _WORKSPACE
    if _WORKSPACE is None:
        with _WORKSPACE_LOCK:
            if _WORKSPACE is None:
                active = _current_config()
                default_root = (
                    str(active.workspace.root) if active.workspace.root else None
                )
                workspace, _ = workspace_from_env(default_root=default_root)
                configure_workspace(workspace)
    assert _WORKSPACE is not None
    return _WORKSPACE


def _set_active_config(config: ContractsAppConfig) -> ContractsAppConfig:
    with _CONFIG_LOCK:
        global _ACTIVE_CONFIG
        _ACTIVE_CONFIG = config
    return config


def _current_config() -> ContractsAppConfig:
    with _CONFIG_LOCK:
        global _ACTIVE_CONFIG
        if _ACTIVE_CONFIG is None:
            _ACTIVE_CONFIG = load_config()
        return _ACTIVE_CONFIG




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

_backend_app: FastAPI | None = None
_backend_transport: ASGITransport | None = None
_backend_client: httpx.AsyncClient | None = None
_backend_base_url: str = "http://dc43-services"
_backend_mode: str = "embedded"
_backend_token: str = ""
_THREAD_CLIENTS = threading.local()
contract_service: RemoteContractServiceClient
dq_service: RemoteDataQualityServiceClient
governance_service: RemoteGovernanceServiceClient


def _close_backend_client() -> None:
    """Best-effort close of the shared HTTP client."""

    global _backend_client
    client = _backend_client
    if client is None:
        return
    try:
        asyncio.run(client.aclose())
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(client.aclose())
        finally:
            loop.close()
    _backend_client = None


def _clear_thread_clients() -> None:
    """Dispose of thread-local HTTP clients for the current thread."""

    bundle = getattr(_THREAD_CLIENTS, "bundle", None)
    if bundle is None:
        return
    try:
        close_client(bundle["http_client"])
    except Exception:  # pragma: no cover - defensive guard
        logger.exception("Failed to close thread-local backend client")
    finally:
        _THREAD_CLIENTS.bundle = None


def _thread_service_clients() -> tuple[
    RemoteContractServiceClient,
    RemoteDataQualityServiceClient,
    RemoteGovernanceServiceClient,
]:
    """Return backend service clients scoped to the current thread."""

    bundle = getattr(_THREAD_CLIENTS, "bundle", None)
    if bundle is not None and bundle.get("token") == _backend_token:
        return (
            bundle["contract"],
            bundle["dq"],
            bundle["governance"],
        )

    if bundle is not None:
        try:
            close_client(bundle["http_client"])
        except Exception:  # pragma: no cover - defensive guard
            logger.exception("Failed to recycle thread-local backend client")

    if _backend_mode == "remote":
        http_client: httpx.Client | httpx.AsyncClient = httpx.Client(
            base_url=_backend_base_url or None,
        )
    else:
        assert _backend_app is not None
        http_client = httpx.AsyncClient(
            transport=ASGITransport(app=_backend_app),
            base_url=_backend_base_url or None,
        )

    contract = RemoteContractServiceClient(
        base_url=_backend_base_url,
        client=http_client,
    )
    dq = RemoteDataQualityServiceClient(
        base_url=_backend_base_url,
        client=http_client,
    )
    governance = RemoteGovernanceServiceClient(
        base_url=_backend_base_url,
        client=http_client,
    )

    _THREAD_CLIENTS.bundle = {
        "token": _backend_token,
        "http_client": http_client,
        "contract": contract,
        "dq": dq,
        "governance": governance,
    }
    return contract, dq, governance


def _initialise_backend(*, base_url: str | None = None) -> None:
    """Configure service clients against an in-process or remote backend."""

    global _backend_app, _backend_transport, _backend_client
    global contract_service, dq_service, governance_service
    global _backend_base_url, _backend_mode, _backend_token

    _close_backend_client()
    _clear_thread_clients()

    client_base_url = (base_url.rstrip("/") if base_url else "http://dc43-services")

    if base_url:
        _backend_app = None
        _backend_transport = None
        _backend_client = httpx.AsyncClient(base_url=client_base_url)
        _backend_mode = "remote"
    else:
        _backend_app = build_local_app(store)
        _backend_transport = ASGITransport(app=_backend_app)
        _backend_client = httpx.AsyncClient(
            transport=_backend_transport,
            base_url=client_base_url,
        )
        _backend_mode = "embedded"

    _backend_base_url = client_base_url
    _backend_token = uuid4().hex

    contract_service = RemoteContractServiceClient(
        base_url=client_base_url,
        client=_backend_client,
    )
    dq_service = RemoteDataQualityServiceClient(
        base_url=client_base_url,
        client=_backend_client,
    )
    governance_service = RemoteGovernanceServiceClient(
        base_url=client_base_url,
        client=_backend_client,
    )


def configure_backend(
    base_url: str | None = None, *, config: BackendConfig | None = None
) -> None:
    """Initialise service clients against the configured backend."""

    if base_url is not None:
        _initialise_backend(base_url=base_url or None)
        return

    env_url = os.getenv("DC43_CONTRACTS_APP_BACKEND_URL") or os.getenv(
        "DC43_DEMO_BACKEND_URL"
    )
    if env_url:
        _initialise_backend(base_url=env_url)
        return

    config = config or _current_config().backend
    mode = (config.mode or "embedded").lower()
    if mode == "remote":
        target_url = config.base_url or config.process.url()
        _initialise_backend(base_url=target_url)
    else:
        _initialise_backend(base_url=None)


def _setup_state_path() -> Path:
    """Return the path that stores onboarding progress information."""

    workspace = current_workspace()
    return workspace.root / "setup_state.json"


def _default_setup_state() -> Dict[str, Any]:
    """Return the default onboarding wizard payload."""

    return {
        "current_step": 1,
        "selected_options": {},
        "configuration": {},
        "completed": False,
    }


def load_setup_state() -> Dict[str, Any]:
    """Read the persisted onboarding state if available."""

    path = _setup_state_path()
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        payload = {}
    state = _default_setup_state()
    if isinstance(payload, Mapping):
        state.update(
            {
                "current_step": int(payload.get("current_step") or 1),
                "selected_options": dict(payload.get("selected_options") or {}),
                "configuration": dict(payload.get("configuration") or {}),
                "completed": bool(payload.get("completed")),
            }
        )
        completed_at = payload.get("completed_at") if isinstance(payload, Mapping) else None
        if isinstance(completed_at, str):
            state["completed_at"] = completed_at
    return state


def save_setup_state(state: Mapping[str, Any]) -> None:
    """Persist onboarding progress to the workspace."""

    path = _setup_state_path()
    serialisable = dict(state)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Optional[str] = None
    fd = -1
    try:
        fd, temp_path = tempfile.mkstemp(dir=str(path.parent), prefix=path.name, suffix=".tmp")
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(serialisable, handle, indent=2, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        fd = -1  # file descriptor ownership moved to context manager
        os.replace(temp_path, path)
        temp_path = None
    finally:
        if fd != -1:
            os.close(fd)
        if temp_path:
            with contextlib.suppress(FileNotFoundError):
                os.remove(temp_path)


def reset_setup_state() -> Dict[str, Any]:
    """Reset onboarding progress and persist the default payload."""

    state = _default_setup_state()
    save_setup_state(state)
    return state


def is_setup_complete() -> bool:
    """Return ``True`` when the onboarding flow has been marked as complete."""

    state = load_setup_state()
    return bool(state.get("completed"))


def configure_from_config(config: ContractsAppConfig | None = None) -> ContractsAppConfig:
    """Apply ``config`` to initialise workspace and backend defaults."""

    config = config or load_config()
    workspace_root = config.workspace.root
    default_root = str(workspace_root) if workspace_root else None
    workspace, _ = workspace_from_env(default_root=default_root)
    configure_workspace(workspace)
    configure_backend(config=config.backend)
    return _set_active_config(config)


# Ensure module-level paths and backend clients are ready for import-time users.
configure_from_config()


def _wait_for_backend(base_url: str, timeout: float = 30.0) -> None:
    """Block until the backend responds or ``timeout`` elapses."""

    deadline = time.monotonic() + timeout
    probe_url = f"{base_url.rstrip('/')}/openapi.json"
    with httpx.Client(timeout=2.0) as client:
        while True:
            try:
                response = client.get(probe_url)
                if response.status_code < 500:
                    return
            except httpx.HTTPError:
                pass
            if time.monotonic() >= deadline:
                raise RuntimeError(f"Backend at {base_url} failed to start within {timeout}s")
            time.sleep(0.2)


async def _expectation_predicates(contract: OpenDataContractStandard) -> Dict[str, str]:
    plan = await asyncio.to_thread(dq_service.describe_expectations, contract=contract)
    mapping: Dict[str, str] = {}
    for item in plan:
        key = item.get("key") if isinstance(item, Mapping) else None
        predicate = item.get("predicate") if isinstance(item, Mapping) else None
        if isinstance(key, str) and isinstance(predicate, str):
            mapping[key] = predicate
    return mapping

router = APIRouter()
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


SETUP_MODULES: Dict[str, Dict[str, Any]] = {
    "contracts_backend": {
        "title": "Contracts storage backend",
        "summary": "Choose where contract definitions and their history are persisted.",
        "options": {
            "filesystem": {
                "label": "Local filesystem",
                "description": "Store JSON/YAML contract assets on a mounted volume that the UI can read and write.",
                "installation": [
                    "Create a persistent volume (for example `./volumes/contracts`) and mount it under the working directory.",
                    "Ensure backup or version control for the mounted folder so contract changes are traceable.",
                ],
                "configuration_notes": [
                    "Set `DC43_CONTRACTS_APP_BACKEND_MODE=embedded` so the UI boots the bundled backend.",
                    "Expose the directories below through `DC43_CONTRACTS_APP_WORK_DIR` when running inside Docker.",
                ],
                "fields": [
                    {
                        "name": "work_dir",
                        "label": "Workspace root",
                        "placeholder": "/workspace",
                        "help": "Root directory that will be bound to `DC43_CONTRACTS_APP_WORK_DIR`.",
                        "default_factory": lambda workspace: str(workspace.root),
                    },
                    {
                        "name": "contracts_dir",
                        "label": "Contracts directory",
                        "placeholder": "/workspace/contracts",
                        "help": "Path where contract files will be created (must be inside the workspace).",
                        "default_factory": lambda workspace: str(workspace.contracts_dir),
                    },
                ],
                "diagram": {
                    "nodes": [
                        {
                            "id": "contracts_backend_filesystem",
                            "label": "Contracts workspace (filesystem)",
                            "class": "external",
                            "edges": [
                                {
                                    "from": "contracts_backend",
                                    "to": "contracts_backend_filesystem",
                                    "label": "Stores contracts",
                                }
                            ],
                        }
                    ]
                },
            },
            "collibra": {
                "label": "Collibra governance backend",
                "description": "Persist contracts as assets inside a Collibra Data Governance domain.",
                "installation": [
                    "Provision a Collibra Cloud site with the Data Quality & Observability package enabled.",
                    "Create a service account with write access to the domain that will hold contract assets.",
                ],
                "configuration_notes": [
                    "Export credentials via `COLLIBRA_CLIENT_ID` and `COLLIBRA_CLIENT_SECRET` for the automation user.",
                    "Set `DC43_CONTRACTS_BACKEND=collibra` (or matching Helm/Docker overrides) to activate this connector.",
                ],
                "fields": [
                    {
                        "name": "base_url",
                        "label": "Site URL",
                        "placeholder": "https://acme.collibra.com",
                        "help": "Fully qualified URL for the Collibra environment.",
                    },
                    {
                        "name": "client_id",
                        "label": "Client ID",
                        "placeholder": "collibra-service-client",
                        "help": "OAuth client identifier for the integration user.",
                    },
                    {
                        "name": "client_secret",
                        "label": "Client secret",
                        "placeholder": "••••••",
                        "help": "OAuth client secret stored in your secrets manager.",
                    },
                    {
                        "name": "domain_id",
                        "label": "Target domain",
                        "placeholder": "DATA_CONTRACTS",
                        "help": "Collibra domain that will contain the contract assets.",
                    },
                ],
                "diagram": {
                    "nodes": [
                        {
                            "id": "contracts_backend_collibra",
                            "label": "Collibra contracts domain",
                            "class": "external",
                            "edges": [
                                {
                                    "from": "contracts_backend",
                                    "to": "contracts_backend_collibra",
                                    "label": "Syncs assets",
                                }
                            ],
                        }
                    ]
                },
            },
            "sql": {
                "label": "SQL database",
                "description": "Use a relational database for contract metadata with transactional guarantees.",
                "installation": [
                    "Provision the database (PostgreSQL, SQL Server, or compatible) and create a dedicated schema.",
                    "Apply migration scripts from `dc43-service-backends` to prepare the contracts tables.",
                ],
                "configuration_notes": [
                    "Populate `DC43_CONTRACTS_SQL_URL` with an application role and SSL enforced connection string.",
                    "Set `DC43_CONTRACTS_BACKEND=sql` so the services load the SQLAlchemy implementation.",
                ],
                "fields": [
                    {
                        "name": "connection_uri",
                        "label": "Connection URI",
                        "placeholder": "postgresql+psycopg://user:pass@host:5432/contracts",
                        "help": "SQLAlchemy compatible URI including credentials and database name.",
                    },
                    {
                        "name": "schema",
                        "label": "Schema",
                        "placeholder": "contracts",
                        "help": "Database schema where contract tables will be created.",
                    },
                    {
                        "name": "ssl_mode",
                        "label": "SSL mode",
                        "placeholder": "require",
                        "help": "SSL requirement flag passed to the driver (e.g. `require`, `verify-full`).",
                        "optional": True,
                    },
                ],
                "diagram": {
                    "nodes": [
                        {
                            "id": "contracts_backend_sql",
                            "label": "Contracts SQL database",
                            "class": "external",
                            "edges": [
                                {
                                    "from": "contracts_backend",
                                    "to": "contracts_backend_sql",
                                    "label": "Persists metadata",
                                }
                            ],
                        }
                    ]
                },
            },
            "delta_lake": {
                "label": "Delta Lake (SQL-on-lake)",
                "description": "Back contracts with Delta tables so history is queryable via Spark or SQL endpoints.",
                "installation": [
                    "Create a Delta table (Unity Catalog, Hive metastore, or lakehouse) dedicated to contract revisions.",
                    "Grant the governance and product services read/write privileges on the catalog/schema pair.",
                ],
                "configuration_notes": [
                    "Set `DC43_CONTRACTS_BACKEND=delta` to enable the Delta implementation.",
                    "Point the storage location below to a managed table path or external location with ACID support.",
                ],
                "fields": [
                    {
                        "name": "storage_path",
                        "label": "Delta storage location",
                        "placeholder": "s3://contracts-lake/contracts",
                        "help": "URI or catalog path where the Delta table is stored.",
                    },
                    {
                        "name": "catalog",
                        "label": "Catalog (optional)",
                        "placeholder": "main",
                        "help": "Unity Catalog or metastore catalog name if using managed tables.",
                        "optional": True,
                    },
                    {
                        "name": "schema",
                        "label": "Schema",
                        "placeholder": "contracts",
                        "help": "Schema or database that groups the contract Delta tables.",
                    },
                ],
                "diagram": {
                    "nodes": [
                        {
                            "id": "contracts_backend_delta",
                            "label": "Contracts Delta Lake",
                            "class": "external",
                            "edges": [
                                {
                                    "from": "contracts_backend",
                                    "to": "contracts_backend_delta",
                                    "label": "ACID table",
                                }
                            ],
                        }
                    ]
                },
            },
        },
    },
    "products_backend": {
        "title": "Data products backend",
        "summary": "Decide where published product manifests are stored alongside operational metadata.",
        "options": {
            "filesystem": {
                "label": "Local filesystem",
                "description": "Reuse the mounted workspace to materialise product descriptors next to contracts.",
                "installation": [
                    "Mount a persistent directory that is shared between the producer pipelines and the UI.",
                    "Seed the folder with existing product definitions if you are migrating from another platform.",
                ],
                "configuration_notes": [
                    "Set `DC43_PRODUCTS_BACKEND=filesystem` so the services co-locate products with contracts.",
                    "Keep the directory consistent with the contracts workspace to ease local development.",
                ],
                "fields": [
                    {
                        "name": "products_dir",
                        "label": "Products directory",
                        "placeholder": "/workspace/products",
                        "help": "Folder that stores published product descriptors (JSON/YAML).",
                        "default_factory": lambda workspace: str(workspace.root / "products"),
                    },
                ],
                "diagram": {
                    "nodes": [
                        {
                            "id": "products_backend_filesystem",
                            "label": "Products workspace (filesystem)",
                            "class": "external",
                            "edges": [
                                {
                                    "from": "products_backend",
                                    "to": "products_backend_filesystem",
                                    "label": "Stores products",
                                }
                            ],
                        }
                    ]
                },
            },
            "collibra": {
                "label": "Collibra domain",
                "description": "Manage product lifecycles directly in Collibra next to ownership metadata.",
                "installation": [
                    "Reuse the Collibra site used for contracts or provision a dedicated community for products.",
                    "Grant the automation account stewardship permissions for the target domain.",
                ],
                "configuration_notes": [
                    "Share the same credentials as the contracts backend or supply overrides below.",
                    "Export `DC43_PRODUCTS_BACKEND=collibra` when deploying the governance workflows.",
                ],
                "fields": [
                    {
                        "name": "base_url",
                        "label": "Site URL",
                        "placeholder": "https://acme.collibra.com",
                        "help": "URL of the Collibra instance hosting product assets.",
                    },
                    {
                        "name": "client_id",
                        "label": "Client ID",
                        "placeholder": "collibra-products-client",
                        "help": "OAuth client for the product synchronisation job.",
                    },
                    {
                        "name": "client_secret",
                        "label": "Client secret",
                        "placeholder": "••••••",
                        "help": "Store this secret securely – it is required for API authentication.",
                    },
                    {
                        "name": "domain_id",
                        "label": "Products domain",
                        "placeholder": "DATA_PRODUCTS",
                        "help": "Collibra domain identifier where product assets are stored.",
                    },
                ],
                "diagram": {
                    "nodes": [
                        {
                            "id": "products_backend_collibra",
                            "label": "Collibra products domain",
                            "class": "external",
                            "edges": [
                                {
                                    "from": "products_backend",
                                    "to": "products_backend_collibra",
                                    "label": "Syncs assets",
                                }
                            ],
                        }
                    ]
                },
            },
            "sql": {
                "label": "SQL database",
                "description": "Persist product manifests and release checkpoints in a relational database.",
                "installation": [
                    "Create the schema that will store product metadata and grant DDL/DML to the automation role.",
                    "Execute the database migrations from the product service package if available for your engine.",
                ],
                "configuration_notes": [
                    "Populate `DC43_PRODUCTS_SQL_URL` (or Helm secret) with an SSL-enabled connection string.",
                    "Enable the connector with `DC43_PRODUCTS_BACKEND=sql` to align with the contracts backend.",
                ],
                "fields": [
                    {
                        "name": "connection_uri",
                        "label": "Connection URI",
                        "placeholder": "postgresql+psycopg://user:pass@host:5432/products",
                        "help": "SQLAlchemy compatible URI for the product metadata database.",
                    },
                    {
                        "name": "schema",
                        "label": "Schema",
                        "placeholder": "products",
                        "help": "Schema that contains the product tables and views.",
                    },
                ],
                "diagram": {
                    "nodes": [
                        {
                            "id": "products_backend_sql",
                            "label": "Products SQL database",
                            "class": "external",
                            "edges": [
                                {
                                    "from": "products_backend",
                                    "to": "products_backend_sql",
                                    "label": "Persists metadata",
                                }
                            ],
                        }
                    ]
                },
            },
            "delta_lake": {
                "label": "Delta Lake",
                "description": "Use Delta tables (local lakehouse or Unity Catalog) to track product releases.",
                "installation": [
                    "Provision an external location or managed catalog for the product Delta tables.",
                    "Grant the publishing pipelines and governance hook read/write access to the location.",
                ],
                "configuration_notes": [
                    "Set `DC43_PRODUCTS_BACKEND=delta` to keep the configuration consistent with contracts.",
                    "Use the same catalog/schema pair as the contracts backend when sharing infrastructure.",
                ],
                "fields": [
                    {
                        "name": "storage_path",
                        "label": "Delta storage location",
                        "placeholder": "s3://contracts-lake/products",
                        "help": "URI or catalog path where the product Delta table lives.",
                    },
                    {
                        "name": "catalog",
                        "label": "Catalog",
                        "placeholder": "main",
                        "help": "Unity Catalog or Hive metastore catalog containing the product Delta tables.",
                    },
                    {
                        "name": "schema",
                        "label": "Schema",
                        "placeholder": "products",
                        "help": "Schema or database that holds the product Delta tables.",
                    },
                ],
                "diagram": {
                    "nodes": [
                        {
                            "id": "products_backend_delta",
                            "label": "Products Delta Lake",
                            "class": "external",
                            "edges": [
                                {
                                    "from": "products_backend",
                                    "to": "products_backend_delta",
                                    "label": "ACID table",
                                }
                            ],
                        }
                    ]
                },
            },
        },
    },
    "data_quality": {
        "title": "Data quality service",
        "summary": "Select the engine that evaluates expectations against your datasets.",
        "options": {
            "embedded_engine": {
                "label": "Embedded local engine",
                "description": "Run the bundled expectation runner alongside the contracts UI (default).",
                "installation": [
                    "Mount a directory for expectation suites and validation results inside the workspace.",
                    "Install any additional libraries required by your custom expectations (Great Expectations, soda-core, …).",
                ],
                "configuration_notes": [
                    "Set `DC43_DQ_ENGINE=local` to confirm the UI should manage execution locally.",
                    "Point the directories below to locations writable by the container user.",
                ],
                "fields": [
                    {
                        "name": "expectations_path",
                        "label": "Expectations directory",
                        "placeholder": "/workspace/expectations",
                        "help": "Where `.yml`/`.json` suites defining rules are stored.",
                        "default_factory": lambda workspace: str(workspace.records_dir / "expectations"),
                    },
                    {
                        "name": "results_path",
                        "label": "Results directory",
                        "placeholder": "/workspace/expectations/results",
                        "help": "Optional folder where validation run outputs are persisted.",
                        "optional": True,
                    },
                ],
            }
        },
    },
    "governance_service": {
        "title": "Governance interface",
        "summary": "Decide whether orchestration runs in-process, via the bundled web service, or through a remote API.",
        "options": {
            "embedded_monolith": {
                "label": "Embedded web service (server.py)",
                "description": "Keep the all-in-one FastAPI application that exposes UI and service endpoints together.",
                "installation": [
                    "Expose the container port (default 8000) through Docker Compose or your orchestrator.",
                    "Mount the same workspace volume used by the contracts and product backends.",
                ],
                "configuration_notes": [
                    "No additional configuration is required beyond the workspace paths defined above.",
                    "Use `uvicorn dc43_contracts_app.server:app --reload` for local development mode.",
                ],
                "fields": [],
            },
            "direct_runtime": {
                "label": "Direct Python orchestration",
                "description": "Call governance, contracts, and product services in-process without exposing HTTP endpoints.",
                "installation": [
                    "Install the dc43 service backends (`pip install dc43-service-backends[contracts,products,dq]`) inside your orchestrator runtime.",
                    "Ensure the runtime has filesystem or network access to the storage targets configured above.",
                ],
                "configuration_notes": [
                    "Import the backend factories (for example `dc43_service_backends.governance`) directly from your pipelines or notebooks.",
                    "Reuse the same environment variables or generated configuration files from previous sections – no web server is required.",
                ],
                "fields": [],
            },
            "remote_api": {
                "label": "Remote governance API",
                "description": "Run governance orchestration as a standalone service and let the UI connect over HTTPS.",
                "installation": [
                    "Deploy the governance service package (for example `dc43-service-backends`) to your preferred platform.",
                    "Enable networking between the UI container and the remote governance endpoint.",
                ],
                "configuration_notes": [
                    "Set `DC43_GOVERNANCE_MODE=remote` (or Helm override) to disable the embedded orchestrator.",
                    "Provide the base URL and optional credentials below via environment variables or secrets.",
                ],
                "fields": [
                    {
                        "name": "base_url",
                        "label": "Governance API URL",
                        "placeholder": "https://governance.example.com",
                        "help": "HTTPS endpoint exposing the governance service.",
                    },
                    {
                        "name": "api_token",
                        "label": "API token",
                        "placeholder": "Optional bearer token",
                        "help": "Authentication token presented to the remote governance API.",
                        "optional": True,
                    },
                ],
            },
        },
    },
    "governance_deployment": {
        "title": "Governance service deployment",
        "summary": "Capture how the governance backends are hosted so the wizard can emit scripts or Terraform stubs per environment.",
        "depends_on": "governance_service",
        "visible_when": {
            "embedded_monolith": ["local_python", "local_docker"],
            "remote_api": [
                "local_python",
                "local_docker",
                "aws_terraform",
                "azure_terraform",
            ],
            "direct_runtime": ["not_required"],
        },
        "hide_when": ["direct_runtime"],
        "default_for": {
            "embedded_monolith": "local_python",
            "remote_api": "local_docker",
            "direct_runtime": "not_required",
        },
        "default_option": "local_python",
        "options": {
            "local_python": {
                "label": "Local Python process",
                "description": "Start the bundled FastAPI server directly with `uvicorn` for interactive development.",
                "installation": [
                    "Install the contracts app package into your virtual environment (for example `pip install -e .`).",
                    "Run `uvicorn dc43_contracts_app.server:app --reload --port 8000` from the workspace root.",
                ],
                "configuration_notes": [
                    "Reuse the TOML files exported by the wizard or environment variables to configure backends.",
                    "Ensure the process runs with access to the same workspace paths defined in the storage step.",
                ],
                "fields": [
                    {
                        "name": "command",
                        "label": "Launch command",
                        "placeholder": "uvicorn dc43_contracts_app.server:app --reload --port 8000",
                        "help": "Command used by developers to start the governance service locally.",
                        "optional": True,
                        "default_factory": lambda workspace: "uvicorn dc43_contracts_app.server:app --reload --port 8000",
                    },
                ],
            },
            "local_docker": {
                "label": "Local Docker or Compose",
                "description": "Keep the governance service on a laptop or developer workstation using Docker Compose and the generated helper scripts.",
                "installation": [
                    "Install Docker Desktop or an equivalent container runtime.",
                    "Use `scripts/run_local_stack.py` from the exported bundle to start the governance APIs alongside the UI.",
                ],
                "configuration_notes": [
                    "Reuse the TOML files exported by the wizard to keep local environments consistent.",
                    "Commit the generated configuration into your infrastructure repo once you are happy with the defaults.",
                ],
                "fields": [],
            },
            "aws_terraform": {
                "label": "AWS (Terraform)",
                "description": "Provision the governance and data-quality APIs on AWS Fargate using the provided Terraform module skeleton.",
                "installation": [
                    "Clone the Terraform module under `deploy/terraform/aws-service-backend` into your infrastructure repository.",
                    "Ensure the AWS provider is authenticated (for example via `aws configure` or CI/CD secrets).",
                    "Run `terraform init` and `terraform apply` after reviewing the generated `terraform.tfvars` file.",
                ],
                "configuration_notes": [
                    "The wizard will emit a `terraform.tfvars` stub aligned with your selections – fill in any missing secrets before applying.",
                    "Set `contract_store_mode` to `sql` when the contracts backend uses a relational database, otherwise keep `filesystem` for the default volume-backed store.",
                ],
                "fields": [
                    {
                        "name": "aws_region",
                        "label": "AWS region",
                        "placeholder": "us-east-1",
                        "help": "Region where the ECS service will be created.",
                    },
                    {
                        "name": "cluster_name",
                        "label": "ECS cluster name",
                        "placeholder": "dc43-governance",
                        "help": "Existing ECS/Fargate cluster that will host the tasks.",
                    },
                    {
                        "name": "ecr_image_uri",
                        "label": "ECR image URI",
                        "placeholder": "123456789012.dkr.ecr.us-east-1.amazonaws.com/dc43-backends:latest",
                        "help": "Container image containing the dc43 service backends.",
                    },
                    {
                        "name": "backend_token",
                        "label": "Backend bearer token",
                        "placeholder": "Optional shared secret",
                        "help": "Optional token enforced by the HTTP API – keep in sync with the exported TOML.",
                        "optional": True,
                    },
                    {
                        "name": "contract_store_mode",
                        "label": "Contract store mode",
                        "placeholder": "filesystem",
                        "help": "`filesystem` for EFS backed storage or `sql` when using the relational implementation.",
                        "optional": True,
                    },
                    {
                        "name": "contract_filesystem",
                        "label": "EFS filesystem ID",
                        "placeholder": "fs-0123456789abcdef0",
                        "help": "File system identifier used when `contract_store_mode` is `filesystem`.",
                        "optional": True,
                    },
                    {
                        "name": "contract_storage_path",
                        "label": "Container mount path",
                        "placeholder": "/contracts",
                        "help": "Directory inside the container exposing the contracts volume.",
                        "optional": True,
                    },
                    {
                        "name": "contract_store_dsn",
                        "label": "SQL DSN",
                        "placeholder": "postgresql+psycopg://user:pass@host:5432/contracts",
                        "help": "Connection string used when `contract_store_mode` is `sql`.",
                        "optional": True,
                    },
                    {
                        "name": "contract_store_dsn_secret_arn",
                        "label": "DSN secret ARN",
                        "placeholder": "arn:aws:secretsmanager:...",
                        "help": "Secrets Manager or SSM parameter storing the DSN (alternative to plaintext).",
                        "optional": True,
                    },
                    {
                        "name": "contract_store_table",
                        "label": "Contracts table name",
                        "placeholder": "contracts",
                        "help": "Override the default contracts table for the SQL store.",
                        "optional": True,
                    },
                    {
                        "name": "contract_store_schema",
                        "label": "Contracts schema",
                        "placeholder": "governance",
                        "help": "Optional schema/namespace for the contracts SQL store.",
                        "optional": True,
                    },
                    {
                        "name": "private_subnet_ids",
                        "label": "Private subnet IDs",
                        "placeholder": "subnet-1a2b3c4d, subnet-4d3c2b1a",
                        "help": "Comma separated list of subnet IDs used by the ECS tasks.",
                    },
                    {
                        "name": "load_balancer_subnet_ids",
                        "label": "Load balancer subnet IDs",
                        "placeholder": "subnet-1a2b3c4d, subnet-4d3c2b1a",
                        "help": "Comma separated list of subnet IDs for the public load balancer.",
                    },
                    {
                        "name": "service_security_group_id",
                        "label": "Service security group",
                        "placeholder": "sg-0123456789abcdef0",
                        "help": "Security group attached to the ECS tasks.",
                    },
                    {
                        "name": "load_balancer_security_group_id",
                        "label": "Load balancer security group",
                        "placeholder": "sg-0123456789abcdef0",
                        "help": "Security group attached to the Application Load Balancer.",
                    },
                    {
                        "name": "certificate_arn",
                        "label": "ACM certificate ARN",
                        "placeholder": "arn:aws:acm:...",
                        "help": "Certificate used for HTTPS ingress.",
                    },
                    {
                        "name": "vpc_id",
                        "label": "VPC ID",
                        "placeholder": "vpc-0123456789abcdef0",
                        "help": "Virtual private cloud hosting the deployment.",
                    },
                    {
                        "name": "task_cpu",
                        "label": "Task CPU units",
                        "placeholder": "512",
                        "help": "Fargate CPU allocation for the service backends task.",
                        "optional": True,
                    },
                    {
                        "name": "task_memory",
                        "label": "Task memory (MiB)",
                        "placeholder": "1024",
                        "help": "Fargate memory allocation for the service backends task.",
                        "optional": True,
                    },
                    {
                        "name": "container_port",
                        "label": "Container port",
                        "placeholder": "8001",
                        "help": "Port exposed by the container.",
                        "optional": True,
                    },
                    {
                        "name": "desired_count",
                        "label": "Desired task count",
                        "placeholder": "2",
                        "help": "Number of task replicas to run.",
                        "optional": True,
                    },
                    {
                        "name": "health_check_path",
                        "label": "Health check path",
                        "placeholder": "/health",
                        "help": "HTTP path polled by the load balancer.",
                        "optional": True,
                    },
                    {
                        "name": "health_check_interval",
                        "label": "Health check interval",
                        "placeholder": "30",
                        "help": "Seconds between health checks.",
                        "optional": True,
                    },
                    {
                        "name": "health_check_timeout",
                        "label": "Health check timeout",
                        "placeholder": "5",
                        "help": "Seconds before a health check is considered failed.",
                        "optional": True,
                    },
                    {
                        "name": "health_check_healthy_threshold",
                        "label": "Healthy threshold",
                        "placeholder": "2",
                        "help": "Number of consecutive successes before the target is considered healthy.",
                        "optional": True,
                    },
                    {
                        "name": "health_check_unhealthy_threshold",
                        "label": "Unhealthy threshold",
                        "placeholder": "2",
                        "help": "Number of consecutive failures before the target is considered unhealthy.",
                        "optional": True,
                    },
                    {
                        "name": "log_retention_days",
                        "label": "Log retention (days)",
                        "placeholder": "30",
                        "help": "CloudWatch log retention period.",
                        "optional": True,
                    },
                ],
            },
            "azure_terraform": {
                "label": "Azure (Terraform)",
                "description": "Deploy the governance APIs to Azure Container Apps using the bundled Terraform template.",
                "installation": [
                    "Copy the Terraform module under `deploy/terraform/azure-service-backend` into your infrastructure repository.",
                    "Authenticate the Azure CLI or provide service principal credentials to Terraform.",
                    "Review the generated `terraform.tfvars` and run `terraform apply` to provision the resources.",
                ],
                "configuration_notes": [
                    "Point the configuration at your container registry and decide whether the contracts store uses Azure Files or SQL.",
                    "Ensure the exported TOML configuration is mounted or baked into the container image.",
                ],
                "fields": [
                    {
                        "name": "subscription_id",
                        "label": "Subscription ID",
                        "placeholder": "00000000-0000-0000-0000-000000000000",
                        "help": "Azure subscription hosting the deployment.",
                    },
                    {
                        "name": "resource_group_name",
                        "label": "Resource group",
                        "placeholder": "rg-dc43-governance",
                        "help": "Resource group where the Container App and storage resources will be created.",
                    },
                    {
                        "name": "location",
                        "label": "Location",
                        "placeholder": "westeurope",
                        "help": "Azure region for the resources.",
                    },
                    {
                        "name": "container_registry",
                        "label": "Container registry",
                        "placeholder": "dc43.azurecr.io",
                        "help": "Azure Container Registry host serving the backend image.",
                    },
                    {
                        "name": "container_registry_username",
                        "label": "Registry username",
                        "placeholder": "dc43",
                        "help": "Username with pull permissions on the registry.",
                    },
                    {
                        "name": "container_registry_password",
                        "label": "Registry password",
                        "placeholder": "••••••",
                        "help": "Password or access key for the container registry.",
                    },
                    {
                        "name": "image_tag",
                        "label": "Image tag",
                        "placeholder": "dc43-backends:latest",
                        "help": "Full reference (repository:tag) for the container image.",
                    },
                    {
                        "name": "backend_token",
                        "label": "Backend bearer token",
                        "placeholder": "Optional shared secret",
                        "help": "Optional token enforced by the HTTP API – align with the exported TOML.",
                        "optional": True,
                    },
                    {
                        "name": "contract_store_mode",
                        "label": "Contract store mode",
                        "placeholder": "filesystem",
                        "help": "`filesystem` for Azure Files, `sql` for Azure SQL deployments.",
                        "optional": True,
                    },
                    {
                        "name": "contract_storage",
                        "label": "Container mount path",
                        "placeholder": "/contracts",
                        "help": "Directory inside the container exposing the Azure Files share.",
                        "optional": True,
                    },
                    {
                        "name": "contract_share_name",
                        "label": "Azure Files share name",
                        "placeholder": "contracts",
                        "help": "Name of the file share created for the filesystem-backed contracts store.",
                        "optional": True,
                    },
                    {
                        "name": "contract_share_quota_gb",
                        "label": "Share quota (GiB)",
                        "placeholder": "100",
                        "help": "Optional quota for the Azure Files share in gibibytes.",
                        "optional": True,
                    },
                    {
                        "name": "contract_store_dsn",
                        "label": "SQL DSN",
                        "placeholder": "sqlserver://user:pass@host:1433;database=contracts",
                        "help": "Connection string used when `contract_store_mode` is `sql`.",
                        "optional": True,
                    },
                    {
                        "name": "contract_store_table",
                        "label": "Contracts table name",
                        "placeholder": "contracts",
                        "help": "Override the default contracts table for the SQL store.",
                        "optional": True,
                    },
                    {
                        "name": "contract_store_schema",
                        "label": "Contracts schema",
                        "placeholder": "governance",
                        "help": "Optional schema/namespace for the contracts SQL store.",
                        "optional": True,
                    },
                    {
                        "name": "container_app_environment_name",
                        "label": "Container Apps environment",
                        "placeholder": "dc43-env",
                        "help": "Name assigned to the Container Apps environment.",
                        "optional": True,
                    },
                    {
                        "name": "container_app_name",
                        "label": "Container App name",
                        "placeholder": "dc43-service-backends",
                        "help": "Name of the Container App resource created by Terraform.",
                        "optional": True,
                    },
                    {
                        "name": "ingress_port",
                        "label": "Ingress port",
                        "placeholder": "8001",
                        "help": "Port exposed publicly by the Container App.",
                        "optional": True,
                    },
                    {
                        "name": "min_replicas",
                        "label": "Minimum replicas",
                        "placeholder": "1",
                        "help": "Lower bound for Container Apps autoscaling.",
                        "optional": True,
                    },
                    {
                        "name": "max_replicas",
                        "label": "Maximum replicas",
                        "placeholder": "3",
                        "help": "Upper bound for Container Apps autoscaling.",
                        "optional": True,
                    },
                    {
                        "name": "container_cpu",
                        "label": "Container CPU",
                        "placeholder": "0.5",
                        "help": "vCPU allocation per replica (for example 0.5, 1.0).",
                        "optional": True,
                    },
                    {
                        "name": "container_memory",
                        "label": "Container memory",
                        "placeholder": "1.0Gi",
                        "help": "Memory allocation per replica (GiB).",
                        "optional": True,
                    },
                    {
                        "name": "tags",
                        "label": "Resource tags",
                        "placeholder": "env=dev, owner=data-platform",
                        "help": "Comma-separated key=value pairs applied to Azure resources.",
                        "optional": True,
                    },
                ],
            },
            "not_required": {
                "label": "Not required (direct runtime)",
                "description": "Skip dedicated deployment automation when governance orchestration stays embedded in Python workloads.",
                "installation": [
                    "No standalone service is provisioned – pipelines import and execute governance code directly.",
                ],
                "configuration_notes": [
                    "Ensure orchestrators install `dc43-service-backends` and reference the generated TOML files.",
                ],
                "fields": [],
                "skip_configuration": True,
            },
        },
    },
    "governance_extensions": {
        "title": "Governance hooks",
        "summary": "Extend the governance service with additional integrations such as Unity Catalog tagging.",
        "options": {
            "none": {
                "label": "No additional hooks",
                "description": "Skip optional extensions – contracts and products remain self-contained.",
                "installation": [],
                "configuration_notes": [
                    "Use this option when you do not need to synchronise Unity Catalog or other downstream systems yet.",
                ],
                "fields": [],
            },
            "unity_catalog": {
                "label": "Unity Catalog synchronisation",
                "description": "Tag Delta tables and views in Unity Catalog with contract and product metadata.",
                "installation": [
                    "Install the `dc43-integrations` wheel on the Databricks cluster executing the hook.",
                    "Grant the service principal data steward permissions on the target catalog and schema.",
                ],
                "configuration_notes": [
                    "Provide Databricks host/token pairs via `DATABRICKS_HOST` and `DATABRICKS_TOKEN`.",
                    "Set `DC43_GOVERNANCE_HOOK=unity_catalog` so the governance job loads the extension module.",
                ],
                "fields": [
                    {
                        "name": "workspace_url",
                        "label": "Workspace URL",
                        "placeholder": "https://adb-1234567890123456.7.azuredatabricks.net",
                        "help": "Base URL of the Databricks workspace hosting Unity Catalog.",
                    },
                    {
                        "name": "catalog",
                        "label": "Catalog",
                        "placeholder": "main",
                        "help": "Unity Catalog containing the managed tables to tag.",
                    },
                    {
                        "name": "schema",
                        "label": "Schema",
                        "placeholder": "contracts",
                        "help": "Schema that will receive Unity Catalog tags.",
                    },
                    {
                        "name": "token",
                        "label": "Personal access token",
                        "placeholder": "dapi...",
                        "help": "Databricks PAT used by the governance hook.",
                    },
                ],
            },
            "custom_module": {
                "label": "Custom Python module",
                "description": "Load your own governance hook module for bespoke synchronisation steps.",
                "installation": [
                    "Package the Python module with your deployment or mount it into the container image.",
                    "Document the callable entrypoints (`register`, `on_publish`, …) expected by the governance runtime.",
                ],
                "configuration_notes": [
                    "Set `DC43_GOVERNANCE_HOOK=python` and provide the module path below.",
                    "Use virtualenv or Poetry extras to install dependencies required by the hook.",
                ],
                "fields": [
                    {
                        "name": "module_path",
                        "label": "Module import path",
                        "placeholder": "acme.governance.hooks",
                        "help": "Python import path that exposes the hook entrypoints.",
                    },
                    {
                        "name": "config_path",
                        "label": "Hook configuration file",
                        "placeholder": "/workspace/config/governance_hook.yml",
                        "help": "Optional YAML/JSON file consumed by the custom hook.",
                        "optional": True,
                    },
                ],
            },
        },
    },
    "user_interface": {
        "title": "User interface",
        "summary": "Choose how operators will interact with the governance workflows.",
        "options": {
            "local_web": {
                "label": "Bundled web application",
                "description": "Serve the FastAPI + Bootstrap UI directly from the same container as the services.",
                "installation": [
                    "Expose the web port (default 8000) and secure it behind your ingress/proxy of choice.",
                    "Mount the workspace volume so UI actions persist to the same storage as the services.",
                ],
                "configuration_notes": [
                    "Use environment variables from previous sections – no additional settings are required.",
                    "Set `DC43_UI_MODE=local` when packaging the UI with Docker Compose.",
                ],
                "fields": [],
            },
            "remote_portal": {
                "label": "Hosted portal",
                "description": "Point users to an externally hosted UI that talks to the governance API.",
                "installation": [
                    "Deploy the UI assets (for example via static hosting + backend-for-frontend layer).",
                    "Configure networking so the hosted portal can reach the remote governance service.",
                ],
                "configuration_notes": [
                    "Set `DC43_UI_MODE=remote` to disable the embedded templates.",
                    "Provide the portal base URL below so deep links are generated correctly.",
                ],
                "fields": [
                    {
                        "name": "portal_url",
                        "label": "Portal base URL",
                        "placeholder": "https://contracts.acme.com",
                        "help": "URL users will visit to access the hosted UI.",
                    },
                ],
            },
        },
    },
    "ui_deployment": {
        "title": "User interface deployment",
        "summary": "Document how the contracts UI is hosted so deployment scripts and Terraform variables can be generated per environment.",
        "depends_on": "user_interface",
        "visible_when": {
            "local_web": ["skip_hosting"],
            "local_python": ["local_python"],
            "local_docker": ["local_docker"],
            "remote_portal": ["skip_hosting", "aws_terraform", "azure_terraform"],
            "aws_terraform": ["aws_terraform"],
            "azure_terraform": ["azure_terraform"],
            "skip_hosting": ["skip_hosting"],
        },
        "hide_when": ["local_web", "skip_hosting"],
        "default_for": {
            "local_web": "skip_hosting",
            "local_python": "local_python",
            "local_docker": "local_docker",
            "remote_portal": "skip_hosting",
            "aws_terraform": "aws_terraform",
            "azure_terraform": "azure_terraform",
            "skip_hosting": "skip_hosting",
        },
        "default_option": "skip_hosting",
        "options": {
            "local_python": {
                "label": "Local Python process",
                "description": "Run the UI with the bundled FastAPI server directly from your shell (no containers).",
                "installation": [
                    "Install the contracts app package and its extras into a virtual environment.",
                    "Start the UI with `uvicorn dc43_contracts_app.server:app --reload --port 8000` and keep the terminal session running.",
                ],
                "configuration_notes": [
                    "Load the generated TOML files via `DC43_CONTRACTS_APP_CONFIG` or environment variables.",
                    "Ensure the workspace folders selected earlier are accessible on the host machine.",
                ],
                "fields": [
                    {
                        "name": "command",
                        "label": "Launch command",
                        "placeholder": "uvicorn dc43_contracts_app.server:app --reload --port 8000",
                        "help": "Command your operators should run to bring up the UI locally.",
                        "optional": True,
                        "default_factory": lambda workspace: "uvicorn dc43_contracts_app.server:app --reload --port 8000",
                    },
                ],
            },
            "local_docker": {
                "label": "Local Docker or Compose",
                "description": "Run the UI from the same container that serves governance by using the helper scripts provided by the wizard.",
                "installation": [
                    "Install Docker Desktop or an equivalent container runtime.",
                    "Launch the stack with `scripts/run_local_stack.py` or integrate the container into your Compose file.",
                ],
                "configuration_notes": [
                    "Point the UI at the exported TOML configuration or environment variables from previous sections.",
                    "Reuse the same workspace volume as the governance service so edits persist.",
                ],
                "fields": [],
            },
            "skip_hosting": {
                "label": "No dedicated hosting",
                "description": "Document that the UI will not be deployed – operators rely on APIs or another portal.",
                "installation": [
                    "No services are provisioned by this wizard selection.",
                ],
                "configuration_notes": [
                    "Ensure governance APIs are reachable for automation even without the bundled UI.",
                ],
                "fields": [],
                "skip_configuration": True,
            },
            "aws_terraform": {
                "label": "AWS (Terraform scaffold)",
                "description": "Capture AWS hosting details so you can feed them into your infrastructure-as-code module for the UI.",
                "installation": [
                    "Clone or adapt your existing Terraform stacks for ECS/ALB frontends.",
                    "Copy the generated `terraform.tfvars` stub into that repository to keep values in sync with the wizard.",
                ],
                "configuration_notes": [
                    "Provide the container image that serves the UI (for example the monolith image with `DC43_UI_MODE=local`).",
                    "Ensure the security groups and subnets align with those used by the governance service when colocated.",
                ],
                "fields": [
                    {
                        "name": "aws_region",
                        "label": "AWS region",
                        "placeholder": "us-east-1",
                        "help": "Region hosting the UI frontend.",
                    },
                    {
                        "name": "cluster_name",
                        "label": "ECS cluster name",
                        "placeholder": "dc43-ui",
                        "help": "Existing ECS/Fargate cluster that will run the UI tasks.",
                    },
                    {
                        "name": "service_name",
                        "label": "ECS service name",
                        "placeholder": "dc43-contracts-ui",
                        "help": "Name of the ECS service that will expose the UI.",
                        "optional": True,
                    },
                    {
                        "name": "ecr_image_uri",
                        "label": "ECR image URI",
                        "placeholder": "123456789012.dkr.ecr.us-east-1.amazonaws.com/dc43-contracts:latest",
                        "help": "Container image containing the contracts UI application.",
                    },
                    {
                        "name": "private_subnet_ids",
                        "label": "Private subnet IDs",
                        "placeholder": "subnet-1a2b3c4d, subnet-4d3c2b1a",
                        "help": "Comma separated list of subnet IDs used by the ECS tasks.",
                    },
                    {
                        "name": "load_balancer_subnet_ids",
                        "label": "Load balancer subnet IDs",
                        "placeholder": "subnet-1a2b3c4d, subnet-4d3c2b1a",
                        "help": "Comma separated list of subnet IDs for the public load balancer.",
                    },
                    {
                        "name": "service_security_group_id",
                        "label": "Service security group",
                        "placeholder": "sg-0123456789abcdef0",
                        "help": "Security group attached to the ECS tasks.",
                    },
                    {
                        "name": "load_balancer_security_group_id",
                        "label": "Load balancer security group",
                        "placeholder": "sg-0123456789abcdef0",
                        "help": "Security group attached to the Application Load Balancer.",
                    },
                    {
                        "name": "certificate_arn",
                        "label": "ACM certificate ARN",
                        "placeholder": "arn:aws:acm:...",
                        "help": "Certificate used for HTTPS ingress.",
                    },
                    {
                        "name": "vpc_id",
                        "label": "VPC ID",
                        "placeholder": "vpc-0123456789abcdef0",
                        "help": "Virtual private cloud hosting the deployment.",
                    },
                    {
                        "name": "container_port",
                        "label": "Container port",
                        "placeholder": "8000",
                        "help": "Port exposed by the UI container.",
                        "optional": True,
                    },
                    {
                        "name": "desired_count",
                        "label": "Desired task count",
                        "placeholder": "2",
                        "help": "Number of task replicas to run.",
                        "optional": True,
                    },
                    {
                        "name": "task_cpu",
                        "label": "Task CPU units",
                        "placeholder": "512",
                        "help": "Fargate CPU allocation for the UI task.",
                        "optional": True,
                    },
                    {
                        "name": "task_memory",
                        "label": "Task memory (MiB)",
                        "placeholder": "1024",
                        "help": "Fargate memory allocation for the UI task.",
                        "optional": True,
                    },
                    {
                        "name": "health_check_path",
                        "label": "Health check path",
                        "placeholder": "/health",
                        "help": "HTTP path polled by the load balancer.",
                        "optional": True,
                    },
                    {
                        "name": "health_check_interval",
                        "label": "Health check interval",
                        "placeholder": "30",
                        "help": "Seconds between health checks.",
                        "optional": True,
                    },
                    {
                        "name": "health_check_timeout",
                        "label": "Health check timeout",
                        "placeholder": "5",
                        "help": "Seconds before a health check is considered failed.",
                        "optional": True,
                    },
                    {
                        "name": "health_check_healthy_threshold",
                        "label": "Healthy threshold",
                        "placeholder": "2",
                        "help": "Number of consecutive successes before the target is considered healthy.",
                        "optional": True,
                    },
                    {
                        "name": "health_check_unhealthy_threshold",
                        "label": "Unhealthy threshold",
                        "placeholder": "2",
                        "help": "Number of consecutive failures before the target is considered unhealthy.",
                        "optional": True,
                    },
                    {
                        "name": "log_retention_days",
                        "label": "Log retention (days)",
                        "placeholder": "30",
                        "help": "CloudWatch log retention period.",
                        "optional": True,
                    },
                ],
            },
            "azure_terraform": {
                "label": "Azure (Terraform scaffold)",
                "description": "Collect Azure Container Apps parameters to drive your UI deployment templates.",
                "installation": [
                    "Integrate the generated variables into your Container Apps Terraform module or Bicep deployment.",
                    "Store sensitive values (like registry passwords) in your preferred secret manager before applying.",
                ],
                "configuration_notes": [
                    "Provide the registry location for the UI image and adjust autoscaling bounds as required.",
                    "Align the tags with your governance service deployment for easier inventory tracking.",
                ],
                "fields": [
                    {
                        "name": "subscription_id",
                        "label": "Subscription ID",
                        "placeholder": "00000000-0000-0000-0000-000000000000",
                        "help": "Azure subscription hosting the UI deployment.",
                    },
                    {
                        "name": "resource_group_name",
                        "label": "Resource group",
                        "placeholder": "rg-dc43-ui",
                        "help": "Resource group where the Container App will be created.",
                    },
                    {
                        "name": "location",
                        "label": "Location",
                        "placeholder": "westeurope",
                        "help": "Azure region for the resources.",
                    },
                    {
                        "name": "container_registry",
                        "label": "Container registry",
                        "placeholder": "dc43.azurecr.io",
                        "help": "Azure Container Registry host serving the UI image.",
                    },
                    {
                        "name": "container_registry_username",
                        "label": "Registry username",
                        "placeholder": "dc43",
                        "help": "Username with pull permissions on the registry.",
                    },
                    {
                        "name": "container_registry_password",
                        "label": "Registry password",
                        "placeholder": "••••••",
                        "help": "Password or access key for the container registry.",
                    },
                    {
                        "name": "image_tag",
                        "label": "Image tag",
                        "placeholder": "dc43-contracts:latest",
                        "help": "Full reference (repository:tag) for the UI container image.",
                    },
                    {
                        "name": "container_app_environment_name",
                        "label": "Container Apps environment",
                        "placeholder": "dc43-ui-env",
                        "help": "Name assigned to the Container Apps environment.",
                        "optional": True,
                    },
                    {
                        "name": "container_app_name",
                        "label": "Container App name",
                        "placeholder": "dc43-contracts-ui",
                        "help": "Name of the Container App resource created by Terraform.",
                        "optional": True,
                    },
                    {
                        "name": "ingress_port",
                        "label": "Ingress port",
                        "placeholder": "8000",
                        "help": "Port exposed publicly by the Container App.",
                        "optional": True,
                    },
                    {
                        "name": "min_replicas",
                        "label": "Minimum replicas",
                        "placeholder": "1",
                        "help": "Lower bound for Container Apps autoscaling.",
                        "optional": True,
                    },
                    {
                        "name": "max_replicas",
                        "label": "Maximum replicas",
                        "placeholder": "3",
                        "help": "Upper bound for Container Apps autoscaling.",
                        "optional": True,
                    },
                    {
                        "name": "container_cpu",
                        "label": "Container CPU",
                        "placeholder": "0.5",
                        "help": "vCPU allocation per replica (for example 0.5, 1.0).",
                        "optional": True,
                    },
                    {
                        "name": "container_memory",
                        "label": "Container memory",
                        "placeholder": "1.0Gi",
                        "help": "Memory allocation per replica (GiB).",
                        "optional": True,
                    },
                    {
                        "name": "tags",
                        "label": "Resource tags",
                        "placeholder": "env=dev, owner=data-platform",
                        "help": "Comma-separated key=value pairs applied to Azure resources.",
                        "optional": True,
                    },
                ],
            },
        },
    },
    "demo_automation": {
        "title": "Demo automation",
        "summary": "Optionally launch the sample stack so teams can explore dc43 quickly.",
        "default_option": "skip_demo",
        "options": {
            "skip_demo": {
                "label": "Do not launch the demo",
                "description": "Skip demo orchestration and proceed with production workflows only.",
                "installation": [
                    "No demo-specific scripts or datasets will be provisioned.",
                ],
                "configuration_notes": [
                    "You can always re-run the wizard and export demo helpers later if needed.",
                ],
                "fields": [],
                "skip_configuration": True,
            },
            "local_python": {
                "label": "Run demo locally (Python)",
                "description": "Execute the generated helper script to populate sample contracts and run orchestrated pipelines on your laptop.",
                "installation": [
                    "Install the repository in editable mode (`pip install -e .[demo]`) so demo modules are importable.",
                    "Execute `python scripts/run_demo.py` from the exported bundle to seed sample data and launch demo flows.",
                ],
                "configuration_notes": [
                    "Ensure the workspace directories selected in storage steps exist and are writable.",
                    "Review the generated README for credentials and follow-up teardown guidance.",
                ],
                "fields": [],
            },
            "local_docker": {
                "label": "Run demo locally (Docker)",
                "description": "Leverage Docker Compose via the helper script to start the demo UI, governance APIs, and datasets.",
                "installation": [
                    "Install Docker Desktop (or equivalent) with access to the repository workspace volume.",
                    "Run `./run_local_stack.py --with-demo` from the exported bundle to launch the demo stack.",
                ],
                "configuration_notes": [
                    "The helper reuses the TOML configuration emitted by the wizard for consistency.",
                    "Stop the containers before running infrastructure automation to avoid port conflicts.",
                ],
                "fields": [],
            },
        },
    },
    "authentication": {
        "title": "Authentication & access",
        "summary": "Configure how users authenticate with the UI and downstream services.",
        "options": {
            "none": {
                "label": "No authentication",
                "description": "Rely on network controls only – all endpoints remain unauthenticated.",
                "installation": [],
                "configuration_notes": [
                    "Use this only in isolated demo environments.",
                    "Consider enabling at least basic auth before exposing the UI broadly.",
                ],
                "fields": [],
            },
            "basic": {
                "label": "HTTP basic auth",
                "description": "Protect the UI with a simple username/password pair managed via environment variables.",
                "installation": [
                    "Generate credentials and inject them as secrets in your container orchestrator.",
                    "Enable HTTPS termination on your ingress/load balancer.",
                ],
                "configuration_notes": [
                    "Set `DC43_AUTH_MODE=basic` and provide the credentials below.",
                    "Rotate passwords frequently and avoid reusing them across environments.",
                ],
                "fields": [
                    {
                        "name": "username",
                        "label": "Username",
                        "placeholder": "governance-admin",
                        "help": "Login used to access the UI.",
                    },
                    {
                        "name": "password",
                        "label": "Password",
                        "placeholder": "••••••",
                        "help": "Strong password stored as a secret.",
                    },
                ],
            },
            "oauth_oidc": {
                "label": "OAuth / OIDC",
                "description": "Delegate authentication to your identity provider using OAuth 2.0 / OpenID Connect.",
                "installation": [
                    "Register the application with your IdP and generate client credentials.",
                    "Configure redirect URIs that match the portal hostname or localhost for development.",
                ],
                "configuration_notes": [
                    "Set `DC43_AUTH_MODE=oauth` and configure the client settings below.",
                    "Sync group or role mappings in the governance service to enforce authorisation policies.",
                ],
                "fields": [
                    {
                        "name": "issuer_url",
                        "label": "Issuer URL",
                        "placeholder": "https://login.microsoftonline.com/<tenant>/v2.0",
                        "help": "Discovery endpoint for the identity provider.",
                    },
                    {
                        "name": "client_id",
                        "label": "Client ID",
                        "placeholder": "00000000-0000-0000-0000-000000000000",
                        "help": "Application (client) identifier registered with the IdP.",
                    },
                    {
                        "name": "client_secret",
                        "label": "Client secret",
                        "placeholder": "••••••",
                        "help": "Client secret or certificate thumbprint used for token exchange.",
                    },
                    {
                        "name": "redirect_uri",
                        "label": "Redirect URI",
                        "placeholder": "https://contracts.acme.com/oauth/callback",
                        "help": "URL the IdP will redirect to after authentication.",
                    },
                ],
            },
        },
    },
}


SETUP_MODULE_GROUPS: List[Dict[str, Any]] = [
    {
        "key": "storage_foundations",
        "title": "Storage foundations",
        "summary": "Decide how contracts and product metadata are persisted before the orchestration layer is wired in.",
        "modules": ["contracts_backend", "products_backend"],
    },
    {
        "key": "governance_runtime",
        "title": "Governance runtime",
        "summary": "Pair the governance interface model with the matching deployment approach so runtime and automation stay in sync.",
        "modules": ["governance_service", "governance_deployment"],
    },
    {
        "key": "user_experience",
        "title": "User experience",
        "summary": "Choose how operators reach the contracts UI and how that interface is hosted or automated.",
        "modules": ["user_interface", "ui_deployment"],
    },
    {
        "key": "access_security",
        "title": "Access & security",
        "summary": "Specify how users authenticate with the UI and downstream services.",
        "modules": ["authentication"],
    },
    {
        "key": "quality_extensions",
        "title": "Quality & extensions",
        "summary": "Cover the bundled data-quality engine and optional governance hooks that enrich downstream catalogs.",
        "modules": ["data_quality", "governance_extensions"],
    },
    {
        "key": "accelerators",
        "title": "Accelerators",
        "summary": "Optionally launch the bundled demo so teams can explore the platform end-to-end.",
        "modules": ["demo_automation"],
    },
]


_SETUP_TOTAL_STEPS = 3


def _setup_progress(step: int) -> int:
    """Return a progress percentage for the onboarding wizard."""

    clamped = max(1, min(_SETUP_TOTAL_STEPS, step))
    return int((clamped / _SETUP_TOTAL_STEPS) * 100)


def _requires_configuration(selected: Mapping[str, str], configuration: Mapping[str, Any]) -> bool:
    """Return ``True`` when mandatory fields are missing from ``configuration``."""

    for module_key, option_key in selected.items():
        module_meta = SETUP_MODULES.get(module_key)
        if not module_meta:
            continue
        option_meta = module_meta["options"].get(option_key)
        if not option_meta:
            continue
        config_values = configuration.get(module_key, {})
        for field_meta in option_meta.get("fields", []):
            if field_meta.get("optional"):
                continue
            value = str(config_values.get(field_meta.get("name"), "") or "").strip()
            if not value:
                return True
    return False


def _module_dependency_value(module_key: str, selected: Mapping[str, str]) -> str | None:
    """Return the dependency value for ``module_key`` if configured."""

    module_meta = SETUP_MODULES.get(module_key)
    if not module_meta:
        return None
    depends_on = module_meta.get("depends_on")
    if not depends_on:
        return None
    value = selected.get(depends_on)
    return str(value) if value is not None else None


def _module_visible_options(module_key: str, selected: Mapping[str, str]) -> List[str]:
    """Return option keys that should be shown for ``module_key``."""

    module_meta = SETUP_MODULES.get(module_key)
    if not module_meta:
        return []
    options = module_meta.get("options", {})
    depends_on = module_meta.get("depends_on")
    visible_map = module_meta.get("visible_when") or {}
    if not depends_on or not visible_map:
        return [str(key) for key in options.keys()]
    dependency_value = _module_dependency_value(module_key, selected)
    if dependency_value is None:
        allowed = visible_map.get("__default__", [])
    else:
        allowed = visible_map.get(dependency_value)
        if allowed is None:
            allowed = visible_map.get("__default__", [])
    if not isinstance(allowed, Iterable):
        return []
    return [key for key in allowed if key in options]


def _module_should_hide(module_key: str, selected: Mapping[str, str]) -> bool:
    """Return ``True`` when a module should be hidden based on dependencies."""

    module_meta = SETUP_MODULES.get(module_key)
    if not module_meta:
        return False
    hide_when = module_meta.get("hide_when") or []
    if not hide_when:
        return False
    dependency_value = _module_dependency_value(module_key, selected)
    return dependency_value in hide_when


def _module_default_option(module_key: str, selected: Mapping[str, str]) -> str | None:
    """Return the default option for ``module_key`` given current selections."""

    module_meta = SETUP_MODULES.get(module_key)
    if not module_meta:
        return None
    default_map = module_meta.get("default_for") or {}
    dependency_value = _module_dependency_value(module_key, selected)
    if dependency_value is not None and dependency_value in default_map:
        return str(default_map[dependency_value])
    default_option = module_meta.get("default_option")
    if isinstance(default_option, str) and default_option in module_meta.get("options", {}):
        return default_option
    visible_options = _module_visible_options(module_key, selected)
    if len(visible_options) == 1:
        return visible_options[0]
    return None


def _serialise_field(
    module_key: str,
    field_meta: Mapping[str, Any],
    *,
    configuration: Mapping[str, Any],
    workspace: ContractsAppWorkspace,
) -> Dict[str, Any]:
    """Return template-friendly metadata for a setup field."""

    stored = configuration.get(module_key, {}) if isinstance(configuration, Mapping) else {}
    value = str(stored.get(field_meta.get("name"), "") or "")
    if not value:
        if "default" in field_meta and field_meta["default"] is not None:
            value = str(field_meta["default"])
        else:
            default_factory = field_meta.get("default_factory")
            if callable(default_factory):
                try:
                    value = str(default_factory(workspace))
                except Exception:  # pragma: no cover - defensive defaults
                    value = ""
    return {
        "name": field_meta.get("name"),
        "label": field_meta.get("label"),
        "placeholder": field_meta.get("placeholder", ""),
        "help": field_meta.get("help", ""),
        "optional": bool(field_meta.get("optional")),
        "type": field_meta.get("type", "text"),
        "value": value,
    }


def _serialise_diagram(diagram_meta: Mapping[str, Any]) -> Dict[str, Any]:
    """Return a JSON-safe payload describing diagram nodes/edges."""

    nodes: List[Dict[str, Any]] = []
    for node_meta in diagram_meta.get("nodes", []):
        if not isinstance(node_meta, Mapping):
            continue
        node_id = str(node_meta.get("id") or "").strip()
        if not node_id:
            continue
        node_payload: Dict[str, Any] = {
            "id": node_id,
            "label": node_meta.get("label") or node_id,
        }
        class_name = (
            node_meta.get("class")
            or node_meta.get("class_name")
            or node_meta.get("className")
        )
        if class_name:
            node_payload["className"] = str(class_name)

        edges: List[Dict[str, Any]] = []
        for edge_meta in node_meta.get("edges", []):
            if not isinstance(edge_meta, Mapping):
                continue
            edge_from = str(edge_meta.get("from") or "").strip()
            edge_to = str(edge_meta.get("to") or "").strip()
            if not edge_from or not edge_to:
                continue
            edge_payload: Dict[str, Any] = {
                "from": edge_from,
                "to": edge_to,
            }
            if edge_meta.get("label"):
                edge_payload["label"] = str(edge_meta.get("label"))
            edges.append(edge_payload)

        if edges:
            node_payload["edges"] = edges

        nodes.append(node_payload)

    payload: Dict[str, Any] = {}
    if nodes:
        payload["nodes"] = nodes
    return payload


def _setup_export_payload(state: Mapping[str, Any]) -> Dict[str, Any]:
    """Return a serialisable payload that captures the wizard selections."""

    selected_options = state.get("selected_options") if isinstance(state, Mapping) else {}
    configuration = state.get("configuration") if isinstance(state, Mapping) else {}
    if not isinstance(selected_options, Mapping):
        selected_options = {}
    if not isinstance(configuration, Mapping):
        configuration = {}

    modules: List[Dict[str, Any]] = []
    for module_key, option_key in selected_options.items():
        module_meta = SETUP_MODULES.get(module_key)
        if not module_meta:
            continue
        option_meta = module_meta["options"].get(option_key)
        if not option_meta:
            continue
        module_config = configuration.get(module_key, {})
        if not isinstance(module_config, Mapping):
            module_config = {}

        settings: Dict[str, Any] = {}
        for field_meta in option_meta.get("fields", []):
            field_name = str(field_meta.get("name") or "")
            if not field_name:
                continue
            raw_value = module_config.get(field_name)
            if isinstance(raw_value, str):
                value = raw_value.strip()
            else:
                value = raw_value
            if value in {None, ""}:
                continue
            settings[field_name] = value

        modules.append(
            {
                "key": module_key,
                "title": module_meta.get("title"),
                "summary": module_meta.get("summary"),
                "option": option_key,
                "option_label": option_meta.get("label"),
                "installation": list(option_meta.get("installation", [])),
                "configuration_notes": list(option_meta.get("configuration_notes", [])),
                "settings": settings,
            }
        )

    configuration_payload: Dict[str, Dict[str, Any]] = {}
    for module_key, module_values in configuration.items():
        if isinstance(module_values, Mapping):
            configuration_payload[module_key] = {
                key: value
                for key, value in module_values.items()
                if not isinstance(value, Mapping)
            }

    payload: Dict[str, Any] = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "completed": bool(state.get("completed")),
        "modules": modules,
        "selected_options": dict(selected_options),
        "configuration": configuration_payload,
    }
    completed_at = state.get("completed_at") if isinstance(state, Mapping) else None
    if isinstance(completed_at, str) and completed_at:
        payload["completed_at"] = completed_at
    return payload


def _clean_str(value: Any) -> str | None:
    """Return ``value`` as a trimmed string when non-empty."""

    if isinstance(value, str):
        trimmed = value.strip()
        if trimmed:
            return trimmed
    return None


def _toml_string(value: str) -> str:
    """Serialise ``value`` using TOML string quoting rules."""

    return json.dumps(value)


def _clean_number(value: Any) -> int | float | None:
    """Return ``value`` as a number when possible."""

    if isinstance(value, (int, float)):
        return value
    text = _clean_str(value)
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        try:
            return float(text)
        except ValueError:
            return None


def _split_csv(value: Any) -> List[str]:
    """Return a list parsed from comma/newline separated ``value``."""

    text = _clean_str(value)
    if not text:
        return []
    parts = [part.strip() for part in re.split(r"[,\n]+", text) if part.strip()]
    return parts


def _parse_key_value_pairs(value: Any) -> Dict[str, str]:
    """Return a map parsed from ``key=value`` pairs separated by commas/newlines."""

    text = _clean_str(value)
    if not text:
        return {}
    pairs: Dict[str, str] = {}
    for part in re.split(r"[,\n]+", text):
        if not part.strip():
            continue
        if "=" not in part:
            continue
        key, raw_value = part.split("=", 1)
        key = key.strip()
        if not key:
            continue
        pairs[key] = raw_value.strip()
    return pairs


def _terraform_identifier(key: str) -> str:
    """Return a Terraform-compatible identifier for ``key``."""

    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
        return key
    return json.dumps(key)


def _terraform_literal(value: Any) -> str:
    """Return ``value`` formatted as a Terraform literal."""

    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, Mapping):
        items = []
        for key, item_value in value.items():
            ident = _terraform_identifier(str(key))
            items.append(f"  {ident} = {_terraform_literal(item_value)}")
        if not items:
            return "{}"
        return "{\n" + "\n".join(items) + "\n}"
    if isinstance(value, Iterable) and not isinstance(value, (bytes, bytearray)):
        items = [f"  {_terraform_literal(item)}," for item in value]
        if not items:
            return "[]"
        return "[\n" + "\n".join(items) + "\n]"
    return json.dumps(str(value))


def _render_terraform_tfvars(
    entries: List[Tuple[str, Any]],
    missing: Iterable[str],
) -> str:
    """Render ``entries`` as a Terraform ``.tfvars`` file."""

    lines = [
        "# terraform.tfvars generated by the dc43 setup wizard",
        "# Update any TODO values before running `terraform apply`.",
        "",
    ]
    missing_list = [item for item in missing if item]
    if missing_list:
        lines.append("# The following values are still required:")
        for item in missing_list:
            lines.append(f"# - {item}")
        lines.append("")
    for key, value in entries:
        lines.append(f"{key} = {_terraform_literal(value)}")
    lines.append("")
    return "\n".join(lines)


def _collect_tfvars_entries(
    data: Mapping[str, Any],
    *,
    field_map: Mapping[str, str],
    field_labels: Mapping[str, str],
    required_fields: Iterable[str],
    list_fields: Iterable[str] = (),
    numeric_fields: Iterable[str] = (),
    map_fields: Iterable[str] = (),
    list_parser: Callable[[Any], Iterable[Any]] | None = None,
    map_parser: Callable[[Any], Mapping[str, Any]] | None = None,
) -> Tuple[List[Tuple[str, Any]], List[str]]:
    """Return Terraform variable entries and missing labels for ``field_map``."""

    entries: List[Tuple[str, Any]] = []
    missing_labels: List[str] = []
    required_set = {str(name) for name in required_fields}
    list_fields = {str(name) for name in list_fields}
    numeric_fields = {str(name) for name in numeric_fields}
    map_fields = {str(name) for name in map_fields}

    for field_name, var_name in field_map.items():
        field_name = str(field_name)
        if field_name in map_fields:
            parser = map_parser or (lambda value: _parse_key_value_pairs(value))
            mapping = parser(data.get(field_name))
            if mapping:
                entries.append((var_name, mapping))
            elif field_name in required_set:
                missing_labels.append(field_labels.get(field_name, field_name))
        elif field_name in list_fields:
            parser = list_parser or (lambda value: _split_csv(value))
            values = [item for item in parser(data.get(field_name)) if item]
            if values:
                entries.append((var_name, values))
            elif field_name in required_set:
                missing_labels.append(field_labels.get(field_name, field_name))
        elif field_name in numeric_fields:
            number = _clean_number(data.get(field_name))
            if number is not None:
                entries.append((var_name, number))
            elif field_name in required_set:
                missing_labels.append(field_labels.get(field_name, field_name))
        else:
            text = _clean_str(data.get(field_name))
            if text:
                entries.append((var_name, text))
            elif field_name in required_set:
                missing_labels.append(field_labels.get(field_name, field_name))

    return entries, missing_labels


def _terraform_template_files(
    provider: str,
    *,
    target: str,
    template_dir: Path | None = None,
) -> List[Tuple[str, str]]:
    """Return template files bundled with the repository for ``provider`` and ``target``."""

    files: List[Tuple[str, str]] = []
    base = template_dir or (TERRAFORM_TEMPLATE_ROOT / f"{provider}-service-backend")
    for filename in ("main.tf", "variables.tf", "README.md"):
        path = base / filename
        if not path.exists():
            continue
        try:
            content = path.read_text(encoding="utf-8")
        except OSError:
            continue
        files.append((f"dc43-setup/terraform/{target}/{provider}/{filename}", content))
    return files


def _aws_governance_tfvars(
    module_config: Mapping[str, Any],
    *,
    field_labels: Mapping[str, str],
    required_fields: Iterable[str],
    selected: Mapping[str, str],
) -> Tuple[str | None, List[str]]:
    """Return rendered AWS ``terraform.tfvars`` content and missing labels for governance deployments."""

    data = {str(key): value for key, value in module_config.items()}
    contract_backend = selected.get("contracts_backend", "").strip().lower()
    if not _clean_str(data.get("contract_store_mode")):
        data["contract_store_mode"] = "sql" if contract_backend == "sql" else "filesystem"

    field_map = {
        "aws_region": "aws_region",
        "cluster_name": "cluster_name",
        "ecr_image_uri": "ecr_image_uri",
        "backend_token": "backend_token",
        "contract_store_mode": "contract_store_mode",
        "contract_filesystem": "contract_filesystem",
        "contract_storage_path": "contract_storage_path",
        "contract_store_dsn": "contract_store_dsn",
        "contract_store_dsn_secret_arn": "contract_store_dsn_secret_arn",
        "contract_store_table": "contract_store_table",
        "contract_store_schema": "contract_store_schema",
        "private_subnet_ids": "private_subnet_ids",
        "load_balancer_subnet_ids": "load_balancer_subnet_ids",
        "service_security_group_id": "service_security_group_id",
        "load_balancer_security_group_id": "load_balancer_security_group_id",
        "certificate_arn": "certificate_arn",
        "vpc_id": "vpc_id",
        "desired_count": "desired_count",
        "task_cpu": "task_cpu",
        "task_memory": "task_memory",
        "health_check_path": "health_check_path",
    }
    entries, missing_labels = _collect_tfvars_entries(
        data,
        field_map=field_map,
        field_labels=field_labels,
        required_fields=required_fields,
        list_fields={"private_subnet_ids", "load_balancer_subnet_ids"},
        numeric_fields={"desired_count"},
    )
    if not entries and not missing_labels:
        return None, []
    return _render_terraform_tfvars(entries, missing_labels), missing_labels

def _aws_ui_tfvars(
    module_config: Mapping[str, Any],
    *,
    field_labels: Mapping[str, str],
    required_fields: Iterable[str],
    selected: Mapping[str, str],
) -> Tuple[str | None, List[str]]:
    """Return rendered AWS ``terraform.tfvars`` content for UI deployments."""

    data = {str(key): value for key, value in module_config.items()}
    field_map = {
        "aws_region": "aws_region",
        "cluster_name": "cluster_name",
        "service_name": "service_name",
        "ecr_image_uri": "ecr_image_uri",
        "private_subnet_ids": "private_subnet_ids",
        "load_balancer_subnet_ids": "load_balancer_subnet_ids",
        "service_security_group_id": "service_security_group_id",
        "load_balancer_security_group_id": "load_balancer_security_group_id",
        "certificate_arn": "certificate_arn",
        "vpc_id": "vpc_id",
        "container_port": "container_port",
        "desired_count": "desired_count",
        "task_cpu": "task_cpu",
        "task_memory": "task_memory",
        "health_check_path": "health_check_path",
        "health_check_interval": "health_check_interval",
        "health_check_timeout": "health_check_timeout",
        "health_check_healthy_threshold": "health_check_healthy_threshold",
        "health_check_unhealthy_threshold": "health_check_unhealthy_threshold",
        "log_retention_days": "log_retention_days",
    }
    entries, missing_labels = _collect_tfvars_entries(
        data,
        field_map=field_map,
        field_labels=field_labels,
        required_fields=required_fields,
        list_fields={"private_subnet_ids", "load_balancer_subnet_ids"},
        numeric_fields={
            "container_port",
            "desired_count",
            "health_check_interval",
            "health_check_timeout",
            "health_check_healthy_threshold",
            "health_check_unhealthy_threshold",
            "log_retention_days",
        },
    )
    if not entries and not missing_labels:
        return None, []
    return _render_terraform_tfvars(entries, missing_labels), missing_labels


def _azure_ui_tfvars(
    module_config: Mapping[str, Any],
    *,
    field_labels: Mapping[str, str],
    required_fields: Iterable[str],
    selected: Mapping[str, str],
) -> Tuple[str | None, List[str]]:
    """Return rendered Azure ``terraform.tfvars`` content for UI deployments."""

    data = {str(key): value for key, value in module_config.items()}
    field_map = {
        "subscription_id": "subscription_id",
        "resource_group_name": "resource_group_name",
        "location": "location",
        "container_registry": "container_registry",
        "container_registry_username": "container_registry_username",
        "container_registry_password": "container_registry_password",
        "image_tag": "image_tag",
        "container_app_environment_name": "container_app_environment_name",
        "container_app_name": "container_app_name",
        "ingress_port": "ingress_port",
        "min_replicas": "min_replicas",
        "max_replicas": "max_replicas",
        "container_cpu": "container_cpu",
        "container_memory": "container_memory",
        "tags": "tags",
    }
    entries, missing_labels = _collect_tfvars_entries(
        data,
        field_map=field_map,
        field_labels=field_labels,
        required_fields=required_fields,
        numeric_fields={"ingress_port", "min_replicas", "max_replicas"},
        map_fields={"tags"},
    )
    if not entries and not missing_labels:
        return None, []
    return _render_terraform_tfvars(entries, missing_labels), missing_labels


def _azure_governance_tfvars(
    module_config: Mapping[str, Any],
    *,
    field_labels: Mapping[str, str],
    required_fields: Iterable[str],
    selected: Mapping[str, str],
) -> Tuple[str | None, List[str]]:
    """Return rendered Azure ``terraform.tfvars`` content and missing labels for governance deployments."""

    data = {str(key): value for key, value in module_config.items()}
    contract_backend = selected.get("contracts_backend", "").strip().lower()
    if not _clean_str(data.get("contract_store_mode")):
        data["contract_store_mode"] = "sql" if contract_backend == "sql" else "filesystem"

    field_map = {
        "subscription_id": "subscription_id",
        "resource_group_name": "resource_group_name",
        "location": "location",
        "container_registry": "container_registry",
        "container_registry_username": "container_registry_username",
        "container_registry_password": "container_registry_password",
        "image_tag": "image_tag",
        "backend_token": "backend_token",
        "contract_store_mode": "contract_store_mode",
        "contract_storage": "contract_storage",
        "contract_share_name": "contract_share_name",
        "contract_share_quota_gb": "contract_share_quota_gb",
        "contract_store_dsn": "contract_store_dsn",
        "contract_store_table": "contract_store_table",
        "contract_store_schema": "contract_store_schema",
        "container_app_environment_name": "container_app_environment_name",
        "container_app_name": "container_app_name",
        "ingress_port": "ingress_port",
        "min_replicas": "min_replicas",
        "max_replicas": "max_replicas",
        "container_cpu": "container_cpu",
        "container_memory": "container_memory",
        "tags": "tags",
    }
    entries, missing_labels = _collect_tfvars_entries(
        data,
        field_map=field_map,
        field_labels=field_labels,
        required_fields=required_fields,
        numeric_fields={"ingress_port", "min_replicas", "max_replicas", "contract_share_quota_gb"},
        map_fields={"tags"},
    )
    if not entries and not missing_labels:
        return None, []
    return _render_terraform_tfvars(entries, missing_labels), missing_labels


def _terraform_bundle_files(state: Mapping[str, Any]) -> List[Tuple[str, str]]:
    """Return additional Terraform files for the setup bundle."""

    selected_raw = state.get("selected_options") if isinstance(state, Mapping) else {}
    configuration_raw = state.get("configuration") if isinstance(state, Mapping) else {}
    if not isinstance(selected_raw, Mapping) or not isinstance(configuration_raw, Mapping):
        return []

    selected = {str(key): str(value) for key, value in selected_raw.items()}
    configuration: Dict[str, Mapping[str, Any]] = {
        str(key): value
        for key, value in configuration_raw.items()
        if isinstance(value, Mapping)
    }

    bundle_plan = {
        "governance_deployment": {
            "slug": "governance",
            "options": {
                "aws_terraform": {
                    "provider": "aws",
                    "builder": _aws_governance_tfvars,
                    "template_dir": TERRAFORM_TEMPLATE_ROOT / "aws-service-backend",
                },
                "azure_terraform": {
                    "provider": "azure",
                    "builder": _azure_governance_tfvars,
                    "template_dir": TERRAFORM_TEMPLATE_ROOT / "azure-service-backend",
                },
            },
        },
        "ui_deployment": {
            "slug": "ui",
            "options": {
                "aws_terraform": {
                    "provider": "aws",
                    "builder": _aws_ui_tfvars,
                    "template_dir": TERRAFORM_TEMPLATE_ROOT / "aws-contracts-app",
                },
                "azure_terraform": {
                    "provider": "azure",
                    "builder": _azure_ui_tfvars,
                    "template_dir": TERRAFORM_TEMPLATE_ROOT / "azure-contracts-app",
                },
            },
        },
    }

    files: List[Tuple[str, str]] = []

    for module_key, module_plan in bundle_plan.items():
        option_key = selected.get(module_key)
        if not option_key:
            continue
        option_plan = module_plan["options"].get(option_key)
        if not option_plan:
            continue

        module_meta = SETUP_MODULES.get(module_key)
        if not module_meta:
            continue
        option_meta = module_meta["options"].get(option_key)
        if not option_meta:
            continue

        field_labels: Dict[str, str] = {}
        required_fields: List[str] = []
        for field_meta in option_meta.get("fields", []):
            name = str(field_meta.get("name") or "")
            if not name:
                continue
            field_labels[name] = str(field_meta.get("label") or name)
            if not field_meta.get("optional"):
                required_fields.append(name)

        module_config = configuration.get(module_key, {})
        if not isinstance(module_config, Mapping):
            module_config = {}

        builder = option_plan.get("builder")
        provider = option_plan.get("provider")
        template_dir = option_plan.get("template_dir")
        if not provider or not builder:
            continue

        tfvars_text, missing = builder(
            module_config,
            field_labels=field_labels,
            required_fields=required_fields,
            selected=selected,
        )

        template_files = _terraform_template_files(
            provider,
            target=module_plan.get("slug", module_key),
            template_dir=template_dir if isinstance(template_dir, Path) else None,
        )
        files.extend(template_files)

        prefix = f"dc43-setup/terraform/{module_plan.get('slug', module_key)}/{provider}"
        if tfvars_text:
            files.append((f"{prefix}/terraform.tfvars", tfvars_text))
        elif missing:
            stub = _render_terraform_tfvars([], missing)
            files.append((f"{prefix}/terraform.tfvars", stub))

    return files


def _service_backends_config_from_state(
    state: Mapping[str, Any],
) -> ServiceBackendsConfig | None:
    """Return a :class:`ServiceBackendsConfig` built from the wizard state."""

    selected_raw = state.get("selected_options") if isinstance(state, Mapping) else {}
    configuration_raw = state.get("configuration") if isinstance(state, Mapping) else {}
    if not isinstance(selected_raw, Mapping):
        return None

    selected = {str(key): str(value) for key, value in selected_raw.items()}
    configuration: Dict[str, Mapping[str, Any]] = {}
    if isinstance(configuration_raw, Mapping):
        configuration = {
            str(key): value
            for key, value in configuration_raw.items()
            if isinstance(value, Mapping)
        }

    if not selected:
        return None

    def module_config(name: str) -> Mapping[str, Any]:
        module = configuration.get(name, {})
        return module if isinstance(module, Mapping) else {}

    def path_from(value: Any) -> Path | None:
        text = _clean_str(value)
        if not text:
            return None
        try:
            return Path(text)
        except (TypeError, ValueError):  # pragma: no cover - safety
            return None

    contract_option = selected.get("contracts_backend")
    contract_cfg = BackendContractStoreConfig()
    if contract_option:
        option = contract_option.strip().lower()
        module = module_config("contracts_backend")
        if option == "filesystem":
            contract_cfg.type = "filesystem"
            root = (
                module.get("contracts_dir")
                or module.get("work_dir")
                or module.get("storage_path")
            )
            contract_cfg.root = path_from(root)
        elif option == "sql":
            contract_cfg.type = "sql"
            contract_cfg.dsn = _clean_str(module.get("connection_uri"))
            contract_cfg.schema = _clean_str(module.get("schema"))
        elif option == "delta_lake":
            contract_cfg.type = "delta"
            contract_cfg.base_path = path_from(module.get("storage_path"))
            contract_cfg.schema = _clean_str(module.get("schema"))
        elif option == "collibra":
            contract_cfg.type = "collibra_http"
            contract_cfg.base_url = _clean_str(module.get("base_url"))

    product_option = selected.get("products_backend")
    product_cfg = BackendDataProductStoreConfig()
    if product_option:
        option = product_option.strip().lower()
        module = module_config("products_backend")
        if option == "filesystem":
            product_cfg.type = "filesystem"
            root = module.get("products_dir") or module.get("storage_path")
            product_cfg.root = path_from(root)
        elif option == "sql":
            product_cfg.type = "sql"
            product_cfg.dsn = _clean_str(module.get("connection_uri"))
            product_cfg.schema = _clean_str(module.get("schema"))
        elif option == "delta_lake":
            product_cfg.type = "delta"
            product_cfg.base_path = path_from(module.get("storage_path"))
            product_cfg.schema = _clean_str(module.get("schema"))
            product_cfg.catalog = _clean_str(module.get("catalog"))
        elif option == "collibra":
            product_cfg.type = "collibra_stub"
            product_cfg.base_url = _clean_str(module.get("base_url"))

    governance_option = selected.get("governance_extensions")
    unity_cfg = BackendUnityCatalogConfig()
    governance_builders: tuple[str, ...] = ()
    if governance_option == "unity_catalog":
        module = module_config("governance_extensions")
        unity_cfg.enabled = True
        unity_cfg.workspace_host = _clean_str(module.get("workspace_url"))
        unity_cfg.workspace_token = _clean_str(module.get("token"))
        catalog_value = _clean_str(module.get("catalog"))
        schema_value = _clean_str(module.get("schema"))
        notes: dict[str, str] = {}
        if catalog_value:
            notes["catalog"] = catalog_value
        if schema_value:
            notes["schema"] = schema_value
        if notes:
            unity_cfg.static_properties = notes
    elif governance_option == "custom_module":
        module = module_config("governance_extensions")
        module_path = _clean_str(module.get("module_path"))
        if module_path:
            governance_builders = (module_path,)

    auth_cfg = BackendAuthConfig()
    if selected.get("governance_service") == "remote_api":
        module = module_config("governance_service")
        auth_cfg.token = _clean_str(module.get("api_token"))

    config = ServiceBackendsConfig(
        contract_store=contract_cfg,
        data_product_store=product_cfg,
        auth=auth_cfg,
        unity_catalog=unity_cfg,
        governance=BackendGovernanceConfig(
            dataset_contract_link_builders=governance_builders,
        ),
    )

    return config


def _service_backends_toml(state: Mapping[str, Any]) -> str | None:
    """Render the dc43-service-backends TOML using the wizard selections."""

    config = _service_backends_config_from_state(state)
    if config is None:
        return None
    toml_text = dump_service_backends_config(config)
    return toml_text or None


def _contracts_app_config_from_state(
    state: Mapping[str, Any],
) -> ContractsAppConfig | None:
    """Return a :class:`ContractsAppConfig` derived from wizard selections."""

    configuration_raw = state.get("configuration") if isinstance(state, Mapping) else {}
    selected_raw = state.get("selected_options") if isinstance(state, Mapping) else {}

    if not isinstance(configuration_raw, Mapping):
        configuration_raw = {}
    if not isinstance(selected_raw, Mapping):
        selected_raw = {}

    configuration: Dict[str, Mapping[str, Any]] = {
        str(key): value
        for key, value in configuration_raw.items()
        if isinstance(value, Mapping)
    }
    selected = {str(key): str(value) for key, value in selected_raw.items()}

    workspace_module = configuration.get("contracts_backend", {})
    work_dir = _clean_str(workspace_module.get("work_dir"))
    workspace_cfg = WorkspaceConfig(root=Path(work_dir) if work_dir else None)

    backend_mode = "embedded"
    backend_base_url: str | None = None
    governance_config = configuration.get("governance_service", {})
    if selected.get("governance_service") == "remote_api":
        backend_mode = "remote"
        backend_base_url = _clean_str(governance_config.get("base_url"))

    backend_cfg = BackendConfig(
        mode="remote" if backend_mode == "remote" else "embedded",
        base_url=backend_base_url,
        process=BackendProcessConfig(),
    )

    return ContractsAppConfig(workspace=workspace_cfg, backend=backend_cfg)


def _contracts_app_toml(state: Mapping[str, Any]) -> str | None:
    """Render the dc43-contracts-app TOML derived from the wizard configuration."""

    config = _contracts_app_config_from_state(state)
    if config is None:
        return None
    toml_text = dump_contracts_app_config(config)
    return toml_text or None


def _pipeline_bootstrap_script() -> str:
    """Return the helper script that loads the generated configuration."""

    return textwrap.dedent(
        """\
        #!/usr/bin/env python3
        '''Bootstrap dc43 service backends for pipeline orchestration.

        This script loads the generated dc43-service-backends TOML file and
        instantiates the contract and data product backends. Replace the
        placeholder print statements with the logic that wires the backends
        into your orchestration framework.
        '''

        from pathlib import Path

        from dc43_service_backends.bootstrap import build_backends
        from dc43_service_backends.config import load_config


        def main() -> None:
            bundle_root = Path(__file__).resolve().parent.parent
            config_path = bundle_root / "config" / "dc43-service-backends.toml"
            config = load_config(config_path)
            suite = build_backends(config)
            contract_backend, data_product_backend = suite
            dq_backend = suite.data_quality

            print(
                "Contract backend initialised:",
                contract_backend.__class__.__name__,
            )
            print(
                "Data product backend initialised:",
                data_product_backend.__class__.__name__,
            )
            print(
                "Data-quality backend initialised:",
                dq_backend.__class__.__name__,
            )
            print(
                "Integrate these instances into your pipeline or notebooks to"
                " publish contracts, data products, and validation payloads programmatically.",
            )


        if __name__ == "__main__":
            main()
        """
        )


def _start_stack_script() -> str:
    """Return a helper script that launches the local UI and backend."""

    return textwrap.dedent(
        """\
        #!/usr/bin/env python3
        '''Start the dc43 UI and backend using the exported configuration.'''

        import argparse
        import os
        import subprocess
        import sys
        import time
        from pathlib import Path
        from urllib.error import URLError
        from urllib.request import urlopen

        from dc43_contracts_app.config import load_config as load_contracts_config
        from dc43_service_backends.config import load_config as load_service_config


        def _wait_for(url: str, timeout: float = 30.0) -> None:
            probe = url.rstrip("/") + "/openapi.json"
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                try:
                    with urlopen(probe, timeout=2):
                        return
                except URLError:
                    time.sleep(0.25)
            raise RuntimeError(f"Service at {url} failed to start within {timeout}s")


        def main() -> None:
            parser = argparse.ArgumentParser(
                description="Launch the dc43 stack using exported configuration files.",
            )
            parser.add_argument(
                "--ui-host",
                default="127.0.0.1",
                help="Host interface for the contracts UI (default: 127.0.0.1)",
            )
            parser.add_argument(
                "--ui-port",
                type=int,
                default=8000,
                help="Port for the contracts UI (default: 8000)",
            )
            parser.add_argument(
                "--wait-timeout",
                type=float,
                default=45.0,
                help="Seconds to wait for the backend API before aborting (default: 45)",
            )
            args = parser.parse_args()

            bundle_root = Path(__file__).resolve().parent.parent
            config_dir = bundle_root / "config"
            contracts_config_path = config_dir / "dc43-contracts-app.toml"
            service_config_path = config_dir / "dc43-service-backends.toml"

            contracts_config = load_contracts_config(contracts_config_path)
            # Ensure the backends configuration file exists even if we do not inspect it yet.
            load_service_config(service_config_path)

            backend_cfg = contracts_config.backend
            process_cfg = backend_cfg.process
            backend_host = process_cfg.host
            backend_port = process_cfg.port
            backend_url = backend_cfg.base_url or process_cfg.url()

            backend_env = os.environ.copy()
            backend_env.setdefault("DC43_SERVICE_BACKENDS_CONFIG", str(service_config_path))

            backend_proc = None
            try:
                if backend_cfg.mode != "remote":
                    backend_cmd = [
                        sys.executable,
                        "-m",
                        "uvicorn",
                        "dc43_service_backends.webapp:app",
                        "--host",
                        backend_host,
                        "--port",
                        str(backend_port),
                    ]
                    if process_cfg.log_level:
                        backend_cmd.extend(["--log-level", process_cfg.log_level])

                    print(
                        f"Starting service backends at http://{backend_host}:{backend_port} using {service_config_path}..."
                    )
                    backend_proc = subprocess.Popen(backend_cmd, env=backend_env)
                    try:
                        _wait_for(backend_url, timeout=args.wait_timeout)
                    except Exception:
                        backend_proc.terminate()
                        try:
                            backend_proc.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            backend_proc.kill()
                        raise
                else:
                    print(
                        "Contracts app configured for remote governance; skipping local backend startup."
                    )

                ui_env = os.environ.copy()
                ui_env.setdefault("DC43_CONTRACTS_APP_CONFIG", str(contracts_config_path))

                ui_cmd = [
                    sys.executable,
                    "-m",
                    "uvicorn",
                    "dc43_contracts_app.server:app",
                    "--host",
                    args.ui_host,
                    "--port",
                    str(args.ui_port),
                ]

                print(
                    f"Starting contracts UI on http://{args.ui_host}:{args.ui_port} using {contracts_config_path}..."
                )
                subprocess.run(ui_cmd, check=True, env=ui_env)
            except KeyboardInterrupt:
                pass
            finally:
                if backend_proc is not None:
                    backend_proc.terminate()
                    try:
                        backend_proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        backend_proc.kill()


        if __name__ == "__main__":
            main()
        """
    )


def _setup_bundle_readme(payload: Mapping[str, Any]) -> str:
    """Return README text for the setup export archive."""

    generated_at = payload.get("generated_at") or datetime.utcnow().isoformat() + "Z"
    lines = [
        "# DC43 setup bundle",
        "",
        "This archive was generated by the environment setup wizard.",
        f"Generated at: {generated_at}",
        "",
        "Files included:",
        "- configuration.json — complete summary of module selections and settings.",
        "- modules/<module>.json — per-module configuration stubs for automation.",
        "- config/dc43-service-backends.toml — drop-in configuration for the backend services.",
        "- config/dc43-contracts-app.toml — configuration for the web interface.",
        "- scripts/bootstrap_pipeline.py — helper to load the configuration from pipelines.",
        "- scripts/run_local_stack.py — start the local UI and backend services for quick testing.",
        "- terraform/governance/<provider>/ — Terraform templates and generated variables for governance deployments (when selected).",
        "- terraform/ui/<provider>/ — Terraform variable stubs for UI hosting (when selected).",
        "",
        "How to use:",
        "1. Copy the TOML files into your deployment repository or configuration management system.",
        "2. Update any commented placeholders (for example Unity Catalog tables or secrets).",
        "3. Execute `scripts/run_local_stack.py` to launch the local stack or `scripts/bootstrap_pipeline.py` inside orchestration jobs.",
        "4. Follow the installation and configuration notes in each module to finish provisioning.",
        "",
        "Modules exported:",
    ]

    modules = payload.get("modules") if isinstance(payload, Mapping) else []
    if isinstance(modules, Iterable):
        for module in modules:
            if not isinstance(module, Mapping):
                continue
            title = module.get("title") or module.get("key") or "module"
            option_label = module.get("option_label") or module.get("option") or "selection"
            lines.append(f"- {title} — {option_label}")

    lines.extend(
        [
            "",
            "You can revisit /setup to regenerate this bundle after making different selections.",
        ]
    )
    return "\n".join(lines) + "\n"


def _build_setup_bundle(state: Mapping[str, Any]) -> Tuple[io.BytesIO, Dict[str, Any]]:
    """Return a ZIP archive stream and payload for the current wizard state."""

    payload = _setup_export_payload(state)
    modules = payload.get("modules")
    if not modules:
        raise ValueError("No modules available to export.")

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("dc43-setup/README.md", _setup_bundle_readme(payload))
        archive.writestr(
            "dc43-setup/configuration.json",
            json.dumps(payload, indent=2, sort_keys=True),
        )
        for module in modules:
            if not isinstance(module, Mapping):
                continue
            module_key = str(module.get("key") or "module").replace("/", "-")
            archive.writestr(
                f"dc43-setup/modules/{module_key}.json",
                json.dumps(module, indent=2, sort_keys=True),
            )

        service_backends_toml = _service_backends_toml(state)
        if service_backends_toml:
            archive.writestr(
                "dc43-setup/config/dc43-service-backends.toml",
                service_backends_toml,
            )

        contracts_app_toml = _contracts_app_toml(state)
        if contracts_app_toml:
            archive.writestr(
                "dc43-setup/config/dc43-contracts-app.toml",
                contracts_app_toml,
            )

        bootstrap_script = _pipeline_bootstrap_script()
        script_info = zipfile.ZipInfo("dc43-setup/scripts/bootstrap_pipeline.py")
        script_info.external_attr = 0o755 << 16  # Mark the script as executable.
        archive.writestr(script_info, bootstrap_script)

        start_script = _start_stack_script()
        start_info = zipfile.ZipInfo("dc43-setup/scripts/run_local_stack.py")
        start_info.external_attr = 0o755 << 16
        archive.writestr(start_info, start_script)

        for relative_path, content in _terraform_bundle_files(state):
            archive.writestr(relative_path, content)

    buffer.seek(0)
    return buffer, payload


def _build_setup_context(
    request: Request,
    state: Mapping[str, Any],
    *,
    step: Optional[int] = None,
    errors: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Return the template context shared by onboarding views."""

    selected_options_raw = state.get("selected_options") if isinstance(state, Mapping) else {}
    raw_selected_options: Dict[str, str] = {}
    if isinstance(selected_options_raw, Mapping):
        raw_selected_options = {str(key): str(value) for key, value in selected_options_raw.items()}

    configuration_raw = state.get("configuration") if isinstance(state, Mapping) else {}
    configuration: Dict[str, Any] = {}
    if isinstance(configuration_raw, Mapping):
        configuration = {str(key): value for key, value in configuration_raw.items()}

    module_order = list(SETUP_MODULES.keys())
    resolved_selected: Dict[str, str] = dict(raw_selected_options)
    auto_selected: Set[str] = set()
    for module_key in module_order:
        visible_options = _module_visible_options(module_key, resolved_selected)
        current_value = resolved_selected.get(module_key)
        if current_value and visible_options and current_value not in visible_options:
            current_value = None
        auto_choice = False
        if not current_value and visible_options:
            default_option = _module_default_option(module_key, resolved_selected)
            if default_option and (not visible_options or default_option in visible_options):
                current_value = default_option
                if raw_selected_options.get(module_key) != default_option:
                    auto_choice = True
        if current_value:
            resolved_selected[module_key] = current_value
            if auto_choice:
                auto_selected.add(module_key)
            elif module_key in auto_selected and raw_selected_options.get(module_key) == current_value:
                auto_selected.discard(module_key)
        else:
            resolved_selected.pop(module_key, None)
            auto_selected.discard(module_key)

    selected_options = resolved_selected

    requested_step = int(state.get("current_step") or 1) if step is None else int(step)
    if requested_step > 1 and not raw_selected_options:
        requested_step = 1

    if requested_step >= _SETUP_TOTAL_STEPS and not state.get("completed"):
        if _requires_configuration(selected_options, configuration):
            requested_step = 2

    if state.get("completed") and step is None:
        requested_step = _SETUP_TOTAL_STEPS

    workspace = current_workspace()

    module_to_group: Dict[str, str] = {}
    for group_meta in SETUP_MODULE_GROUPS:
        for group_module in group_meta.get("modules", []):
            module_to_group[str(group_module)] = group_meta["key"]

    modules: List[Dict[str, Any]] = []
    modules_by_key: Dict[str, Dict[str, Any]] = {}
    modules_metadata: Dict[str, Any] = {}
    for module_key in module_order:
        module_meta = SETUP_MODULES[module_key]
        visible_options = _module_visible_options(module_key, selected_options)
        module_hidden = _module_should_hide(module_key, selected_options) or (
            module_meta.get("depends_on") and not visible_options
        )

        option_items: Iterable[Tuple[str, Mapping[str, Any]]] = module_meta["options"].items()
        if visible_options:
            option_items = [
                (key, module_meta["options"][key])
                for key in visible_options
                if key in module_meta["options"]
            ]

        options: List[Dict[str, Any]] = []
        safe_options: Dict[str, Any] = {}
        for option_key, option_meta in option_items:
            options.append(
                {
                    "key": option_key,
                    "label": option_meta.get("label"),
                    "description": option_meta.get("description", ""),
                    "selected": selected_options.get(module_key) == option_key,
                }
            )
            safe_fields: List[Dict[str, Any]] = []
            for field_meta in option_meta.get("fields", []):
                field_name = field_meta.get("name")
                if not field_name:
                    continue
                safe_fields.append(
                    {
                        "name": field_name,
                        "label": field_meta.get("label"),
                        "optional": bool(field_meta.get("optional")),
                    }
                )
            safe_option: Dict[str, Any] = {
                "label": option_meta.get("label"),
                "description": option_meta.get("description", ""),
                "fields": safe_fields,
                "skip_configuration": bool(option_meta.get("skip_configuration")),
            }
            diagram_meta = option_meta.get("diagram")
            if isinstance(diagram_meta, Mapping):
                serialised_diagram = _serialise_diagram(diagram_meta)
                if serialised_diagram:
                    safe_option["diagram"] = serialised_diagram
            safe_options[option_key] = safe_option

        selected_value = selected_options.get(module_key)
        requires_choice = len(visible_options) > 1 and not module_hidden
        module_missing = requires_choice and (
            not selected_value or selected_value not in visible_options
        )
        if (
            not module_missing
            and module_key in auto_selected
            and not module_hidden
            and len(visible_options) > 1
        ):
            selected_option_meta = module_meta["options"].get(selected_value) if selected_value else None
            skip_auto = bool(selected_option_meta and selected_option_meta.get("skip_configuration"))
            if not skip_auto:
                module_missing = True

        module_payload = {
            "key": module_key,
            "title": module_meta.get("title"),
            "summary": module_meta.get("summary", ""),
            "options": options,
            "group_key": module_to_group.get(module_key),
            "hidden": module_hidden,
            "missing_selection": module_missing,
            "visible_options": visible_options,
            "auto_selected": module_key in auto_selected,
        }
        modules.append(module_payload)
        modules_by_key[module_key] = module_payload
        modules_metadata[module_key] = {
            "title": module_meta.get("title"),
            "summary": module_meta.get("summary", ""),
            "group": module_to_group.get(module_key),
            "options": safe_options,
            "depends_on": module_meta.get("depends_on"),
            "visible_when": module_meta.get("visible_when"),
            "hide_when": module_meta.get("hide_when"),
            "default_for": module_meta.get("default_for"),
            "default_option": module_meta.get("default_option"),
            "hidden": module_hidden,
            "auto_selected": module_key in auto_selected,
        }

    module_groups: List[Dict[str, Any]] = []
    grouped_keys: List[str] = []
    for group_meta in SETUP_MODULE_GROUPS:
        group_module_keys = [key for key in group_meta.get("modules", []) if key in modules_by_key]
        if not group_module_keys:
            continue
        group_modules = [modules_by_key[key] for key in group_module_keys]
        visible_count = sum(1 for module in group_modules if not module.get("hidden"))
        missing_count = sum(1 for module in group_modules if module.get("missing_selection"))
        module_groups.append(
            {
                "key": group_meta["key"],
                "title": group_meta.get("title"),
                "summary": group_meta.get("summary", ""),
                "modules": group_modules,
                "module_keys": group_module_keys,
                "visible_count": visible_count,
                "missing_count": missing_count,
                "total_count": len(group_module_keys),
            }
        )
        grouped_keys.extend(group_module_keys)

    remaining_module_keys = [key for key in module_order if key not in grouped_keys]
    if remaining_module_keys:
        remaining_modules = [modules_by_key[key] for key in remaining_module_keys]
        module_groups.append(
            {
                "key": "additional_modules",
                "title": "Additional modules",
                "summary": "Options that are available outside the primary grouping.",
                "modules": remaining_modules,
                "module_keys": remaining_module_keys,
                "visible_count": sum(1 for module in remaining_modules if not module.get("hidden")),
                "missing_count": sum(1 for module in remaining_modules if module.get("missing_selection")),
                "total_count": len(remaining_module_keys),
            }
        )

    selected_modules: List[Dict[str, Any]] = []
    for module_key in module_order:
        option_key = selected_options.get(module_key)
        if not option_key:
            continue
        module_meta = SETUP_MODULES.get(module_key)
        if not module_meta:
            continue
        option_meta = module_meta["options"].get(option_key)
        if not option_meta:
            continue
        fields = [
            _serialise_field(module_key, field_meta, configuration=configuration, workspace=workspace)
            for field_meta in option_meta.get("fields", [])
        ]
        pending_required = 0
        for field in fields:
            if field.get("optional"):
                continue
            value = str(field.get("value", "") or "").strip()
            if not value:
                pending_required += 1
        selected_modules.append(
            {
                "key": module_key,
                "title": module_meta.get("title"),
                "summary": module_meta.get("summary", ""),
                "option_key": option_key,
                "option": {
                    "label": option_meta.get("label"),
                    "description": option_meta.get("description", ""),
                    "installation": list(option_meta.get("installation", [])),
                    "configuration_notes": list(option_meta.get("configuration_notes", [])),
                },
                "fields": fields,
                "group_key": module_to_group.get(module_key),
                "skip_configuration": bool(option_meta.get("skip_configuration")),
                "pending_required": pending_required,
            }
        )

    selected_by_key = {module["key"]: module for module in selected_modules}
    selected_groups: List[Dict[str, Any]] = []
    seen_selected: Set[str] = set()
    for group in module_groups:
        group_selected: List[Dict[str, Any]] = []
        for key in group.get("module_keys", []):
            if key not in selected_by_key:
                continue
            seen_selected.add(key)
            module_entry = selected_by_key[key]
            if module_entry.get("skip_configuration"):
                continue
            module_entry["has_pending"] = module_entry.get("pending_required", 0) > 0
            group_selected.append(module_entry)
        if not group_selected:
            continue
        pending_total = sum(module.get("pending_required", 0) for module in group_selected)
        selected_groups.append(
            {
                "key": group["key"],
                "title": group.get("title"),
                "summary": group.get("summary", ""),
                "modules": group_selected,
                "module_keys": [module["key"] for module in group_selected],
                "pending_required": pending_total,
            }
        )

    remaining_selected = [
        module
        for module in selected_modules
        if module["key"] not in seen_selected
    ]
    if remaining_selected:
        for module in remaining_selected:
            module["has_pending"] = module.get("pending_required", 0) > 0
        selected_groups.append(
            {
                "key": "additional_modules",
                "title": "Additional modules",
                "summary": "Options that are available outside the primary grouping.",
                "modules": remaining_selected,
                "module_keys": [module["key"] for module in remaining_selected],
                "pending_required": sum(module.get("pending_required", 0) for module in remaining_selected),
            }
        )

    for group_index, group in enumerate(selected_groups, start=1):
        group["display_index"] = group_index
        modules = group.get("modules", [])
        for module_index, module in enumerate(modules, start=1):
            module["display_index"] = f"{group_index}.{module_index}"

    summary_modules_map: Dict[str, Dict[str, Any]] = {}
    for module in selected_modules:
        module_key = module["key"]
        module_config = configuration.get(module_key, {}) if isinstance(configuration, Mapping) else {}
        summary_fields: List[Dict[str, Any]] = []
        for field in module.get("fields", []):
            summary_fields.append(
                {
                    "label": field.get("label"),
                    "value": str(module_config.get(field.get("name"), "") or field.get("value", "")),
                    "optional": field.get("optional", False),
                }
            )
        summary_modules_map[module_key] = {
            "key": module_key,
            "title": module.get("title"),
            "option_label": module["option"].get("label") if isinstance(module.get("option"), Mapping) else "",
            "installation": module.get("option", {}).get("installation", []) if isinstance(module.get("option"), Mapping) else [],
            "configuration_notes": module.get("option", {}).get("configuration_notes", []) if isinstance(module.get("option"), Mapping) else [],
            "fields": summary_fields,
        }

    summary_groups: List[Dict[str, Any]] = []
    seen_summary: List[str] = []
    for group in module_groups:
        group_summary = [
            summary_modules_map[key]
            for key in group.get("module_keys", [])
            if key in summary_modules_map
        ]
        if not group_summary:
            continue
        summary_groups.append(
            {
                "key": group["key"],
                "title": group.get("title"),
                "summary": group.get("summary", ""),
                "modules": group_summary,
            }
        )
        seen_summary.extend(item["key"] for item in group_summary)

    remaining_summary = [
        summary
        for summary in summary_modules_map.values()
        if summary["key"] not in seen_summary
    ]
    if remaining_summary:
        summary_groups.append(
            {
                "key": "additional_modules",
                "title": "Additional modules",
                "summary": "Options that are available outside the primary grouping.",
                "modules": remaining_summary,
            }
        )

    summary_modules: List[Dict[str, Any]] = [
        summary_modules_map[key]
        for key in module_order
        if key in summary_modules_map
    ]

    safe_selected: Dict[str, str] = {}
    for module_key, option_key in selected_options.items():
        if option_key is None:
            continue
        safe_selected[str(module_key)] = str(option_key)

    safe_configuration: Dict[str, Dict[str, Any]] = {}
    if isinstance(configuration, Mapping):
        for module_key, module_config in configuration.items():
            if not isinstance(module_config, Mapping):
                continue
            serialised_config: Dict[str, Any] = {}
            for field_name, value in module_config.items():
                if value in (None, ""):
                    continue
                if isinstance(value, (str, int, float, bool)):
                    serialised_config[str(field_name)] = value
                else:
                    serialised_config[str(field_name)] = str(value)
            if serialised_config:
                safe_configuration[str(module_key)] = serialised_config

    explicit_selected_keys: List[str] = []
    if isinstance(raw_selected_options, Mapping):
        seen_explicit: Set[str] = set()
        for module_key, option_key in raw_selected_options.items():
            if option_key is None:
                continue
            module_str = str(module_key)
            if module_str in seen_explicit:
                continue
            seen_explicit.add(module_str)
            explicit_selected_keys.append(module_str)
    explicit_selected_keys.sort()

    setup_state_payload = {
        "order": module_order,
        "selected": safe_selected,
        "configuration": safe_configuration,
        "modules": modules_metadata,
        "autoSelected": sorted(auto_selected),
        "explicitSelected": explicit_selected_keys,
        "groups": [
            {
                "key": group["key"],
                "title": group.get("title"),
                "summary": group.get("summary", ""),
                "modules": list(group.get("module_keys", [])),
            }
            for group in module_groups
        ],
    }

    configuration_ready = not _requires_configuration(selected_options, configuration)
    can_export_bundle = bool(selected_modules) and configuration_ready

    current_step = max(1, min(_SETUP_TOTAL_STEPS, requested_step))

    return {
        "request": request,
        "step": current_step,
        "progress": _setup_progress(current_step),
        "modules": modules,
        "module_groups": module_groups,
        "selected_modules": selected_modules,
        "selected_groups": selected_groups,
        "summary_modules": summary_modules,
        "summary_groups": summary_groups,
        "state": state,
        "errors": errors or [],
        "completed": bool(state.get("completed")),
        "can_export_bundle": can_export_bundle,
        "module_order": module_order,
        "setup_state_payload": setup_state_payload,
    }


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


_CONTRACT_STATUS_BADGES: Dict[str, str] = {
    "active": "bg-success",
    "draft": "bg-warning text-dark",
    "deprecated": "bg-secondary",
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
    custom: Dict[str, Any] = custom_properties_dict(first)
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
        if "dc43.core.versioning" in custom:
            info["versioning"] = custom.get("dc43.core.versioning")
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
    for prop in normalise_custom_properties(contract.customProperties):
        if isinstance(prop, Mapping):
            key = prop.get("property")
            value = prop.get("value")
        else:
            key = getattr(prop, "property", None)
            value = getattr(prop, "value", None)
        if key != "draft_change_log":
            continue
        try:
            items = list(value or [])
        except TypeError:
            continue
        for item in items:
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
    if not DATASETS_FILE.exists():
        return []
    try:
        raw = json.loads(DATASETS_FILE.read_text())
    except (OSError, json.JSONDecodeError):
        return []
    return [DatasetRecord(**r) for r in raw]


def save_records(records: List[DatasetRecord]) -> None:
    DATASETS_FILE.parent.mkdir(parents=True, exist_ok=True)
    DATASETS_FILE.write_text(
        json.dumps([r.__dict__ for r in records], indent=2), encoding="utf-8"
    )


def _scenario_dataset_name(params: Mapping[str, Any]) -> str:
    """Return the expected output dataset for a scenario."""

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
    """Return scenario metadata enriched with the latest recorded run."""

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
        latest_record = dataset_records[-1] if dataset_records else None

        rows.append(
            {
                "key": key,
                "label": cfg.get("label", key.replace("-", " ").title()),
                "description": cfg.get("description"),
                "diagram": cfg.get("diagram"),
                "dataset_name": dataset_name,
                "contract_id": params.get("contract_id"),
                "contract_version": params.get("contract_version"),
                "run_type": params.get("run_type", "infer"),
                "run_count": len(dataset_records),
                "latest": latest_record.__dict__.copy() if latest_record else None,
            }
        )

    return rows


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


def _flatten_schema_entries(contract: OpenDataContractStandard) -> List[Dict[str, Any]]:
    """Return a flattened list of schema properties for UI displays."""

    entries: List[Dict[str, Any]] = []
    for obj in getattr(contract, "schema_", None) or []:
        object_name = str(getattr(obj, "name", "") or "")
        prefix = f"{object_name}." if object_name else ""
        for prop in getattr(obj, "properties", None) or []:
            field_name = str(getattr(prop, "name", "") or "")
            full_name = f"{prefix}{field_name}".strip(".")
            entries.append(
                {
                    "field": full_name,
                    "object": object_name,
                    "name": field_name,
                    "physicalType": getattr(prop, "physicalType", "") or "",
                    "logicalType": getattr(prop, "logicalType", "") or "",
                    "required": bool(getattr(prop, "required", False)),
                    "description": getattr(prop, "description", "") or "",
                    "businessName": getattr(prop, "businessName", "") or "",
                }
            )
    return entries


def _integration_catalog() -> List[Dict[str, Any]]:
    """Return basic metadata for all stored contracts."""

    catalog: List[Dict[str, Any]] = []
    for cid in sorted(store.list_contracts()):
        try:
            versions = store.list_versions(cid)
        except FileNotFoundError:
            continue
        sorted_versions = _sorted_versions(versions)
        if not sorted_versions:
            continue
        latest_contract: Optional[OpenDataContractStandard] = None
        for version in reversed(sorted_versions):
            try:
                latest_contract = store.get(cid, version)
                break
            except FileNotFoundError:
                continue
        description = ""
        status = ""
        name = ""
        if latest_contract is not None:
            name = getattr(latest_contract, "name", "") or ""
            if getattr(latest_contract, "description", None):
                description = getattr(latest_contract.description, "usage", "") or ""
            status = getattr(latest_contract, "status", "") or ""
        catalog.append(
            {
                "id": cid,
                "name": name or cid,
                "description": description,
                "versions": sorted_versions,
                "latestVersion": sorted_versions[-1],
                "status": status,
            }
        )
    return catalog


@dataclass
class IntegrationContractContext:
    """Container storing contract objects alongside serialized metadata."""

    contract: OpenDataContractStandard
    summary: Dict[str, Any]


async def _load_integration_contract(cid: str, ver: str) -> IntegrationContractContext:
    """Return the contract and summary information for helper endpoints."""

    try:
        contract = store.get(cid, ver)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    expectations = await _expectation_predicates(contract)
    server_info = _server_details(contract)
    description = ""
    if getattr(contract, "description", None):
        description = getattr(contract.description, "usage", "") or ""
    schema_entries = _flatten_schema_entries(contract)
    summary: Dict[str, Any] = {
        "id": cid,
        "version": ver,
        "name": getattr(contract, "name", "") or cid,
        "description": description,
        "server": jsonable_encoder(server_info) if server_info else None,
        "expectations": expectations,
        "schemaEntries": schema_entries,
        "fieldCount": len(schema_entries),
        "datasetId": (server_info.get("dataset_id") if server_info else contract.id or cid),
    }
    return IntegrationContractContext(contract=contract, summary=summary)


def _normalise_selection(entries: Iterable[Mapping[str, Any]]) -> List[Dict[str, str]]:
    """Normalise payload selections into ``contract_id``/``version`` pairs."""

    result: List[Dict[str, str]] = []
    for entry in entries:
        if not isinstance(entry, Mapping):
            continue
        cid = entry.get("contract_id") or entry.get("contractId") or entry.get("id")
        ver = entry.get("version")
        if not cid or not ver:
            raise HTTPException(status_code=422, detail="contract_id and version are required")
        result.append({"contract_id": str(cid), "version": str(ver)})
    return result


_IDENTIFIER_SANITISER = re.compile(r"[^0-9A-Za-z_]")


def _sanitise_identifier(value: str, default: str) -> str:
    """Return a Python identifier derived from ``value``."""

    candidate = _IDENTIFIER_SANITISER.sub("_", value)
    candidate = re.sub(r"_+", "_", candidate).strip("_")
    if not candidate:
        candidate = default
    if candidate[0].isdigit():
        candidate = f"{default}_{candidate}"
    return candidate.lower()


def _summarise_predicates(expectations: Mapping[str, str]) -> str:
    """Return a human-friendly summary of SQL predicates."""

    if not expectations:
        return ""
    parts = [f"{key}: {value}" for key, value in expectations.items()]
    return textwrap.shorten("; ".join(parts), width=160, placeholder=" …")


def _normalise_read_strategy(payload: Mapping[str, Any] | None) -> Dict[str, Any]:
    """Validate and normalise the requested read strategy."""

    raw_mode = (payload or {}).get("mode")
    mode = str(raw_mode or "status").lower()
    if mode not in {"status", "strict"}:
        raise HTTPException(status_code=400, detail=f"Unsupported read strategy: {mode}")
    return {"mode": mode}


def _normalise_write_strategy(payload: Mapping[str, Any] | None) -> Dict[str, Any]:
    """Validate and normalise the requested write strategy."""

    data = dict(payload or {})
    mode_raw = data.get("mode")
    mode = str(mode_raw or "split").lower()
    if mode == "noop":
        return {"mode": "noop"}

    include_valid = bool(data.get("include_valid", True))
    include_reject = bool(data.get("include_reject", True))
    if not include_valid and not include_reject:
        include_valid = True
    if mode == "split":
        return {
            "mode": "split",
            "include_valid": include_valid,
            "include_reject": include_reject,
        }
    if mode == "strict":
        return {
            "mode": "strict",
            "include_valid": include_valid,
            "include_reject": include_reject,
            "fail_on_warnings": bool(data.get("fail_on_warnings", False)),
        }
    raise HTTPException(status_code=400, detail=f"Unsupported write strategy: {mode}")


def _spark_stub_for_selection(
    inputs: List[Dict[str, str]],
    outputs: List[Dict[str, str]],
    context_map: Mapping[Tuple[str, str], IntegrationContractContext],
    *,
    read_strategy: Mapping[str, Any],
    write_strategy: Mapping[str, Any],
) -> str:
    """Return a Spark pipeline stub tailored to the selected contracts."""

    read_mode = str(read_strategy.get("mode") or "status").lower()
    write_mode = str(write_strategy.get("mode") or "split").lower()

    violation_imports: List[str] = []
    if outputs:
        if write_mode == "noop":
            violation_imports.append("NoOpWriteViolationStrategy")
        else:
            violation_imports.append("SplitWriteViolationStrategy")
            if write_mode == "strict":
                violation_imports.append("StrictWriteViolationStrategy")

    lines: List[str] = [
        "from pyspark.sql import SparkSession",
        "from dc43_integrations.spark.io import read_with_contract, write_with_contract",
    ]
    lines.append(
        "# Contract status guardrails reject draft/deprecated contracts unless the strategies opt in."
    )
    if violation_imports:
        unique_violation_imports = ", ".join(dict.fromkeys(violation_imports))
        lines.append(
            "from dc43_integrations.spark.violation_strategy import "
            + unique_violation_imports
        )
    lines.extend(
        [
            "from dc43_service_clients.contracts.client.remote import RemoteContractServiceClient",
            "from dc43_service_clients.data_quality.client.remote import RemoteDataQualityServiceClient",
            "from dc43_service_clients.governance.client.remote import RemoteGovernanceServiceClient",
            "",
            "# Generated by the DC43 integration helper",
            'BASE_URL = "http://dc43-services"',
            "",
            "contract_client = RemoteContractServiceClient(base_url=BASE_URL)",
            "dq_client = RemoteDataQualityServiceClient(base_url=BASE_URL)",
            "governance_client = RemoteGovernanceServiceClient(base_url=BASE_URL)",
            "",
            'spark = SparkSession.builder.appName("dc43-pipeline").getOrCreate()',
        ]
    )

    if outputs:
        lines.append("")
        if write_mode == "noop":
            lines.extend(
                [
                    "# NoOpWriteViolationStrategy keeps writes in a single target dataset.",
                    "# Pass allowed_contract_statuses=(\"active\", \"draft\") to allow draft contracts in development.",
                    "write_strategy = NoOpWriteViolationStrategy()",
                ]
            )
        else:
            include_valid = bool(write_strategy.get("include_valid", True))
            include_reject = bool(write_strategy.get("include_reject", True))
            include_valid_flag = "True" if include_valid else "False"
            include_reject_flag = "True" if include_reject else "False"
            lines.extend(
                [
                    "# SplitWriteViolationStrategy routes rows based on the contract predicates.",
                    "split_strategy = SplitWriteViolationStrategy(",
                    '    valid_suffix="valid",',
                    '    reject_suffix="reject",',
                    f"    include_valid={include_valid_flag},",
                    f"    include_reject={include_reject_flag},",
                    ")",
                ]
            )
            if write_mode == "strict":
                fail_on_warnings = bool(write_strategy.get("fail_on_warnings", False))
                fail_flag = "True" if fail_on_warnings else "False"
                lines.extend(
                    [
                        "",
                        "# StrictWriteViolationStrategy escalates contract issues to failures.",
                        "write_strategy = StrictWriteViolationStrategy(",
                        "    base=split_strategy,",
                        f"    fail_on_warnings={fail_flag},",
                        ")",
                    ]
                )
            else:
                lines.extend(["", "write_strategy = split_strategy"])

    input_vars: List[str] = []
    for index, entry in enumerate(inputs, start=1):
        key = (entry["contract_id"], entry["version"])
        ctx = context_map[key]
        summary = ctx.summary
        server_raw = summary.get("server") or {}
        server = dict(server_raw) if isinstance(server_raw, Mapping) else {}
        location = server.get("path") or server.get("dataset")
        fmt = server.get("format")
        base_name = _sanitise_identifier(summary["id"], f"input{index}")
        df_var = f"{base_name}_df"
        status_var = f"{base_name}_status"
        input_vars.append(df_var)

        lines.extend(
            [
                "",
                f"# Input: {summary['id']} {summary['version']} ({summary['datasetId']})",
            ]
        )
        if location:
            lines.append(f"#   Location: {location}")
        if fmt:
            lines.append(f"#   Format: {fmt}")
        lines.extend(
            [
                f"{df_var}, {status_var} = read_with_contract(",
                "    spark,",
                f"    contract_id={summary['id']!r},",
                f"    expected_contract_version=\"=={summary['version']}\",",
                "    contract_service=contract_client,",
                "    data_quality_service=dq_client,",
            ]
        )
        if server.get("dataset"):
            lines.append(f"    table={server['dataset']!r},")
        lines.extend(
            [
                "    enforce=True,",
                "    auto_cast=True,",
                "    return_status=True,",
                "    # status_strategy=DefaultReadStatusStrategy(allowed_contract_statuses=(\"active\", \"draft\")),",
                ")",
                "",
            ]
        )
        if read_mode == "strict":
            lines.extend(
                [
                    f"if {status_var} and {status_var}.status != \"ok\":",
                    "    raise RuntimeError(",
                    f"        f\"{summary['id']} status: {{{status_var}.status}} {{{status_var}.reason or ''}}\"",
                    "    )",
                ]
            )
        else:
            lines.extend(
                [
                    f"if {status_var} and {status_var}.status != \"ok\":",
                    f"    print(\"{summary['id']} status:\", {status_var}.status, {status_var}.reason or \"\")",
                ]
            )

    if input_vars:
        primary_df = input_vars[0]
        lines.extend(
            [
                "",
                "# TODO: implement business logic for the loaded dataframes",
            ]
        )
        if len(input_vars) > 1:
            lines.append("# Available inputs: " + ", ".join(input_vars))
        lines.append(f"transformed_df = {primary_df}  # replace with your transformations")
    else:
        lines.extend(
            [
                "",
                "# TODO: create a dataframe that matches the output contract schema",
                "transformed_df = spark.createDataFrame([], schema=None)",
            ]
        )

    for index, entry in enumerate(outputs, start=1):
        key = (entry["contract_id"], entry["version"])
        ctx = context_map[key]
        summary = ctx.summary
        server_raw = summary.get("server") or {}
        server = dict(server_raw) if isinstance(server_raw, Mapping) else {}
        fmt = server.get("format")
        base_name = _sanitise_identifier(summary["id"], f"output{index}")
        validation_var = f"{base_name}_validation"
        status_var = f"{base_name}_status"
        location = server.get("path") or server.get("dataset")

        lines.extend(
            [
                "",
                f"# Output: {summary['id']} {summary['version']} ({summary['datasetId']})",
            ]
        )
        if location:
            lines.append(f"#   Location: {location}")
        if fmt:
            lines.append(f"#   Format: {fmt}")
        lines.extend(
            [
                f"{validation_var}, {status_var} = write_with_contract(",
                "    df=transformed_df,  # TODO: replace with dataframe for this output",
                f"    contract_id={summary['id']!r},",
                f"    expected_contract_version=\"=={summary['version']}\",",
                "    contract_service=contract_client,",
                "    data_quality_service=dq_client,",
                "    governance_service=governance_client,",
            ]
        )
        if server.get("dataset"):
            lines.append(f"    table={server['dataset']!r},")
        lines.extend(
            [
                "    violation_strategy=write_strategy,",
                "    return_status=True,",
                ")",
                "",
                f"if {status_var}:",
                f"    print(\"{summary['id']} governance status:\", {status_var}.status)",
                f"print(\"{summary['id']} write validation ok:\", {validation_var}.ok)",
            ]
        )

    return "\n".join(lines).rstrip() + "\n"


def _read_strategy_notes(
    selections: List[Dict[str, str]],
    context_map: Mapping[Tuple[str, str], IntegrationContractContext],
    strategy: Mapping[str, Any],
) -> List[Dict[str, str]]:
    """Describe how read strategies are applied for the helper UI."""

    mode = str(strategy.get("mode") or "status").lower()
    if mode == "strict":
        intro = (
            "read_with_contract(... return_status=True) enforces schema alignment and the stub "
            "raises a RuntimeError whenever validation verdicts are not OK."
        )
    else:
        intro = (
            "read_with_contract(... return_status=True) enforces schema alignment and logs non-OK "
            "statuses so orchestration can branch on data quality verdicts."
        )
    intro += " Non-active contract statuses raise unless the strategy explicitly allows them."
    notes: List[Dict[str, str]] = [
        {
            "title": "Contract-aware reads",
            "description": intro,
        }
    ]
    for entry in selections:
        key = (entry["contract_id"], entry["version"])
        ctx = context_map[key]
        summary = ctx.summary
        server = summary.get("server") or {}
        location = server.get("path") or server.get("dataset")
        location_clause = f"Source location: {location}." if location else ""
        predicate_summary = _summarise_predicates(summary.get("expectations") or {})
        if predicate_summary:
            predicate_clause = f"Valid if {predicate_summary}."
        else:
            predicate_clause = "Valid if the contract schema and recorded rules pass."
        action_clause = (
            "Validation failures raise RuntimeError so the pipeline stops."
            if mode == "strict"
            else "Validation verdicts are logged for orchestration decisions."
        )
        description = " ".join(
            part for part in (location_clause, predicate_clause, action_clause) if part
        )
        notes.append(
            {
                "title": f"{summary['id']} {summary['version']} read",
                "description": description,
            }
        )
    return notes


def _write_strategy_notes(
    selections: List[Dict[str, str]],
    context_map: Mapping[Tuple[str, str], IntegrationContractContext],
    strategy: Mapping[str, Any],
) -> List[Dict[str, str]]:
    """Describe write strategies recommended for the helper UI."""

    mode = str(strategy.get("mode") or "split").lower()
    include_valid = bool(strategy.get("include_valid", True))
    include_reject = bool(strategy.get("include_reject", True))
    fail_on_warnings = bool(strategy.get("fail_on_warnings", False))

    notes: List[Dict[str, str]] = [
        {
            "title": "Governance hand-off",
            "description": (
                "write_with_contract(... return_status=True) records validation results and relays "
                "dataset versions to the governance client so each pipeline run is traceable."
                " By default non-active contracts are rejected; extend the contract-status options"
                " on your chosen strategy when drafts should be allowed."
            ),
        }
    ]

    if mode == "noop":
        notes.append(
            {
                "title": "Primary dataset only",
                "description": (
                    "NoOpWriteViolationStrategy keeps all rows in the primary dataset while still "
                    "capturing validation metadata."
                ),
            }
        )
    else:
        if include_valid and include_reject:
            split_desc = (
                "SplitWriteViolationStrategy writes passing rows to '<dataset>::valid' and rejected "
                "rows to '<dataset>::reject', preserving failed samples for triage."
            )
        elif include_valid:
            split_desc = (
                "SplitWriteViolationStrategy emits '<dataset>::valid' while violations stay with the "
                "primary dataset for follow-up."
            )
        elif include_reject:
            split_desc = (
                "SplitWriteViolationStrategy routes violations to '<dataset>::reject' and keeps valid "
                "rows in the primary dataset."
            )
        else:
            split_desc = "SplitWriteViolationStrategy keeps the primary dataset intact."
        description = split_desc
        if mode == "strict":
            strict_clause = " StrictWriteViolationStrategy raises when validation is not OK."
            if fail_on_warnings:
                strict_clause += " Warnings are treated as failures."
            description += strict_clause
        notes.append(
            {
                "title": "Split rejected rows" if mode == "split" else "Split & fail on violations",
                "description": description,
            }
        )
    for entry in selections:
        key = (entry["contract_id"], entry["version"])
        ctx = context_map[key]
        summary = ctx.summary
        dataset = summary.get("datasetId") or summary["id"]
        predicate_summary = _summarise_predicates(summary.get("expectations") or {})
        location = summary.get("server", {}).get("path") or summary.get("server", {}).get("dataset")
        location_clause = f"Target location: {location}." if location else ""
        if predicate_summary:
            predicate_clause = f"Valid if {predicate_summary}."
        else:
            predicate_clause = "Valid if the contract schema passes."
        if mode == "noop":
            routing_clause = f"All rows remain in '{dataset}' while validation metadata is captured."
        else:
            valid_target = f"'{dataset}::valid'" if include_valid else None
            reject_target = f"'{dataset}::reject'" if include_reject else None
            if include_valid and include_reject:
                routing_clause = (
                    f"Rows meeting the predicates flow to {valid_target} while violations route to {reject_target}."
                )
            elif include_valid:
                routing_clause = (
                    f"Rows meeting the predicates flow to {valid_target}; violations stay with '{dataset}'."
                )
            elif include_reject:
                routing_clause = (
                    f"Violations route to {reject_target} while passing rows remain in '{dataset}'."
                )
            else:
                routing_clause = f"Rows remain in '{dataset}'."
        extra_clause = ""
        if mode == "strict":
            if fail_on_warnings:
                extra_clause = " Validation errors or warnings raise RuntimeError so the run stops."
            else:
                extra_clause = " Validation errors raise RuntimeError so the run stops."
        notes.append(
            {
                "title": f"{summary['id']} {summary['version']} write",
                "description": " ".join(
                    part
                    for part in (
                        location_clause,
                        predicate_clause,
                        routing_clause,
                        extra_clause.strip(),
                    )
                    if part
                ),
            }
        )
    return notes

@router.get("/api/contracts")
async def api_contracts() -> List[Dict[str, Any]]:
    return load_contract_meta()


@router.get("/api/contracts/{cid}/{ver}")
async def api_contract_detail(cid: str, ver: str) -> Dict[str, Any]:
    try:
        contract = store.get(cid, ver)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    datasets = [r.__dict__ for r in load_records() if r.contract_id == cid and r.contract_version == ver]
    expectations = await _expectation_predicates(contract)
    return {
        "contract": contract_to_dict(contract),
        "datasets": datasets,
        "expectations": expectations,
    }


@router.get("/api/contracts/{cid}/{ver}/preview")
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
        def _load_preview() -> tuple[list[Mapping[str, Any]], list[str]]:
            local_contract_service, local_dq_service, _ = _thread_service_clients()
            spark = _spark_session()
            locator = ContractVersionLocator(
                dataset_version=selected_version,
                dataset_id=effective_dataset_id,
            )
            df = read_with_contract(  # type: ignore[misc]
                spark,
                contract_id=cid,
                contract_service=local_contract_service,
                expected_contract_version=f"=={ver}",
                dataset_locator=locator,
                enforce=False,
                auto_cast=False,
                data_quality_service=local_dq_service,
                return_status=False,
            )
            rows_raw = [row.asDict(recursive=True) for row in df.limit(limit).collect()]
            return rows_raw, list(df.columns)

        rows_raw, columns = await run_in_threadpool(_load_preview)
        rows = jsonable_encoder(rows_raw)
    except Exception as exc:  # pragma: no cover - defensive guard for preview errors
        logger.exception(
            "Failed to load preview for %s@%s dataset %s version %s",
            cid,
            ver,
            effective_dataset_id,
            selected_version,
        )
        raise HTTPException(status_code=500, detail=f"Failed to load preview: {exc}")

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


@router.post("/api/contracts/{cid}/{ver}/validate")
async def api_validate_contract(cid: str, ver: str) -> Dict[str, str]:
    return {"status": "active"}


@router.get("/api/datasets")
async def api_datasets() -> List[Dict[str, Any]]:
    records = load_records()
    return [r.__dict__.copy() for r in records]


@router.get("/api/datasets/{dataset_version}")
async def api_dataset_detail(dataset_version: str) -> Dict[str, Any]:
    for r in load_records():
        if r.dataset_version == dataset_version:
            contract = store.get(r.contract_id, r.contract_version)
            return {
                "record": r.__dict__,
                "contract": contract_to_dict(contract),
                "expectations": await _expectation_predicates(contract),
            }
    raise HTTPException(status_code=404, detail="Dataset not found")


@router.get("/api/integration-helper/contracts")
async def api_integration_contracts() -> Dict[str, Any]:
    """Return catalog metadata for the integration helper UI."""

    return {"contracts": _integration_catalog()}


@router.get("/api/integration-helper/contracts/{cid}/{ver}")
async def api_integration_contract_detail(cid: str, ver: str) -> Dict[str, Any]:
    """Return contract details enriched for the integration helper."""

    context = await _load_integration_contract(cid, ver)
    return {
        "contract": contract_to_dict(context.contract),
        "summary": jsonable_encoder(context.summary),
    }


@router.post("/api/integration-helper/stub")
async def api_integration_stub(request: Request) -> Dict[str, Any]:
    """Return a generated stub and strategy notes for an integration selection."""

    payload = await request.json()
    integration = str(payload.get("integration") or "spark").lower()
    if integration != "spark":
        raise HTTPException(status_code=400, detail=f"Unsupported integration: {integration}")

    inputs = _normalise_selection(payload.get("inputs") or [])
    outputs = _normalise_selection(payload.get("outputs") or [])
    if not inputs:
        raise HTTPException(status_code=422, detail="At least one input contract is required")
    if not outputs:
        raise HTTPException(status_code=422, detail="At least one output contract is required")

    read_strategy = _normalise_read_strategy(payload.get("read_strategy") or {})
    write_strategy = _normalise_write_strategy(payload.get("write_strategy") or {})

    context_map: Dict[Tuple[str, str], IntegrationContractContext] = {}
    for entry in inputs + outputs:
        key = (entry["contract_id"], entry["version"])
        if key not in context_map:
            context_map[key] = await _load_integration_contract(*key)

    stub_text = _spark_stub_for_selection(
        inputs,
        outputs,
        context_map,
        read_strategy=read_strategy,
        write_strategy=write_strategy,
    )
    read_notes = _read_strategy_notes(inputs, context_map, read_strategy)
    write_notes = _write_strategy_notes(outputs, context_map, write_strategy)

    return {
        "integration": integration,
        "stub": stub_text,
        "strategies": {
            "read": read_notes,
            "write": write_notes,
        },
        "selected_strategies": {
            "read": read_strategy,
            "write": write_strategy,
        },
        "contracts": {
            "inputs": [
                jsonable_encoder(context_map[(item["contract_id"], item["version"])].summary)
                for item in inputs
            ],
            "outputs": [
                jsonable_encoder(context_map[(item["contract_id"], item["version"])].summary)
                for item in outputs
            ],
        },
    }


@router.get("/setup", response_class=HTMLResponse)
async def setup_get(request: Request, step: Optional[int] = None, restart: bool = False) -> HTMLResponse:
    """Render the environment setup wizard."""

    if restart:
        state = reset_setup_state()
    else:
        state = load_setup_state()

    context = _build_setup_context(request, state, step=step)
    return templates.TemplateResponse("setup.html", context)


@router.post("/setup", response_class=HTMLResponse)
async def setup_post(request: Request) -> HTMLResponse:
    """Handle setup wizard transitions and persist configuration."""

    form = await request.form()
    action = str(form.get("step") or "1")
    state = load_setup_state()

    if action == "1":
        selections: Dict[str, str] = {}
        errors: List[str] = []
        for module_key in SETUP_MODULES.keys():
            module_meta = SETUP_MODULES[module_key]
            field_name = f"module__{module_key}"
            raw_value = str(form.get(field_name) or "").strip()
            allowed_options = _module_visible_options(module_key, selections)
            hide_module = _module_should_hide(module_key, selections) or (
                module_meta.get("depends_on") and not allowed_options
            )

            if hide_module:
                default_option = _module_default_option(module_key, selections)
                if default_option:
                    selections[module_key] = default_option
                continue

            if allowed_options:
                if len(allowed_options) == 1:
                    selections[module_key] = allowed_options[0]
                    continue
                if raw_value and raw_value in allowed_options:
                    selections[module_key] = raw_value
                    continue
                default_option = _module_default_option(module_key, selections)
                if default_option and default_option in allowed_options:
                    selections[module_key] = default_option
                    continue
                errors.append(f"Select an option for {module_meta.get('title') or module_key}.")
                continue

            if raw_value and raw_value in module_meta["options"]:
                selections[module_key] = raw_value
                continue

            default_option = _module_default_option(module_key, selections)
            if default_option:
                selections[module_key] = default_option
            else:
                errors.append(f"Select an option for {module_meta.get('title') or module_key}.")

        if errors:
            temp_state = dict(state)
            temp_state["selected_options"] = selections
            context = _build_setup_context(request, temp_state, step=1, errors=errors)
            return templates.TemplateResponse("setup.html", context, status_code=422)

        configuration = state.get("configuration") if isinstance(state, Mapping) else {}
        new_configuration: Dict[str, Any] = {}
        if isinstance(configuration, Mapping):
            for module_key in selections:
                module_config = configuration.get(module_key, {})
                if isinstance(module_config, Mapping):
                    new_configuration[module_key] = dict(module_config)

        updated_state = dict(state)
        updated_state["selected_options"] = selections
        updated_state["configuration"] = new_configuration
        updated_state["current_step"] = 2
        updated_state["completed"] = False
        save_setup_state(updated_state)
        return RedirectResponse(url="/setup?step=2", status_code=303)

    if action == "2":
        selected_options = state.get("selected_options") if isinstance(state, Mapping) else {}
        if not isinstance(selected_options, Mapping) or not selected_options:
            context = _build_setup_context(request, state, step=1, errors=["Choose an implementation for each module first."])
            return templates.TemplateResponse("setup.html", context, status_code=422)

        configuration: Dict[str, Dict[str, Any]] = {}
        errors = []
        for module_key, option_key in selected_options.items():
            module_meta = SETUP_MODULES.get(module_key)
            option_meta = module_meta["options"].get(option_key) if module_meta else None
            if not option_meta:
                continue
            module_config: Dict[str, Any] = {}
            for field_meta in option_meta.get("fields", []):
                field_name = str(field_meta.get("name") or "")
                if not field_name:
                    continue
                form_key = f"config__{module_key}__{field_name}"
                value = str(form.get(form_key) or "").strip()
                if not value and not field_meta.get("optional"):
                    errors.append(f"{module_meta.get('title')}: {field_meta.get('label')} is required.")
                module_config[field_name] = value
            configuration[module_key] = module_config

        if errors:
            temp_state = dict(state)
            temp_state["configuration"] = configuration
            context = _build_setup_context(request, temp_state, step=2, errors=errors)
            return templates.TemplateResponse("setup.html", context, status_code=422)

        updated_state = dict(state)
        updated_state["configuration"] = configuration
        updated_state["current_step"] = 3
        updated_state["completed"] = False
        save_setup_state(updated_state)
        return RedirectResponse(url="/setup?step=3", status_code=303)

    if action == "complete":
        updated_state = dict(state)
        updated_state["completed"] = True
        updated_state["current_step"] = 3
        updated_state["completed_at"] = datetime.utcnow().isoformat() + "Z"
        save_setup_state(updated_state)
        return RedirectResponse(url="/", status_code=303)

    if action == "reset":
        reset_setup_state()
        return RedirectResponse(url="/setup?step=1", status_code=303)

    context = _build_setup_context(request, state)
    return templates.TemplateResponse("setup.html", context)


@router.get("/setup/export", response_class=StreamingResponse)
async def setup_export() -> StreamingResponse:
    """Return a ZIP archive with the current setup selections."""

    state = load_setup_state()
    selected_options = state.get("selected_options") if isinstance(state, Mapping) else {}
    if not isinstance(selected_options, Mapping) or not selected_options:
        raise HTTPException(
            status_code=400,
            detail="Select module implementations before exporting the configuration bundle.",
        )

    configuration = state.get("configuration") if isinstance(state, Mapping) else {}
    if not isinstance(configuration, Mapping):
        configuration = {}

    if _requires_configuration(selected_options, configuration):
        raise HTTPException(
            status_code=400,
            detail="Provide the required configuration values for each module before exporting.",
        )

    try:
        buffer, export_payload = _build_setup_bundle(state)
    except ValueError as exc:  # pragma: no cover - defensive
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    generated_at = str(export_payload.get("generated_at") or datetime.utcnow().isoformat() + "Z")
    slug = re.sub(r"[^0-9A-Za-z]", "", generated_at)
    timestamp = slug or datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    filename = f"dc43-setup-{timestamp}.zip"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(buffer, media_type="application/zip", headers=headers)


@router.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("index.html", {"request": request})


@router.get("/integration-helper", response_class=HTMLResponse)
async def integration_helper(request: Request) -> HTMLResponse:
    """Render the contract integration helper interface."""

    context = {
        "request": request,
        "catalog": _integration_catalog(),
        "integration_options": [
            {"value": "spark", "label": "Spark (PySpark / Delta Lake)"},
        ],
    }
    return templates.TemplateResponse("integration_helper.html", context)


@router.get("/contracts", response_class=HTMLResponse)
async def list_contracts(request: Request) -> HTMLResponse:
    contract_ids = store.list_contracts()
    return templates.TemplateResponse(
        "contracts.html", {"request": request, "contracts": contract_ids}
    )


@router.get("/contracts/new", response_class=HTMLResponse)
async def new_contract_form(request: Request) -> HTMLResponse:
    editor_state = _contract_editor_state()
    editor_state["version"] = editor_state.get("version") or "1.0.0"
    context = _editor_context(request, editor_state=editor_state)
    return templates.TemplateResponse("new_contract.html", context)


@router.post("/contracts/new", response_class=HTMLResponse)
async def create_contract(
    request: Request,
    payload: str = Form(...),
) -> HTMLResponse:
    error: Optional[str] = None
    try:
        editor_state = json.loads(payload)
    except json.JSONDecodeError as exc:
        error = f"Invalid editor payload: {exc.msg}"
        editor_state = _contract_editor_state()
    else:
        try:
            _validate_contract_payload(editor_state, editing=False)
            model = _build_contract_from_payload(editor_state)
            store.put(model)
            return RedirectResponse(url="/contracts", status_code=303)
        except (ValidationError, ValueError) as exc:
            error = str(exc)
        except Exception as exc:  # pragma: no cover - display unexpected errors
            error = str(exc)
    context = _editor_context(
        request,
        editor_state=editor_state,
        error=error,
    )
    return templates.TemplateResponse("new_contract.html", context)




@router.get("/contracts/{cid}", response_class=HTMLResponse)
async def list_contract_versions(request: Request, cid: str) -> HTMLResponse:
    versions = store.list_versions(cid)
    if not versions:
        raise HTTPException(status_code=404, detail="Contract not found")
    records_by_version: Dict[str, List[DatasetRecord]] = {}
    for record in load_records():
        if record.contract_id != cid:
            continue
        records_by_version.setdefault(record.contract_version, []).append(record)

    contracts = []
    for ver in versions:
        try:
            contract = store.get(cid, ver)
        except FileNotFoundError:
            continue

        status_raw = getattr(contract, "status", "") or "unknown"
        status_value = str(status_raw).lower()
        status_label = str(status_raw).replace("_", " ").title()
        status_badge = _CONTRACT_STATUS_BADGES.get(status_value, "bg-secondary")

        server_info = _server_details(contract)
        dataset_hint = (
            server_info.get("dataset_id")
            if server_info
            else (contract.id or cid)
        )

        latest_run: Optional[DatasetRecord] = None
        run_entries = records_by_version.get(ver, [])
        if run_entries:
            run_entries.sort(key=lambda item: _version_sort_key(item.dataset_version or ""))
            latest_run = run_entries[-1]

        contracts.append(
            {
                "id": cid,
                "version": ver,
                "status": status_value,
                "status_label": status_label,
                "status_badge": status_badge,
                "server": server_info,
                "dataset_hint": dataset_hint,
                "latest_run": latest_run.__dict__ if latest_run else None,
            }
        )
    context = {"request": request, "contract_id": cid, "contracts": contracts}
    return templates.TemplateResponse("contract_versions.html", context)


@router.get("/contracts/{cid}/{ver}", response_class=HTMLResponse)
async def contract_detail(request: Request, cid: str, ver: str) -> HTMLResponse:
    try:
        contract = store.get(cid, ver)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    records = load_records()
    datasets = [r for r in records if r.contract_id == cid and r.contract_version == ver]
    product_links = data_products_for_contract(cid, records)
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
        "expectations": await _expectation_predicates(contract),
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
        "data_products": product_links,
    }
    return templates.TemplateResponse("contract_detail.html", context)


def _next_version(ver: str) -> str:
    v = Version(ver)
    return f"{v.major}.{v.minor}.{v.micro + 1}"


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
    except (TypeError, ValueError):
        raise ValueError(f"Expected integer value, got {value!r}")


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
        if str(key) == "dc43.core.versioning":
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


def _contract_editor_state(contract: Optional[OpenDataContractStandard] = None) -> Dict[str, Any]:
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
    description = getattr(contract.description, "usage", "") if getattr(contract, "description", None) else ""
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
        state["schemaObjects"] = _contract_editor_state(None)["schemaObjects"]
    return state


def _sorted_versions(values: Iterable[str]) -> List[str]:
    parsed: List[Tuple[Version, str]] = []
    invalid: List[str] = []
    for value in values:
        if not value:
            continue
        try:
            parsed.append((Version(str(value)), str(value)))
        except InvalidVersion:
            invalid.append(str(value))
    parsed.sort(key=lambda entry: entry[0])
    return [ver for _, ver in parsed] + sorted(invalid)


def _build_editor_meta(
    *,
    editor_state: Mapping[str, Any],
    editing: bool,
    original_version: Optional[str],
    baseline_state: Optional[Mapping[str, Any]],
    baseline_contract: Optional[OpenDataContractStandard],
) -> Dict[str, Any]:
    existing_contracts = sorted(store.list_contracts())
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
        "contractId": str(editor_state.get("id", "")) or (
            getattr(baseline_contract, "id", "") if baseline_contract else ""
        ),
    }
    if original_version:
        meta["baseVersion"] = original_version
    if baseline_state is None and baseline_contract is not None:
        baseline_state = _contract_editor_state(baseline_contract)
    if baseline_state is not None:
        # ensure baseline is JSON serializable
        meta["baselineState"] = jsonable_encoder(baseline_state)
    if baseline_contract is not None:
        meta["baseContract"] = contract_to_dict(baseline_contract)
    return meta


def _editor_context(
    request: Request,
    *,
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


def _custom_properties_models(items: Optional[Iterable[Mapping[str, Any]]]) -> List[CustomProperty] | None:
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


def _validate_contract_payload(
    payload: Mapping[str, Any],
    *,
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
    existing_contracts = set(store.list_contracts())
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


def _support_models(items: Optional[Iterable[Mapping[str, Any]]]) -> List[Support] | None:
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


def _sla_models(items: Optional[Iterable[Mapping[str, Any]]]) -> List[ServiceLevelAgreementProperty] | None:
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
            raise ValueError(f"Expectation {expectation} requires a numeric value") from exc
    if expectation in {"mustBe", "mustNotBe"}:
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text
    return text


def _quality_models(items: Optional[Iterable[Mapping[str, Any]]]) -> List[DataQuality] | None:
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
            payload[expectation] = _parse_expectation_value(expectation, item.get("expectationValue"))
        implementation = item.get("implementation")
        if implementation not in (None, ""):
            payload["implementation"] = _parse_json_value(implementation)
        custom_props = _custom_properties_models(item.get("customProperties"))
        if custom_props:
            payload["customProperties"] = custom_props
        if payload:
            result.append(DataQuality(**payload))
    return result or None


def _schema_properties_models(items: Optional[Iterable[Mapping[str, Any]]]) -> List[SchemaProperty]:
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


def _schema_objects_models(items: Optional[Iterable[Mapping[str, Any]]]) -> List[SchemaObject]:
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


def _server_models(items: Optional[Iterable[Mapping[str, Any]]]) -> List[Server] | None:
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
                raise ValueError("dc43.core.versioning must be provided as an object")
            custom_props.append(
                CustomProperty(property="dc43.core.versioning", value=parsed_versioning)
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


def _build_contract_from_payload(payload: Mapping[str, Any]) -> OpenDataContractStandard:
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
    # Ensure each schema object has properties
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


@router.get("/contracts/{cid}/{ver}/edit", response_class=HTMLResponse)
async def edit_contract_form(request: Request, cid: str, ver: str) -> HTMLResponse:
    try:
        contract = store.get(cid, ver)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    new_ver = _next_version(ver)
    editor_state = _contract_editor_state(contract)
    baseline_state = json.loads(json.dumps(editor_state))
    editor_state["version"] = new_ver
    context = _editor_context(
        request,
        editor_state=editor_state,
        editing=True,
        original_version=ver,
        baseline_state=baseline_state,
        baseline_contract=contract,
    )
    return templates.TemplateResponse("new_contract.html", context)


@router.post("/contracts/{cid}/{ver}/edit", response_class=HTMLResponse)
async def save_contract_edits(
    request: Request,
    cid: str,
    ver: str,
    payload: str = Form(...),
    original_version: str = Form(""),
) -> HTMLResponse:
    editor_state: Dict[str, Any]
    baseline_contract: Optional[OpenDataContractStandard] = None
    baseline_state: Optional[Dict[str, Any]] = None
    base_version = original_version or ver
    try:
        baseline_contract = store.get(cid, base_version)
        baseline_state = json.loads(json.dumps(_contract_editor_state(baseline_contract)))
    except FileNotFoundError:
        baseline_contract = None
        baseline_state = None
    try:
        editor_state = json.loads(payload)
    except json.JSONDecodeError as exc:
        error = f"Invalid editor payload: {exc.msg}"
        editor_state = _contract_editor_state()
        editor_state["id"] = cid
        editor_state["version"] = _next_version(ver)
    else:
        try:
            _validate_contract_payload(
                editor_state,
                editing=True,
                base_contract_id=cid,
                base_version=base_version,
            )
            model = _build_contract_from_payload(editor_state)
            store.put(model)
            return RedirectResponse(
                url=f"/contracts/{model.id}/{model.version}", status_code=303
            )
        except (ValidationError, ValueError) as exc:
            error = str(exc)
        except Exception as exc:  # pragma: no cover - display unexpected errors
            error = str(exc)
    context = _editor_context(
        request,
        editor_state=editor_state,
        editing=True,
        original_version=base_version,
        baseline_state=baseline_state,
        baseline_contract=baseline_contract,
        error=error,
    )
    return templates.TemplateResponse("new_contract.html", context)


@router.post("/contracts/{cid}/{ver}/validate")
async def html_validate_contract(cid: str, ver: str) -> HTMLResponse:
    return RedirectResponse(url=f"/contracts/{cid}/{ver}", status_code=303)


@router.get("/datasets", response_class=HTMLResponse)
async def list_datasets(request: Request) -> HTMLResponse:
    records = load_records()
    catalog = dataset_catalog(records)
    context = {"request": request, "datasets": catalog}
    return templates.TemplateResponse("datasets.html", context)


@router.get("/datasets/{dataset_name}", response_class=HTMLResponse)
async def dataset_versions(request: Request, dataset_name: str) -> HTMLResponse:
    records = [r.__dict__.copy() for r in load_records() if r.dataset_name == dataset_name]
    context = {"request": request, "dataset_name": dataset_name, "records": records}
    return templates.TemplateResponse("dataset_versions.html", context)


@router.get("/data-products", response_class=HTMLResponse)
async def list_data_products(request: Request) -> HTMLResponse:
    records = load_records()
    catalog = data_product_catalog(records)
    context = {"request": request, "products": catalog}
    return templates.TemplateResponse("data_products.html", context)


@router.get("/data-products/{product_id}", response_class=HTMLResponse)
async def data_product_detail_view(request: Request, product_id: str) -> HTMLResponse:
    records = load_records()
    details = describe_data_product(product_id, records)
    if details is None:
        raise HTTPException(status_code=404, detail="Data product not found")
    context = {"request": request, "product": details}
    return templates.TemplateResponse("data_product_detail.html", context)


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




def _data_products_payload() -> List[Mapping[str, Any]]:
    try:
        raw = json.loads(DATA_PRODUCTS_FILE.read_text())
    except (OSError, json.JSONDecodeError):
        return []
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, Mapping)]
    return []


def load_data_products() -> List[OpenDataProductStandard]:
    documents: List[OpenDataProductStandard] = []
    for payload in _data_products_payload():
        try:
            documents.append(OpenDataProductStandard.from_dict(payload))
        except Exception:  # pragma: no cover - defensive
            continue
    return documents


def _port_custom_map(port: Any) -> Dict[str, Any]:
    props = getattr(port, "custom_properties", []) or []
    mapping: Dict[str, Any] = {}
    for entry in props:
        if isinstance(entry, Mapping):
            key = entry.get("property")
            if key:
                mapping[str(key)] = entry.get("value")
    return mapping


def _records_by_data_product(records: Iterable[DatasetRecord]) -> Dict[str, List[DatasetRecord]]:
    grouped: Dict[str, List[DatasetRecord]] = {}
    for record in records:
        if record.data_product_id:
            grouped.setdefault(record.data_product_id, []).append(record)
    for entries in grouped.values():
        entries.sort(key=lambda item: _version_sort_key(item.dataset_version or ""))
    return grouped


def data_product_catalog(records: Iterable[DatasetRecord]) -> List[Dict[str, Any]]:
    grouped = _records_by_data_product(records)
    summaries: List[Dict[str, Any]] = []
    for product in load_data_products():
        product_records = list(grouped.get(product.id, []))
        latest_record = product_records[-1] if product_records else None
        inputs: List[Dict[str, Any]] = []
        for port in product.input_ports:
            props = _port_custom_map(port)
            inputs.append(
                {
                    "name": port.name,
                    "contract_id": port.contract_id,
                    "version": port.version,
                    "source_data_product": props.get("dc43.input.source_data_product"),
                    "source_output_port": props.get("dc43.input.source_output_port"),
                    "custom_properties": props,
                }
            )
        outputs: List[Dict[str, Any]] = []
        for port in product.output_ports:
            props = _port_custom_map(port)
            outputs.append(
                {
                    "name": port.name,
                    "contract_id": port.contract_id,
                    "version": port.version,
                    "dataset_id": props.get("dc43.dataset.id") or props.get("dc43.contract.ref"),
                    "stage_contract": props.get("dc43.stage.contract"),
                    "custom_properties": props,
                }
            )
        summaries.append(
            {
                "id": product.id,
                "name": product.name or product.id,
                "status": product.status,
                "version": product.version or "",
                "description": product.description or {},
                "tags": list(product.tags or []),
                "inputs": inputs,
                "outputs": outputs,
                "run_count": len(product_records),
                "latest_run": latest_record,
            }
        )
    summaries.sort(key=lambda item: item["id"])
    return summaries


def describe_data_product(
    product_id: str,
    records: Iterable[DatasetRecord],
) -> Dict[str, Any] | None:
    products = {doc.id: doc for doc in load_data_products()}
    product = products.get(product_id)
    if product is None:
        return None
    grouped = _records_by_data_product(records)
    product_records = list(grouped.get(product.id, []))
    product_records.sort(key=lambda item: _version_sort_key(item.dataset_version or ""))
    input_ports: List[Dict[str, Any]] = []
    for port in product.input_ports:
        props = _port_custom_map(port)
        input_ports.append(
            {
                "name": port.name,
                "contract_id": port.contract_id,
                "version": port.version,
                "custom_properties": props,
                "source_data_product": props.get("dc43.input.source_data_product"),
                "source_output_port": props.get("dc43.input.source_output_port"),
            }
        )
    output_ports: List[Dict[str, Any]] = []
    for port in product.output_ports:
        props = _port_custom_map(port)
        dataset_id = props.get("dc43.dataset.id") or props.get("dc43.contract.ref")
        related_records = [
            rec
            for rec in product_records
            if rec.data_product_port == port.name
            or rec.dataset_name == dataset_id
            or rec.contract_id == port.contract_id
        ]
        output_ports.append(
            {
                "name": port.name,
                "contract_id": port.contract_id,
                "version": port.version,
                "dataset_id": dataset_id,
                "stage_contract": props.get("dc43.stage.contract"),
                "custom_properties": props,
                "records": related_records,
            }
        )
    return {
        "id": product.id,
        "name": product.name or product.id,
        "status": product.status,
        "version": product.version or "",
        "description": product.description or {},
        "custom_properties": product.custom_properties or [],
        "tags": list(product.tags or []),
        "inputs": input_ports,
        "outputs": output_ports,
        "records": product_records,
    }


def data_products_for_contract(contract_id: str, records: Iterable[DatasetRecord]) -> List[Dict[str, Any]]:
    matches: List[Dict[str, Any]] = []
    grouped_records = _records_by_data_product(records)
    for product in load_data_products():
        for port in list(product.input_ports) + list(product.output_ports):
            if port.contract_id != contract_id:
                continue
            props = _port_custom_map(port)
            product_records = grouped_records.get(product.id, [])
            matches.append(
                {
                    "product_id": product.id,
                    "product_name": product.name or product.id,
                    "port_name": port.name,
                    "port_version": port.version,
                    "direction": "input" if port in product.input_ports else "output",
                    "records": product_records,
                    "custom_properties": props,
                }
            )
    matches.sort(key=lambda item: (item["product_id"], item["port_name"]))
    return matches


def data_products_for_dataset(dataset_name: str, records: Iterable[DatasetRecord]) -> List[Dict[str, Any]]:
    associations: List[Dict[str, Any]] = []
    for record in records:
        if record.dataset_name != dataset_name or not record.data_product_id:
            continue
        associations.append(
            {
                "product_id": record.data_product_id,
                "port_name": record.data_product_port,
                "role": record.data_product_role,
                "status": record.status,
                "dataset_version": record.dataset_version,
            }
        )
    associations.sort(key=lambda item: _version_sort_key(item.get("dataset_version") or ""))
    return associations



def dataset_catalog(records: Iterable[DatasetRecord]) -> List[Dict[str, Any]]:
    """Summarise known datasets and associated contract information."""

    grouped: Dict[str, Dict[str, Any]] = {}
    for record in records:
        if not record.dataset_name:
            continue
        bucket = grouped.setdefault(
            record.dataset_name,
            {"dataset_name": record.dataset_name, "records": []},
        )
        bucket["records"].append(record)

    for contract_id in store.list_contracts():
        for version in store.list_versions(contract_id):
            try:
                contract = store.get(contract_id, version)
            except FileNotFoundError:
                continue
            server_info = _server_details(contract) or {}
            dataset_id = (
                server_info.get("dataset_id")
                or server_info.get("dataset")
                or contract.id
                or contract_id
            )
            bucket = grouped.setdefault(
                dataset_id,
                {"dataset_name": dataset_id, "records": []},
            )
            contracts_map = bucket.setdefault("contracts_by_id", {})
            contracts = contracts_map.setdefault(contract_id, [])
            contracts.append(
                {
                    "version": version,
                    "status": getattr(contract, "status", ""),
                    "server": server_info,
                }
            )

    catalog: List[Dict[str, Any]] = []
    for dataset_name, payload in grouped.items():
        dataset_records: List[DatasetRecord] = list(payload.get("records", []))
        dataset_records.sort(key=lambda item: _version_sort_key(item.dataset_version or ""))
        latest_record: Optional[DatasetRecord] = dataset_records[-1] if dataset_records else None

        product_associations = data_products_for_dataset(dataset_name, dataset_records)
        product_summary_map: Dict[tuple[str, str], Dict[str, Any]] = {}
        for association in product_associations:
            if not isinstance(association, Mapping):
                continue
            product_id = str(association.get("product_id") or "")
            port_name = str(association.get("port_name") or "")
            product_summary_map[(product_id, port_name)] = dict(association)
        product_summaries: List[Dict[str, Any]] = []
        for summary in product_summary_map.values():
            product_summaries.append(
                {
                    "product_id": summary.get("product_id"),
                    "port_name": summary.get("port_name"),
                    "role": summary.get("role"),
                    "latest_status": summary.get("status"),
                    "latest_dataset_version": summary.get("dataset_version"),
                }
            )
        product_summaries.sort(
            key=lambda item: (
                str(item.get("product_id") or ""),
                str(item.get("port_name") or ""),
            )
        )

        latest_status_value: Optional[str] = None
        latest_status_label: str = ""
        latest_status_badge: str = ""
        latest_reason: Optional[str] = None
        if latest_record:
            status_raw = str(latest_record.status or "unknown")
            latest_status_value = status_raw.lower()
            latest_status_label = status_raw.replace("_", " ").title()
            latest_status_badge = _DQ_STATUS_BADGES.get(latest_status_value, "bg-secondary")
            latest_reason = latest_record.reason or None

        run_drafts = sorted(
            {rec.draft_contract_version for rec in dataset_records if rec.draft_contract_version},
            key=_version_sort_key,
        )
        contracts_summary: List[Dict[str, Any]] = []
        contracts_map: Dict[str, List[Dict[str, Any]]] = payload.get("contracts_by_id", {})
        for contract_id, versions in contracts_map.items():
            versions.sort(key=lambda item: _version_sort_key(item["version"]))
            other_versions = [item["version"] for item in versions[:-1]]
            latest_contract = versions[-1] if versions else {"version": "", "status": ""}
            status_raw = str(latest_contract.get("status") or "unknown")
            status_value = status_raw.lower()
            status_label = status_raw.replace("_", " ").title()
            draft_versions = [
                item for item in versions if str(item.get("status", "")).lower() == "draft"
            ]
            latest_draft = draft_versions[-1]["version"] if draft_versions else None
            contracts_summary.append(
                {
                    "id": contract_id,
                    "latest_version": latest_contract.get("version", ""),
                    "latest_status": status_value,
                    "latest_status_label": status_label,
                    "other_versions": other_versions,
                    "drafts_count": len(draft_versions),
                    "latest_draft_version": latest_draft,
                }
            )

        contracts_summary.sort(key=lambda item: item["id"])

        catalog.append(
            {
                "dataset_name": dataset_name,
                "latest_version": latest_record.dataset_version if latest_record else "",
                "latest_status": latest_status_value,
                "latest_status_label": latest_status_label,
                "latest_status_badge": latest_status_badge,
                "latest_record_reason": latest_reason,
                "contract_summaries": contracts_summary,
                "data_products": product_summaries,
                "run_drafts_count": len(run_drafts),
                "run_latest_draft_version": run_drafts[-1] if run_drafts else None,
            }
        )

    catalog.sort(key=lambda item: item["dataset_name"])
    return catalog


@router.get("/datasets/{dataset_name}/{dataset_version}", response_class=HTMLResponse)
async def dataset_detail(request: Request, dataset_name: str, dataset_version: str) -> HTMLResponse:
    records = load_records()
    associations = data_products_for_dataset(dataset_name, records)
    for r in records:
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
                "data_products": associations,
            }
            return templates.TemplateResponse("dataset_detail.html", context)
    raise HTTPException(status_code=404, detail="Dataset not found")


def create_app() -> FastAPI:
    """Return a FastAPI application serving contract and dataset views."""

    application = FastAPI(title="DC43 Contracts App")

    @application.middleware("http")
    async def setup_guard(request: Request, call_next):  # type: ignore[override]
        path = request.url.path
        exempt_paths = {"/openapi.json"}
        exempt_prefixes = ("/setup", "/static", "/docs", "/redoc")
        if path in exempt_paths or any(path.startswith(prefix) for prefix in exempt_prefixes):
            request.state.setup_required = not is_setup_complete()
            return await call_next(request)

        setup_done = is_setup_complete()
        request.state.setup_required = not setup_done
        if not setup_done and path == "/":
            return RedirectResponse(url="/setup", status_code=307)
        return await call_next(request)

    application.mount(
        "/static",
        StaticFiles(directory=str(BASE_DIR / "static"), check_dir=False),
        name="static",
    )
    application.include_router(router)
    return application


app = create_app()


def run(config_path: str | os.PathLike[str] | None = None) -> None:  # pragma: no cover - convenience runner
    """Run the demo UI and spawn a dedicated backend server."""

    import uvicorn

    config = configure_from_config(load_config(config_path))
    backend_cfg = config.backend
    process_cfg = backend_cfg.process
    backend_host = process_cfg.host
    backend_port = process_cfg.port
    backend_url = backend_cfg.base_url or process_cfg.url()

    env = os.environ.copy()
    env.setdefault("DC43_CONTRACT_STORE", str(CONTRACT_DIR))
    cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        "dc43_service_backends.webapp:app",
        "--host",
        backend_host,
        "--port",
        str(backend_port),
    ]
    log_level = process_cfg.log_level
    if log_level:
        cmd.extend(["--log-level", log_level])

    process = subprocess.Popen(cmd, env=env)

    try:
        _wait_for_backend(backend_url)
    except Exception:
        process.terminate()
        process.wait(timeout=5)
        raise

    try:
        configure_backend(base_url=backend_url)
        uvicorn.run("dc43_contracts_app.server:app", host="0.0.0.0", port=8000)
    finally:
        process.terminate()
        with contextlib.suppress(Exception):
            process.wait(timeout=5)
        configure_from_config(config)
